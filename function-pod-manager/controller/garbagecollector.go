// Copyright 2026 The kpt Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"time"

	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DefaultReapParallelism is how many requests one scan reaps at once when the option is unset.
const DefaultReapParallelism = 8

// GarbageCollector reaps requests, deletes idle evaluator pods, and warms the pool.
// It runs only on the leader.
type GarbageCollector struct {
	*FunctionPodRequestReconciler
	ScanInterval    time.Duration
	WarmupOnStart   bool
	ReapParallelism int
}

// NeedLeaderElection keeps scaling and garbage collection on one replica.
func (gc *GarbageCollector) NeedLeaderElection() bool { return true }

// Start warms the pool, then reaps and garbage-collects on each scan.
func (gc *GarbageCollector) Start(ctx context.Context) error {
	if err := gc.WarmupAll(ctx); err != nil {
		klog.ErrorS(err, "function pod warmup failed")
	}
	interval := gc.ScanInterval
	if interval <= 0 {
		interval = time.Minute
	}
	klog.InfoS("function pod garbage collector started", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := gc.Tick(ctx); err != nil {
				klog.ErrorS(err, "function pod garbage collection failed")
			}
		}
	}
}

// Tick reaps stale requests and deletes idle evaluator pods.
func (gc *GarbageCollector) Tick(ctx context.Context) error {
	klog.InfoS("function pod garbage collection called")
	now := time.Now()
	requests, err := gc.listFunctionPodRequests(ctx)
	if err != nil {
		return err
	}
	pods, err := gc.listPods(ctx)
	if err != nil {
		return err
	}
	if err := gc.FunctionConfigStore.sync(func() ([]functionconfig.FunctionConfig, error) {
		return gc.listFunctionConfigs(ctx)
	}); err != nil {
		return err
	}
	if err := gc.reapAll(ctx, requests, pods, now); err != nil {
		return err
	}
	if err := gc.deleteExpiredErrors(ctx, requests, now); err != nil {
		return err
	}
	return gc.collectIdle(ctx, pods, requests, now)
}

// WarmupAll starts one pod for each FunctionConfig whose first tag is a concrete version.
func (gc *GarbageCollector) WarmupAll(ctx context.Context) error {
	if !gc.WarmupOnStart {
		return nil
	}
	configs, err := gc.listFunctionConfigs(ctx)
	if err != nil {
		return err
	}
	pods, err := gc.listPods(ctx)
	if err != nil {
		return err
	}
	for i := range configs {
		if err := gc.Provisioner.Warmup(ctx, &configs[i], pods); err != nil {
			klog.ErrorS(err, "warmup failed", "functionConfig", configs[i].Name)
		}
	}
	return nil
}

func (gc *GarbageCollector) parallelism() int {
	if gc.ReapParallelism <= 0 {
		return DefaultReapParallelism
	}
	return gc.ReapParallelism
}

func (gc *GarbageCollector) reapAll(ctx context.Context, requests []v1alpha1.FunctionPodRequest, pods []corev1.Pod, now time.Time) error {
	var group errgroup.Group
	group.SetLimit(gc.parallelism())
	for i := range requests {
		group.Go(func() error {
			_, err := gc.reapOne(ctx, &requests[i], pods, now)
			return err
		})
	}
	return group.Wait()
}

func (gc *GarbageCollector) collectIdle(
	ctx context.Context,
	pods []corev1.Pod,
	requests []v1alpha1.FunctionPodRequest,
	now time.Time,
) error {
	claimed := claimedPodNames(requests)
	for i := range pods {
		pod := &pods[i]
		if _, claimedPod := claimed[pod.Name]; claimedPod {
			if err := gc.clearIdle(ctx, pod); err != nil {
				return err
			}
			continue
		}
		if err := gc.markOrDeleteIdle(ctx, pod, now); err != nil {
			return err
		}
	}
	return nil
}

func (gc *GarbageCollector) clearIdle(ctx context.Context, pod *corev1.Pod) error {
	if pod.Annotations[fnpod.AnnotationIdleSince] == "" {
		return nil
	}
	delete(pod.Annotations, fnpod.AnnotationIdleSince)
	return gc.Update(ctx, pod)
}

func (gc *GarbageCollector) markOrDeleteIdle(ctx context.Context, pod *corev1.Pod, now time.Time) error {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	since := pod.Annotations[fnpod.AnnotationIdleSince]
	if since == "" {
		pod.Annotations[fnpod.AnnotationIdleSince] = now.UTC().Format(time.RFC3339)
		return gc.Update(ctx, pod)
	}
	stamp, err := time.Parse(time.RFC3339, since)
	if err != nil {
		return nil
	}
	repository := pod.Annotations[fnpod.AnnotationFunctionImage]
	version := pod.Annotations[fnpod.AnnotationResolvedVersion]
	ttl := gc.FunctionConfigStore.getPodPoolPolicy(repository, version, gc.ImagePrefix, gc.PodPoolPolicy).PodTTL
	if now.Sub(stamp) < ttl {
		return nil
	}
	gc.event(pod, corev1.EventTypeNormal, fnpod.EventDeletedIdle, "Deleted idle pod for "+displayName(repository, version))
	if err := gc.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return gc.deleteService(ctx, pod)
}

func (gc *GarbageCollector) deleteService(ctx context.Context, pod *corev1.Pod) error {
	name := pod.Labels[fnpod.LabelImage]
	if name == "" {
		return nil
	}
	peers := &corev1.PodList{}
	if err := gc.List(ctx, peers, client.InNamespace(pod.Namespace), client.MatchingLabels{fnpod.LabelImage: name}); err != nil {
		return err
	}
	for i := range peers.Items {
		if peers.Items[i].Name == pod.Name || !peers.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		return nil
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pod.Namespace}}
	if err := gc.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func claimedPodNames(requests []v1alpha1.FunctionPodRequest) map[string]struct{} {
	names := map[string]struct{}{}
	for i := range requests {
		if phaseOf(&requests[i]) != v1alpha1.FunctionPodRequestPhaseClaimed {
			continue
		}
		if requests[i].Status.EvaluatorPodName != "" {
			names[requests[i].Status.EvaluatorPodName] = struct{}{}
		}
	}
	return names
}
