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

	functionconfig "github.com/kptdev/kpt/function-pod-manager/api/functionconfig/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner/util"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// GarbageCollector reaps requests, deletes idle evaluator pods, and warms the pool.
// It runs only on the leader.
type GarbageCollector struct {
	*FunctionPodRequestReconciler
	ScanInterval  time.Duration
	WarmupOnStart bool
}

// NeedLeaderElection keeps scaling and garbage collection on one replica.
func (m *GarbageCollector) NeedLeaderElection() bool { return true }

// Start warms the pool, then reaps and garbage-collects on each scan.
func (m *GarbageCollector) Start(ctx context.Context) error {
	if err := m.WarmupAll(ctx); err != nil {
		klog.ErrorS(err, "function pod warmup failed")
	}
	interval := m.ScanInterval
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
			if err := m.Tick(ctx); err != nil {
				klog.ErrorS(err, "function pod garbage collection failed")
			}
		}
	}
}

// Tick reaps stale requests and deletes idle evaluator pods.
func (m *GarbageCollector) Tick(ctx context.Context) error {
	klog.InfoS("function pod garbage collection called")
	now := time.Now()
	requests, err := m.listFunctionPodRequests(ctx)
	if err != nil {
		return err
	}
	pods, err := m.listPods(ctx)
	if err != nil {
		return err
	}
	configs, err := m.listFunctionConfigs(ctx)
	if err != nil {
		return err
	}
	if err := m.reapAll(ctx, requests, pods, now); err != nil {
		return err
	}
	if err := m.deleteExpiredErrors(ctx, requests, now); err != nil {
		return err
	}
	return m.collectIdle(ctx, pods, requests, configs, now)
}

// WarmupAll starts one pod for each FunctionConfig whose first tag is a concrete version.
func (m *GarbageCollector) WarmupAll(ctx context.Context) error {
	if !m.WarmupOnStart {
		return nil
	}
	configs, err := m.listFunctionConfigs(ctx)
	if err != nil {
		return err
	}
	pods, err := m.listPods(ctx)
	if err != nil {
		return err
	}
	for i := range configs {
		if err := m.Provisioner.Warmup(ctx, &configs[i], pods); err != nil {
			klog.ErrorS(err, "warmup failed", "functionConfig", configs[i].Name)
		}
	}
	return nil
}

func (m *GarbageCollector) reapAll(ctx context.Context, requests []v1alpha1.FunctionPodRequest, pods []corev1.Pod, now time.Time) error {
	for i := range requests {
		if _, err := m.reapOne(ctx, &requests[i], pods, now); err != nil {
			return err
		}
	}
	return nil
}

func (m *GarbageCollector) collectIdle(
	ctx context.Context,
	pods []corev1.Pod,
	requests []v1alpha1.FunctionPodRequest,
	configs []functionconfig.FunctionConfig,
	now time.Time,
) error {
	claimed := claimedPodNames(requests)
	for i := range pods {
		pod := &pods[i]
		if _, claimedPod := claimed[pod.Name]; claimedPod {
			if err := m.clearIdle(ctx, pod); err != nil {
				return err
			}
			continue
		}
		if err := m.markOrDeleteIdle(ctx, pod, configs, now); err != nil {
			return err
		}
	}
	return nil
}

func (m *GarbageCollector) clearIdle(ctx context.Context, pod *corev1.Pod) error {
	if pod.Annotations[fnpod.AnnotationIdleSince] == "" {
		return nil
	}
	delete(pod.Annotations, fnpod.AnnotationIdleSince)
	return m.Update(ctx, pod)
}

func (m *GarbageCollector) markOrDeleteIdle(ctx context.Context, pod *corev1.Pod, configs []functionconfig.FunctionConfig, now time.Time) error {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	since := pod.Annotations[fnpod.AnnotationIdleSince]
	if since == "" {
		pod.Annotations[fnpod.AnnotationIdleSince] = now.UTC().Format(time.RFC3339)
		return m.Update(ctx, pod)
	}
	stamp, err := time.Parse(time.RFC3339, since)
	if err != nil {
		return nil
	}
	repository := pod.Annotations[fnpod.AnnotationFunctionImage]
	version := pod.Annotations[fnpod.AnnotationResolvedVersion]
	ttl := limitsFor(configs, repository, version, m.ImagePrefix, m.Limits).PodTTL
	if now.Sub(stamp) < ttl {
		return nil
	}
	m.event(pod, corev1.EventTypeNormal, fnpod.EventDeletedIdle, "Deleted idle pod for "+util.DisplayName(repository, version))
	if err := m.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return m.deleteService(ctx, pod)
}

func (m *GarbageCollector) deleteService(ctx context.Context, pod *corev1.Pod) error {
	name := pod.Labels[fnpod.LabelImage]
	if name == "" {
		return nil
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pod.Namespace}}
	if err := m.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
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
