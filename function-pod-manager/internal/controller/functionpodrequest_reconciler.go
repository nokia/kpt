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

// Package controller scales Function Evaluator pods for FunctionPodRequest objects.
package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kptdev/kpt/api/utils/image"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner/util"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Limits are the pool defaults from pod-lifecycle flags.
type Limits struct {
	PreferredQueue  int
	MaxParallel     int
	PodTTL          time.Duration
	PodReadyTimeout time.Duration
}

// FunctionPodRequestReconciler ensures a Function Evaluator pod can claim each request.
type FunctionPodRequestReconciler struct {
	client.Client
	Recorder         record.EventRecorder
	Provisioner      *provisioner.Provisioner
	RequestNamespace string
	PodNamespace     string
	ImagePrefix      string
	// Debounce is how long a request must stay Created before a new evaluator pod is started.
	Debounce time.Duration
	// Retention is how long a PodManagementError or EvaluationError request is kept before it is deleted.
	Retention time.Duration
	Limits    Limits
}

// SetupWithManager watches requests and the evaluator pods that can claim them.
func (r *FunctionPodRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.FunctionPodRequest{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPod)).
		Complete(r)
}

// Reconcile scales, resolves, or reaps one request. It does not evaluate the function.
func (r *FunctionPodRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Namespace != r.RequestNamespace {
		return ctrl.Result{}, nil
	}
	now := time.Now()
	request := &v1alpha1.FunctionPodRequest{}
	if err := r.Get(ctx, req.NamespacedName, request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	pods, err := r.listPods(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	requests, err := r.listFunctionPodRequests(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	configs, err := r.listFunctionConfigs(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.reconcile(ctx, request, requests, pods, configs, now)
}

func (r *FunctionPodRequestReconciler) reconcile(
	ctx context.Context,
	request *v1alpha1.FunctionPodRequest,
	requests []v1alpha1.FunctionPodRequest,
	pods []corev1.Pod,
	configs []functionconfig.FunctionConfig,
	now time.Time,
) (ctrl.Result, error) {
	if done, err := r.reapOne(ctx, request, pods, now); done || err != nil {
		return ctrl.Result{}, err
	}
	phase := phaseOf(request)
	if phase != v1alpha1.FunctionPodRequestPhaseCreated {
		return ctrl.Result{RequeueAfter: r.untilDeadline(request, now)}, nil
	}
	if wait := r.debounceRemaining(request, now); wait > 0 {
		return ctrl.Result{RequeueAfter: soonestPositive(wait, r.untilDeadline(request, now))}, nil
	}
	result := scaler{
		request:  request,
		requests: requests,
		pods:     pods,
		configs:  configs,
		prefix:   r.ImagePrefix,
		defaults: r.Limits,
		now:      now,
	}.scale()
	switch result.action {
	case scalePodFailed:
		return ctrl.Result{}, r.failPod(ctx, request, result.pod, result.message)
	case scaleCreatePod:
		return r.startPod(ctx, request, result, pods, configs)
	default:
		if err := r.noteMatchingPod(ctx, request, result.pod); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.untilDeadline(request, now)}, nil
	}
}

func (r *FunctionPodRequestReconciler) startPod(
	ctx context.Context,
	request *v1alpha1.FunctionPodRequest,
	result scaleResult,
	pods []corev1.Pod,
	configs []functionconfig.FunctionConfig,
) (ctrl.Result, error) {
	version := result.version
	if version == "" {
		var err error
		version, err = r.resolve(ctx, result.repository, request.Spec.VersionConstraint)
		if err != nil {
			if errors.Is(err, util.ErrNoMatchingTag) {
				return ctrl.Result{}, r.failConstraint(ctx, request, result.repository, request.Spec.VersionConstraint)
			}
			r.event(request, corev1.EventTypeWarning, fnpod.EventStartFailed, "Failed to start new pod: "+err.Error())
			return ctrl.Result{}, r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhasePodManagementError, err.Error())
		}
	}
	exec := executorFor(configs, result.repository, version, r.ImagePrefix)
	postfix := podsForRepository(pods, result.repository)
	pod, err := r.Provisioner.CreateEvaluator(ctx, result.repository, version, exec, len(postfix))
	if err != nil {
		message := err.Error()
		r.event(request, corev1.EventTypeWarning, fnpod.EventStartFailed, "Failed to start new pod: "+message)
		return ctrl.Result{}, r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhasePodManagementError, message)
	}
	display := displayName(result.repository, version)
	r.event(pod, corev1.EventTypeNormal, fnpod.EventStarting, "Starting new pod for "+display)
	r.event(request, corev1.EventTypeNormal, fnpod.EventStarting, "Started evaluator pod "+pod.Name+" for "+display)
	klog.InfoS("function pod request started evaluator", "request", request.Name, "namespace", request.Namespace, "pod", pod.Name, "image", display)
	return ctrl.Result{RequeueAfter: r.Limits.PodReadyTimeout}, nil
}

func (r *FunctionPodRequestReconciler) resolve(ctx context.Context, repository, constraint string) (string, error) {
	if constraint == "" {
		return "latest", nil
	}
	tags, err := r.Provisioner.ListTags(ctx, repository)
	if err != nil {
		return "", err
	}
	return util.SelectTag(tags, constraint)
}

func (r *FunctionPodRequestReconciler) failConstraint(ctx context.Context, request *v1alpha1.FunctionPodRequest, repository, constraint string) error {
	base := displayName(repository, "")
	message := fmt.Sprintf("no published tag satisfies %s for image %s", constraint, repository)
	klog.InfoS("no published tag satisfies constraint", "request", request.Name, "namespace", request.Namespace, "image", repository, "constraint", constraint)
	r.event(request, corev1.EventTypeWarning, fnpod.EventUnsatisfiable,
		fmt.Sprintf("Cannot start pod for %s: no published tag satisfies %s", base, constraint))
	return r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhasePodManagementError, message)
}

func displayName(repository, version string) string {
	base := image.Parse(repository).BaseName
	if version == "" {
		return base
	}
	return base + ":" + version
}

func (r *FunctionPodRequestReconciler) failPod(ctx context.Context, request *v1alpha1.FunctionPodRequest, pod *corev1.Pod, message string) error {
	target := client.Object(request)
	if pod != nil {
		target = pod
	}
	r.event(target, corev1.EventTypeWarning, fnpod.EventStartFailed, "Failed to start new pod: "+message)
	return r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhasePodManagementError, message)
}

func (r *FunctionPodRequestReconciler) requestsForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.Namespace != r.PodNamespace {
		return nil
	}
	repository := pod.Annotations[fnpod.AnnotationFunctionImage]
	if repository == "" {
		return nil
	}
	requests, err := r.listFunctionPodRequests(ctx)
	if err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range requests {
		if repositoryOf(requests[i].Spec.Image, r.ImagePrefix) != repository {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: requests[i].Namespace,
			Name:      requests[i].Name,
		}})
	}
	return out
}

func (r *FunctionPodRequestReconciler) listPods(ctx context.Context) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := r.List(ctx, list, client.InNamespace(r.PodNamespace)); err != nil {
		return nil, err
	}
	var pods []corev1.Pod
	for i := range list.Items {
		if list.Items[i].Annotations[fnpod.AnnotationFunctionImage] == "" {
			continue
		}
		pods = append(pods, list.Items[i])
	}
	return pods, nil
}

func (r *FunctionPodRequestReconciler) listFunctionPodRequests(ctx context.Context) ([]v1alpha1.FunctionPodRequest, error) {
	list := &v1alpha1.FunctionPodRequestList{}
	if err := r.List(ctx, list, client.InNamespace(r.RequestNamespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *FunctionPodRequestReconciler) listFunctionConfigs(ctx context.Context) ([]functionconfig.FunctionConfig, error) {
	list := &functionconfig.FunctionConfigList{}
	if err := r.List(ctx, list, client.InNamespace(r.PodNamespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *FunctionPodRequestReconciler) debounceRemaining(request *v1alpha1.FunctionPodRequest, now time.Time) time.Duration {
	if request.CreationTimestamp.IsZero() {
		return 0
	}
	readyAt := request.CreationTimestamp.Add(r.Debounce)
	if now.Before(readyAt) {
		return readyAt.Sub(now)
	}
	return 0
}

func (r *FunctionPodRequestReconciler) untilDeadline(request *v1alpha1.FunctionPodRequest, now time.Time) time.Duration {
	if request.Spec.Deadline == nil || request.Spec.Deadline.IsZero() {
		return 0
	}
	until := request.Spec.Deadline.Sub(now)
	if until < 0 {
		return 0
	}
	return until
}

func soonestPositive(a, b time.Duration) time.Duration {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

func (r *FunctionPodRequestReconciler) noteMatchingPod(ctx context.Context, request *v1alpha1.FunctionPodRequest, pod *corev1.Pod) error {
	if pod == nil || request.Annotations[fnpod.AnnotationMatchedPod] != "" {
		return nil
	}
	const message = "An existing evaluator can serve this request"
	r.event(request, corev1.EventTypeNormal, fnpod.EventPodAvailable, message)
	klog.InfoS("function pod request matched existing evaluator", "request", request.Name, "namespace", request.Namespace)
	return r.markAnnotation(ctx, request, fnpod.AnnotationMatchedPod, "true")
}

func (r *FunctionPodRequestReconciler) markAnnotation(ctx context.Context, request *v1alpha1.FunctionPodRequest, key, value string) error {
	if request.Annotations == nil {
		request.Annotations = map[string]string{}
	}
	request.Annotations[key] = value
	if err := r.Update(ctx, request); err != nil {
		return fmt.Errorf("mark %s on %s: %w", key, request.Name, err)
	}
	return nil
}

func (r *FunctionPodRequestReconciler) event(obj client.Object, eventType, reason, message string) {
	if r.Recorder == nil || obj == nil {
		return
	}
	r.Recorder.Event(obj, eventType, reason, message)
}

func (r *FunctionPodRequestReconciler) writeError(
	ctx context.Context,
	request *v1alpha1.FunctionPodRequest,
	phase v1alpha1.FunctionPodRequestPhase,
	message string,
) error {
	if request.Annotations == nil {
		request.Annotations = map[string]string{}
	}
	if request.Annotations[fnpod.AnnotationErrorSince] == "" {
		request.Annotations[fnpod.AnnotationErrorSince] = time.Now().UTC().Format(time.RFC3339)
		if err := r.Update(ctx, request); err != nil {
			return err
		}
	}
	request.Status.Phase = phase
	request.Status.Message = message
	if err := r.Status().Update(ctx, request); err != nil {
		return fmt.Errorf("update status of %s: %w", request.Name, err)
	}
	return nil
}

func phaseOf(request *v1alpha1.FunctionPodRequest) v1alpha1.FunctionPodRequestPhase {
	if request.Status.Phase == "" {
		return v1alpha1.FunctionPodRequestPhaseCreated
	}
	return request.Status.Phase
}
