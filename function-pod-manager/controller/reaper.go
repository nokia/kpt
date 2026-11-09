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
	"errors"
	"fmt"
	"time"

	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func (r *FunctionPodRequestReconciler) reapOne(ctx context.Context, request *v1alpha1.FunctionPodRequest, pods []corev1.Pod, now time.Time) (bool, error) {
	phase := phaseOf(request)
	if phase != v1alpha1.FunctionPodRequestPhaseCreated && phase != v1alpha1.FunctionPodRequestPhaseClaimed {
		return false, nil
	}
	if deadlinePassed(request, now) && phase == v1alpha1.FunctionPodRequestPhaseCreated {
		const message = "deadline passed while Created"
		klog.InfoS("function pod request deadline passed", "request", request.Name, "namespace", request.Namespace, "phase", phase, "message", message)
		r.event(request, corev1.EventTypeWarning, fnpod.EventReaped, message)
		err := r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhasePodManagementError, message)
		return true, err
	}
	if phase != v1alpha1.FunctionPodRequestPhaseClaimed {
		return false, nil
	}
	if deadlinePassed(request, now) {
		const message = "deadline passed while Claimed"
		klog.InfoS("function pod request deadline passed", "request", request.Name, "namespace", request.Namespace, "phase", phase, "message", message)
		r.event(request, corev1.EventTypeWarning, fnpod.EventReaped, message)
		err := r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhaseEvaluationError, message)
		return true, err
	}
	if request.Status.EvaluatorPodName != "" && !podNamed(pods, request.Status.EvaluatorPodName) {
		message := fmt.Sprintf("evaluator pod %s no longer exists", request.Status.EvaluatorPodName)
		r.event(request, corev1.EventTypeWarning, fnpod.EventReaped, message)
		err := r.writeError(ctx, request, v1alpha1.FunctionPodRequestPhaseEvaluationError, message)
		return true, err
	}
	return false, nil
}

func deadlinePassed(request *v1alpha1.FunctionPodRequest, now time.Time) bool {
	return request.Spec.Deadline != nil && !request.Spec.Deadline.IsZero() && !request.Spec.Deadline.After(now)
}

func podNamed(pods []corev1.Pod, name string) bool {
	for i := range pods {
		if pods[i].Name == name {
			return true
		}
	}
	return false
}

func (gc *GarbageCollector) deleteExpiredErrors(ctx context.Context, requests []v1alpha1.FunctionPodRequest, now time.Time) error {
	errs := make([]error, len(requests))
	var group errgroup.Group
	group.SetLimit(gc.parallelism())
	for i := range requests {
		group.Go(func() error {
			errs[i] = gc.deleteOneExpired(ctx, &requests[i], now)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return errors.Join(errs...)
}

func (r *FunctionPodRequestReconciler) deleteOneExpired(ctx context.Context, request *v1alpha1.FunctionPodRequest, now time.Time) error {
	phase := request.Status.Phase
	if phase != v1alpha1.FunctionPodRequestPhasePodManagementError && phase != v1alpha1.FunctionPodRequestPhaseEvaluationError {
		return nil
	}
	since := request.Annotations[fnpod.AnnotationErrorSince]
	stamp, err := time.Parse(time.RFC3339, since)
	if err != nil {
		klog.ErrorS(err, "function pod request kept: missing or invalid error-since",
			"request", request.Name, "namespace", request.Namespace, "phase", phase, "errorSince", since)
		return nil
	}
	elapsed := now.Sub(stamp)
	if elapsed < r.Retention {
		klog.InfoS("function pod request waiting for retention", "request", request.Name, "namespace", request.Namespace, "phase", phase, "remaining", r.Retention-elapsed)
		return nil
	}
	if err := r.Delete(ctx, request); err != nil {
		return fmt.Errorf("delete expired request %s: %w", request.Name, err)
	}
	klog.InfoS("function pod request removed after retention", "request", request.Name, "namespace", request.Namespace, "phase", phase)
	return nil
}
