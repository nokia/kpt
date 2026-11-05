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
	"time"

	functionconfig "github.com/kptdev/kpt/function-pod-manager/api/functionconfig/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner/util"
	corev1 "k8s.io/api/core/v1"
)

type scaleAction int

const (
	scaleNone scaleAction = iota
	scaleCreatePod
	scalePodFailed
)

type scaleResult struct {
	action     scaleAction
	repository string
	version    string
	message    string
	pod        *corev1.Pod
}

func scaleRequest(
	request *v1alpha1.FunctionPodRequest,
	requests []v1alpha1.FunctionPodRequest,
	pods []corev1.Pod,
	configs []functionconfig.FunctionConfig,
	prefix string,
	defaults Limits,
	now time.Time,
) scaleResult {
	repository, err := repositoryOf(request.Spec.Image, prefix)
	if err != nil {
		return scaleResult{action: scalePodFailed, message: err.Error()}
	}
	related := podsForRepository(pods, repository)
	healthy, failed := splitPods(related, now, defaults.PodReadyTimeout)
	satisfying := podsSatisfying(healthy, request.Spec.VersionConstraint)
	if len(satisfying) > 0 {
		return scaleExisting(requests, satisfying, healthy, configs, repository, prefix, defaults)
	}
	if failedPod, message, ok := failedSatisfying(failed, request.Spec.VersionConstraint, now, defaults.PodReadyTimeout); ok {
		return scaleResult{action: scalePodFailed, repository: repository, message: message, pod: failedPod}
	}
	if request.Spec.VersionConstraint == "" {
		version, err := util.Tag(util.ResolveImage(request.Spec.Image, prefix))
		if err != nil || version == "" {
			version = "latest"
		}
		return scaleResult{action: scaleCreatePod, repository: repository, version: version}
	}
	return scaleResult{action: scaleCreatePod, repository: repository}
}

func scaleExisting(
	requests []v1alpha1.FunctionPodRequest,
	satisfying, healthy []corev1.Pod,
	configs []functionconfig.FunctionConfig,
	repository, prefix string,
	defaults Limits,
) scaleResult {
	version := util.HighestVersion(versionsOf(satisfying))
	matched := &satisfying[0]
	if version == "" {
		return scaleResult{action: scaleNone, repository: repository, pod: matched}
	}
	limits := limitsFor(configs, repository, version, prefix, defaults)
	pending := pendingAgainst(requests, satisfying, repository, prefix)
	if pending > limits.PreferredQueue && len(healthy) < limits.MaxParallel {
		return scaleResult{action: scaleCreatePod, repository: repository, version: version}
	}
	return scaleResult{action: scaleNone, repository: repository, pod: matched}
}

func splitPods(pods []corev1.Pod, now time.Time, timeout time.Duration) (healthy, failed []corev1.Pod) {
	for i := range pods {
		if _, bad := podProblem(&pods[i], now, timeout); bad {
			failed = append(failed, pods[i])
			continue
		}
		healthy = append(healthy, pods[i])
	}
	return healthy, failed
}

func failedSatisfying(failed []corev1.Pod, constraint string, now time.Time, timeout time.Duration) (*corev1.Pod, string, bool) {
	var chosen *corev1.Pod
	for i := range failed {
		if !util.VersionSatisfies(failed[i].Annotations[fnpod.AnnotationResolvedVersion], constraint) {
			continue
		}
		if chosen == nil || failed[i].CreationTimestamp.After(chosen.CreationTimestamp.Time) {
			chosen = &failed[i]
		}
	}
	if chosen == nil {
		return nil, "", false
	}
	message, _ := podProblem(chosen, now, timeout)
	if message == "" {
		message = string(chosen.Status.Phase)
	}
	return chosen, message, true
}

func podsForRepository(pods []corev1.Pod, repository string) []corev1.Pod {
	var out []corev1.Pod
	for i := range pods {
		if pods[i].Annotations[fnpod.AnnotationFunctionImage] == repository {
			out = append(out, pods[i])
		}
	}
	return out
}

func podsSatisfying(pods []corev1.Pod, constraint string) []corev1.Pod {
	var out []corev1.Pod
	for i := range pods {
		if util.VersionSatisfies(pods[i].Annotations[fnpod.AnnotationResolvedVersion], constraint) {
			out = append(out, pods[i])
		}
	}
	return out
}

func versionsOf(pods []corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for i := range pods {
		out = append(out, pods[i].Annotations[fnpod.AnnotationResolvedVersion])
	}
	return out
}

func pendingAgainst(requests []v1alpha1.FunctionPodRequest, satisfying []corev1.Pod, repository, prefix string) int {
	pending := 0
	for i := range requests {
		if phaseOf(&requests[i]) != v1alpha1.FunctionPodRequestPhaseCreated {
			continue
		}
		repo, err := repositoryOf(requests[i].Spec.Image, prefix)
		if err != nil || repo != repository {
			continue
		}
		if len(podsSatisfying(satisfying, requests[i].Spec.VersionConstraint)) > 0 {
			pending++
		}
	}
	return pending
}

func repositoryOf(image, prefix string) (string, error) {
	return util.Repository(util.ResolveImage(image, prefix))
}

func podProblem(pod *corev1.Pod, now time.Time, timeout time.Duration) (string, bool) {
	if podReady(pod) {
		return "", false
	}
	phase := pod.Status.Phase
	if phase == "" {
		phase = corev1.PodPending
	}
	if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
		return string(phase), true
	}
	if reason := waitingReason(pod); hardFailure(reason) {
		return string(phase) + ": " + reason, true
	}
	if reason := unschedulableReason(pod); reason != "" {
		return string(phase) + ": " + reason, true
	}
	if timeout > 0 && !pod.CreationTimestamp.IsZero() && now.Sub(pod.CreationTimestamp.Time) > timeout {
		return string(phase) + ": timed out waiting to be Ready", true
	}
	return "", false
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func waitingReason(pod *corev1.Pod) string {
	statuses := append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
			return status.State.Waiting.Reason
		}
		if status.State.Terminated != nil && status.State.Terminated.Reason != "" {
			return status.State.Terminated.Reason
		}
	}
	return ""
}

func hardFailure(reason string) bool {
	switch reason {
	case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "CreateContainerConfigError",
		"CrashLoopBackOff", "ImageInspectError", "ErrImageNeverPull":
		return true
	default:
		return false
	}
}

func unschedulableReason(pod *corev1.Pod) string {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable {
			return condition.Reason
		}
	}
	return ""
}
