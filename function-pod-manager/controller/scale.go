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
	"strings"
	"time"

	"github.com/kptdev/kpt/api/utils/image"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/provisioner/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
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

// scaler is the snapshot scale uses to decide whether a request needs another evaluator pod.
type scaler struct {
	request  *v1alpha1.FunctionPodRequest
	requests []v1alpha1.FunctionPodRequest
	pods     []corev1.Pod
	configs  *FunctionConfigStore
	prefix   string
	defaults PodPoolPolicy
	now      time.Time
}

func (s scaler) scale() scaleResult {
	repository := repositoryOf(s.request.Spec.Image, s.prefix)
	constraint := fnpod.GetVersionConstraint(s.request.Spec.VersionConstraint, s.request.Spec.Image)
	related := podsForRepository(s.pods, repository)
	healthy, failed := splitPods(related, s.now, s.defaults.PodReadyTimeout)
	satisfying := podsSatisfying(healthy, constraint)
	if len(satisfying) > 0 {
		return s.existing(satisfying, healthy, repository)
	}
	if failedPod, message, ok := failedSatisfying(failed, constraint, s.now, s.defaults.PodReadyTimeout); ok {
		return scaleResult{action: scalePodFailed, repository: repository, message: message, pod: failedPod}
	}
	// No tag on the image and no spec.versionConstraint: provision the catalog default (latest).
	if constraint == "" {
		version := image.Parse(image.WithPrefix(s.request.Spec.Image, s.prefix)).Tag
		if version == "" {
			version = fnpod.LatestTag
		}
		return scaleResult{action: scaleCreatePod, repository: repository, version: version}
	}
	return scaleResult{action: scaleCreatePod, repository: repository}
}

func (s scaler) existing(satisfying, healthy []corev1.Pod, repository string) scaleResult {
	// Prefer the highest semver. A tag such as "latest" is still a concrete version the
	// next pod must repeat; dropping it skips scale-up and leaves a single evaluator.
	version := util.HighestVersion(versionsOf(satisfying))
	if version == "" {
		version = firstVersion(satisfying)
	}
	matched := &satisfying[0]
	if version == "" {
		return scaleResult{action: scaleNone, repository: repository, pod: matched}
	}
	podPoolPolicy := s.configs.getPodPoolPolicy(repository, version, s.prefix, s.defaults)
	pending := pendingAgainst(s.requests, satisfying, repository, s.prefix)
	create := pending > podPoolPolicy.PreferredMaxQueueLength && len(healthy) < podPoolPolicy.MaxParallelExecutions
	klog.InfoS("evaluator scale",
		"request", s.request.Name,
		"repository", repository,
		"version", version,
		"pending", pending,
		"preferredQueue", podPoolPolicy.PreferredMaxQueueLength,
		"healthy", len(healthy),
		"maxParallel", podPoolPolicy.MaxParallelExecutions,
		"create", create,
	)
	if create {
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

func constraintMatchesPod(concreteVersion, constraint string) bool {
	if strings.TrimSpace(constraint) == "" {
		return strings.TrimSpace(concreteVersion) != ""
	}
	return fnpod.VersionSatisfies(concreteVersion, constraint)
}

func failedSatisfying(failed []corev1.Pod, constraint string, now time.Time, timeout time.Duration) (*corev1.Pod, string, bool) {
	var chosen *corev1.Pod
	for i := range failed {
		if !constraintMatchesPod(failed[i].Annotations[fnpod.AnnotationResolvedVersion], constraint) {
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
		if constraintMatchesPod(pods[i].Annotations[fnpod.AnnotationResolvedVersion], constraint) {
			out = append(out, pods[i])
		}
	}
	return out
}

func firstVersion(pods []corev1.Pod) string {
	for i := range pods {
		if version := pods[i].Annotations[fnpod.AnnotationResolvedVersion]; version != "" {
			return version
		}
	}
	return ""
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
		if repositoryOf(requests[i].Spec.Image, prefix) != repository {
			continue
		}
		other := fnpod.GetVersionConstraint(requests[i].Spec.VersionConstraint, requests[i].Spec.Image)
		if len(podsSatisfying(satisfying, other)) > 0 {
			pending++
		}
	}
	return pending
}

func repositoryOf(name, prefix string) string {
	return image.Parse(image.WithPrefix(name, prefix)).Repository()
}

func podProblem(pod *corev1.Pod, now time.Time, timeout time.Duration) (string, bool) {
	if podReady(pod) {
		return "", false
	}
	phase := pod.Status.Phase
	if phase == "" {
		phase = corev1.PodPending
	}
	reason := waitingReason(pod)
	if !hardFailure(reason) {
		reason = unschedulableReason(pod)
	}
	switch {
	case phase == corev1.PodSucceeded || phase == corev1.PodFailed:
		return string(phase), true
	case reason != "":
		return string(phase) + ": " + reason, true
	case timeout > 0 && !pod.CreationTimestamp.IsZero() && now.Sub(pod.CreationTimestamp.Time) > timeout:
		return string(phase) + ": timed out waiting to be Ready", true
	default:
		return "", false
	}
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
