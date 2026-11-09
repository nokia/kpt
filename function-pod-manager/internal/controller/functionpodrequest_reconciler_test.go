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
	"strings"
	"testing"
	"time"

	"github.com/kptdev/kpt/api/runneroptions"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace   = "fn-test"
	testKrmFunction = "ghcr.io/some-krm-function"
)

func TestSatisfyingPodLeavesRequestCreated(t *testing.T) {
	req := newRequest("keep", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("eval-a")
	reconciler, kube, events := newHarness(t, req, pod)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("keep"))
	require.NoError(t, err)

	assert.Equal(t, v1alpha1.FunctionPodRequestPhaseCreated, currentPhase(t, kube, "keep"))
	assert.Len(t, evaluatorPods(t, kube), 1)
	available := eventsFor(t, events, fnpod.EventPodAvailable)
	require.Len(t, available, 1)
	assert.Contains(t, available[0], "An existing evaluator can serve this request")
	assert.Equal(t, "true", currentRequest(t, kube, "keep").Annotations[fnpod.AnnotationMatchedPod])

	_, err = reconciler.Reconcile(context.Background(), namespacedRequest("keep"))
	require.NoError(t, err)
	assert.Empty(t, eventsFor(t, events, fnpod.EventPodAvailable))
}

func TestDifferentConstraintStartsPodAtHighestTag(t *testing.T) {
	req := newRequest("next", "~1.1.0", "")
	pod := readyPod("eval-a")
	reconciler, kube, events := newHarness(t, req, pod)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("next"))
	require.NoError(t, err)

	versions := versions(t, kube)
	assert.Contains(t, versions, "v1.1.2")
	assert.Contains(t, versions, "v1.0.7")
	started := eventsFor(t, events, fnpod.EventStarting)
	require.Len(t, started, 2)
	assert.Contains(t, started[0], "Starting new pod for "+displayName(testKrmFunction, "v1.1.2"))
	assert.Contains(t, started[1], "Started evaluator pod")
	assert.Contains(t, started[1], displayName(testKrmFunction, "v1.1.2"))
	assert.NotContains(t, started[1], "~1.1.0")
}

func TestQueueStartsAnotherPodAtTheSameVersion(t *testing.T) {
	first := newRequest("one", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	second := newRequest("two", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("eval-a")
	cfg := functionConfig(1, 2, []string{"*"})
	reconciler, kube, _ := newHarness(t, first, second, pod, cfg)
	reconciler.Limits.PreferredQueue = 1
	reconciler.Limits.MaxParallel = 2

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("one"))
	require.NoError(t, err)

	assert.Equal(t, []string{"v1.0.7", "v1.0.7"}, versions(t, kube))
}

func TestQueueRespectsParallelCap(t *testing.T) {
	first := newRequest("one", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	second := newRequest("two", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("eval-a")
	reconciler, kube, _ := newHarness(t, first, second, pod)
	reconciler.Limits.PreferredQueue = 1
	reconciler.Limits.MaxParallel = 1

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("one"))
	require.NoError(t, err)
	assert.Equal(t, []string{"v1.0.7"}, versions(t, kube))
}

func TestUnsatisfiableConstraintSetsPodManagementError(t *testing.T) {
	req := newRequest("missing", "~9.0.0", v1alpha1.FunctionPodRequestPhaseCreated)
	reconciler, kube, events := newHarness(t, req)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("missing"))
	require.NoError(t, err)

	stored := currentRequest(t, kube, "missing")
	assert.Equal(t, v1alpha1.FunctionPodRequestPhasePodManagementError, stored.Status.Phase)
	assert.Contains(t, stored.Status.Message, testKrmFunction)
	assert.Contains(t, stored.Status.Message, "~9.0.0")
	recorded := eventsFor(t, events, fnpod.EventUnsatisfiable)
	require.Len(t, recorded, 1)
	assert.Contains(t, recorded[0], "Cannot start pod for "+displayName(testKrmFunction, "")+": no published tag satisfies ~9.0.0")
	assert.Empty(t, evaluatorPods(t, kube))
}

func TestImagePullFailureSetsPodManagementError(t *testing.T) {
	req := newRequest("pull", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("eval-a")
	pod.Status.Conditions = nil
	pod.Status.Phase = corev1.PodPending
	pod.CreationTimestamp = metav1.Now()
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  fnpod.ContainerFunction,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
	}}
	reconciler, kube, events := newHarness(t, req, pod)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("pull"))
	require.NoError(t, err)

	stored := currentRequest(t, kube, "pull")
	assert.Equal(t, v1alpha1.FunctionPodRequestPhasePodManagementError, stored.Status.Phase)
	assert.Contains(t, stored.Status.Message, "ImagePullBackOff")
	recorded := eventsFor(t, events, fnpod.EventStartFailed)
	require.NotEmpty(t, recorded)
	assert.Contains(t, recorded[0], "ImagePullBackOff")
}

func TestDeadlineMovesCreatedAndClaimedRequests(t *testing.T) {
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	created := newRequest("created", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	created.Spec.Deadline = &past
	claimed := newRequest("claimed", "~1.0.3", v1alpha1.FunctionPodRequestPhaseClaimed)
	claimed.Spec.Deadline = &past
	claimed.Status.EvaluatorPodName = "eval-a"
	missing := newRequest("missing-pod", "~1.0.3", v1alpha1.FunctionPodRequestPhaseClaimed)
	missing.Status.EvaluatorPodName = "gone"
	reconciler, kube, _ := newHarness(t, created, claimed, missing, readyPod("eval-a"))

	for _, name := range []string{"created", "claimed", "missing-pod"} {
		_, err := reconciler.Reconcile(context.Background(), namespacedRequest(name))
		require.NoError(t, err)
	}

	assert.Equal(t, v1alpha1.FunctionPodRequestPhasePodManagementError, currentPhase(t, kube, "created"))
	assert.Equal(t, v1alpha1.FunctionPodRequestPhaseEvaluationError, currentPhase(t, kube, "claimed"))
	assert.Equal(t, v1alpha1.FunctionPodRequestPhaseEvaluationError, currentPhase(t, kube, "missing-pod"))
	assert.NotNil(t, currentRequest(t, kube, "created"))
}

func TestErrorRequestWithoutStampIsKept(t *testing.T) {
	stuck := newRequest("stuck", "~1.0.3", v1alpha1.FunctionPodRequestPhasePodManagementError)
	reconciler, kube, _ := newHarness(t, stuck)
	collector := &GarbageCollector{FunctionPodRequestReconciler: reconciler}

	require.NoError(t, collector.Tick(context.Background()))

	assert.Equal(t, v1alpha1.FunctionPodRequestPhasePodManagementError, currentPhase(t, kube, "stuck"))
}

func TestErrorRequestsAreDeletedAfterRetention(t *testing.T) {
	expired := newRequest("expired", "~1.0.3", v1alpha1.FunctionPodRequestPhasePodManagementError)
	expired.Annotations = map[string]string{
		fnpod.AnnotationErrorSince: time.Now().Add(-11 * time.Minute).UTC().Format(time.RFC3339),
	}
	fresh := newRequest("fresh", "~1.0.3", v1alpha1.FunctionPodRequestPhaseEvaluationError)
	fresh.Annotations = map[string]string{
		fnpod.AnnotationErrorSince: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
	}
	reconciler, kube, _ := newHarness(t, expired, fresh)
	collector := &GarbageCollector{FunctionPodRequestReconciler: reconciler}

	require.NoError(t, collector.Tick(context.Background()))

	err := kube.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "expired"}, &v1alpha1.FunctionPodRequest{})
	assert.True(t, apierrors.IsNotFound(err))
	assert.Equal(t, v1alpha1.FunctionPodRequestPhaseEvaluationError, currentPhase(t, kube, "fresh"))
}

func TestIdlePodIsDeletedAndClaimedPodIsKept(t *testing.T) {
	idle := readyPod("idle-pod")
	idle.Annotations[fnpod.AnnotationIdleSince] = time.Now().Add(-31 * time.Minute).UTC().Format(time.RFC3339)
	busy := readyPod("busy-pod")
	busy.Annotations[fnpod.AnnotationIdleSince] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	claimed := newRequest("claimed", "~1.0.3", v1alpha1.FunctionPodRequestPhaseClaimed)
	claimed.Status.EvaluatorPodName = "busy-pod"
	reconciler, kube, events := newHarness(t, idle, busy, claimed)
	collector := &GarbageCollector{FunctionPodRequestReconciler: reconciler}

	require.NoError(t, collector.Tick(context.Background()))

	err := kube.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "idle-pod"}, &corev1.Pod{})
	assert.True(t, apierrors.IsNotFound(err))
	require.NoError(t, kube.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "busy-pod"}, &corev1.Pod{}))
	deleted := eventsFor(t, events, fnpod.EventDeletedIdle)
	require.Len(t, deleted, 1)
	assert.Contains(t, deleted[0], displayName(testKrmFunction, "v1.0.7"))
}

func TestWarmupConcreteTagSkipsRange(t *testing.T) {
	concrete := functionConfig(0, 0, []string{"v1.2.3"})
	concrete.Name = "concrete"
	ranged := functionConfig(0, 0, []string{"~1.0.3"})
	ranged.Name = "ranged"
	ranged.Spec.Image = "ghcr.io/example/other-fn"
	reconciler, kube, _ := newHarness(t, concrete, ranged)
	collector := &GarbageCollector{FunctionPodRequestReconciler: reconciler, WarmupOnStart: true}

	require.NoError(t, collector.WarmupAll(context.Background()))

	got := versions(t, kube)
	assert.Equal(t, []string{"v1.2.3"}, got)
}

func eventsFor(t *testing.T, recorder *record.FakeRecorder, reason string) []string {
	t.Helper()
	var matched []string
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, " "+reason+" ") {
				matched = append(matched, event)
			}
		default:
			return matched
		}
	}
}

func newHarness(t *testing.T, objs ...client.Object) (*FunctionPodRequestReconciler, client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, functionconfig.AddToScheme(scheme))
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.FunctionPodRequest{}).WithObjects(objs...).Build()
	events := record.NewFakeRecorder(32)
	prov := provisioner.New(kube, provisioner.Config{
		Namespace:          testNamespace,
		WrapperServerImage: "wrapper:test",
		DefaultImagePrefix: runneroptions.GHCRImagePrefix,
		ImageRegistry: &provisioner.FakeImageRegistry{
			Tags: map[string][]string{
				testKrmFunction: {"v1.0.7", "v1.1.2", "latest"},
			},
			Digest:     "0123456789abcdef",
			Entrypoint: []string{"/fn"},
		},
	})
	reconciler := &FunctionPodRequestReconciler{
		Client:           kube,
		Recorder:         events,
		Provisioner:      prov,
		RequestNamespace: testNamespace,
		PodNamespace:     testNamespace,
		ImagePrefix:      runneroptions.GHCRImagePrefix,
		Debounce:         5 * time.Second,
		Retention:        10 * time.Minute,
		Limits: Limits{
			PreferredQueue:  2,
			MaxParallel:     1,
			PodTTL:          30 * time.Minute,
			PodReadyTimeout: 60 * time.Second,
		},
	}
	return reconciler, kube, events
}

func newRequest(name, constraint string, phase v1alpha1.FunctionPodRequestPhase) *v1alpha1.FunctionPodRequest {
	return &v1alpha1.FunctionPodRequest{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "FunctionPodRequest"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Spec: v1alpha1.FunctionPodRequestSpec{
			Image:             testKrmFunction,
			VersionConstraint: constraint,
		},
		Status: v1alpha1.FunctionPodRequestStatus{Phase: phase},
	}
}

func readyPod(name string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
			Labels:            map[string]string{fnpod.LabelImage: name + "-id"},
			Annotations: map[string]string{
				fnpod.AnnotationFunctionImage:   testKrmFunction,
				fnpod.AnnotationResolvedVersion: "v1.0.7",
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func functionConfig(queue, parallel int, tags []string) *functionconfig.FunctionConfig {
	return &functionconfig.FunctionConfig{
		TypeMeta:   metav1.TypeMeta{APIVersion: functionconfig.GroupVersion.String(), Kind: "FunctionConfig"},
		ObjectMeta: metav1.ObjectMeta{Name: "merge-netconf", Namespace: testNamespace},
		Spec: functionconfig.FunctionConfigSpec{
			Image: testKrmFunction,
			PodExecutor: &functionconfig.PodExecutorConfig{
				Tags:                    tags,
				PreferredMaxQueueLength: queue,
				MaxParallelExecutions:   parallel,
			},
		},
	}
}

func namespacedRequest(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name}}
}

func currentRequest(t *testing.T, kube client.Client, name string) *v1alpha1.FunctionPodRequest {
	t.Helper()
	stored := &v1alpha1.FunctionPodRequest{}
	require.NoError(t, kube.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, stored))
	return stored
}

func currentPhase(t *testing.T, kube client.Client, name string) v1alpha1.FunctionPodRequestPhase {
	t.Helper()
	return currentRequest(t, kube, name).Status.Phase
}

func evaluatorPods(t *testing.T, kube client.Client) []corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	require.NoError(t, kube.List(context.Background(), list, client.InNamespace(testNamespace)))
	var pods []corev1.Pod
	for i := range list.Items {
		if list.Items[i].Annotations[fnpod.AnnotationFunctionImage] != "" {
			pods = append(pods, list.Items[i])
		}
	}
	return pods
}

func versions(t *testing.T, kube client.Client) []string {
	t.Helper()
	pods := evaluatorPods(t, kube)
	out := make([]string, 0, len(pods))
	for i := range pods {
		out = append(out, pods[i].Annotations[fnpod.AnnotationResolvedVersion])
	}
	return out
}
