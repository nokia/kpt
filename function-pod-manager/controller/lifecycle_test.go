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
	"testing"
	"time"

	"github.com/kptdev/kpt/api/runneroptions"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestSoonestPositivePicksTheEarlierDelay(t *testing.T) {
	assert.Equal(t, time.Second, soonestPositive(0, time.Second))
	assert.Equal(t, time.Second, soonestPositive(time.Second, 0))
	assert.Equal(t, time.Second, soonestPositive(time.Second, 2*time.Second))
	assert.Equal(t, time.Second, soonestPositive(2*time.Second, time.Second))
}

func TestFreshRequestWaitsForTheDebounce(t *testing.T) {
	request := newRequest("fresh", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	request.CreationTimestamp = metav1.Now()
	deadline := metav1.NewTime(time.Now().Add(time.Hour))
	request.Spec.Deadline = &deadline
	reconciler, _, _ := newHarness(t, request)

	result, err := reconciler.Reconcile(context.Background(), namespacedRequest("fresh"))

	require.NoError(t, err)
	assert.Greater(t, result.RequeueAfter, time.Duration(0))
	assert.LessOrEqual(t, result.RequeueAfter, reconciler.Debounce)
}

func TestUnschedulablePodFailsTheRequest(t *testing.T) {
	request := newRequest("unschedulable", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("pending")
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodPending,
		Conditions: []corev1.PodCondition{{
			Type:    corev1.PodScheduled,
			Status:  corev1.ConditionFalse,
			Reason:  corev1.PodReasonUnschedulable,
			Message: "no nodes",
		}},
	}
	reconciler, kube, events := newHarness(t, request, pod)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("unschedulable"))

	require.NoError(t, err)
	assert.Equal(t, v1alpha1.FunctionPodRequestPhasePodManagementError, currentPhase(t, kube, "unschedulable"))
	failed := eventsFor(t, events, fnpod.EventStartFailed)
	require.NotEmpty(t, failed)
	assert.Contains(t, failed[0], corev1.PodReasonUnschedulable)
}

func TestPendingPodPastTheReadyTimeoutFailsTheRequest(t *testing.T) {
	request := newRequest("slow", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("slow-pod")
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Minute))
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	reconciler, kube, _ := newHarness(t, request, pod)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("slow"))

	require.NoError(t, err)
	assert.Equal(t, v1alpha1.FunctionPodRequestPhasePodManagementError, currentPhase(t, kube, "slow"))
	assert.Contains(t, currentRequest(t, kube, "slow").Status.Message, "timed out")
}

func TestFinishedPodFailsTheRequest(t *testing.T) {
	request := newRequest("finished", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	pod := readyPod("finished-pod")
	pod.Status = corev1.PodStatus{Phase: corev1.PodSucceeded}
	reconciler, kube, _ := newHarness(t, request, pod)

	_, err := reconciler.Reconcile(context.Background(), namespacedRequest("finished"))

	require.NoError(t, err)
	assert.Contains(t, currentRequest(t, kube, "finished").Status.Message, string(corev1.PodSucceeded))
}

func TestRequestsForPodEnqueuesMatchingRequests(t *testing.T) {
	match := newRequest("match", "~1.0.3", v1alpha1.FunctionPodRequestPhaseCreated)
	other := newRequest("other", "", v1alpha1.FunctionPodRequestPhaseCreated)
	other.Spec.Image = "not a valid image"
	reconciler, _, _ := newHarness(t, match, other)
	pod := readyPod("eval")

	got := reconciler.requestsForPod(context.Background(), pod)

	require.Len(t, got, 1)
	assert.Equal(t, "match", got[0].Name)
	assert.Nil(t, reconciler.requestsForPod(context.Background(), &corev1.Service{}))
	pod.Namespace = "elsewhere"
	assert.Nil(t, reconciler.requestsForPod(context.Background(), pod))
	pod.Namespace = testNamespace
	pod.Annotations = nil
	assert.Nil(t, reconciler.requestsForPod(context.Background(), pod))
}

func TestRequestsForPodReturnsNothingWhenTheListFails(t *testing.T) {
	reconciler, kube, _ := newHarness(t)
	reconciler.Client = interceptor.NewClient(mustWatch(t, kube), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return assert.AnError
		},
	})

	assert.Nil(t, reconciler.requestsForPod(context.Background(), readyPod("eval")))
}

func TestFunctionConfigReconcileWarmsAConcreteTag(t *testing.T) {
	cfg := functionConfig(1, 2, []string{"v1.2.3"})
	cfg.Spec.PodExecutor.TimeToLive = metav1.Duration{Duration: time.Hour}
	reconciler, kube, _ := newHarness(t, cfg)
	configs := &FunctionConfigReconciler{Client: kube, Pods: reconciler, Store: reconciler.FunctionConfigStore, Warmup: true}

	_, err := configs.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: testNamespace, Name: cfg.Name,
	}})

	require.NoError(t, err)
	assert.Equal(t, []string{"v1.2.3"}, versions(t, kube))
}

func TestFunctionConfigReconcileSkipsWhenWarmupIsOffOrTheObjectIsGone(t *testing.T) {
	reconciler, kube, _ := newHarness(t)
	configs := &FunctionConfigReconciler{Client: kube, Pods: reconciler, Store: reconciler.FunctionConfigStore, Warmup: false}

	result, err := configs.Reconcile(context.Background(), namespacedRequest("missing"))
	require.NoError(t, err)
	assert.Zero(t, result)

	configs.Warmup = true
	_, err = configs.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "other", Name: "missing"}})
	require.NoError(t, err)
	_, err = configs.Reconcile(context.Background(), namespacedRequest("missing"))
	require.NoError(t, err)
	assert.Empty(t, evaluatorPods(t, kube))
}

func TestFunctionConfigReconcileReturnsAListError(t *testing.T) {
	cfg := functionConfig(0, 0, []string{"v1.2.3"})
	reconciler, kube, _ := newHarness(t, cfg)
	failing := interceptor.NewClient(mustWatch(t, kube), interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, list client.ObjectList, _ ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return assert.AnError
			}
			return nil
		},
	})
	configs := &FunctionConfigReconciler{Client: failing, Pods: reconciler, Store: reconciler.FunctionConfigStore, Warmup: true}
	reconciler.Client = failing

	_, err := configs.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: testNamespace, Name: cfg.Name,
	}})

	require.ErrorIs(t, err, assert.AnError)
}

func TestWildcardFunctionConfigSuppliesTheLimits(t *testing.T) {
	cfg := functionConfig(4, 3, []string{"*"})
	cfg.Spec.PodExecutor.TimeToLive = metav1.Duration{Duration: 15 * time.Minute}
	defaults := PodPoolPolicy{PreferredMaxQueueLength: 2, MaxParallelExecutions: 1, PodTTL: time.Hour}
	cache := NewFunctionConfigStore()
	cache.upsert(cfg)

	got := cache.getPodPoolPolicy(testKrmFunction, "v9.9.9", runneroptions.GHCRImagePrefix, defaults)

	assert.Equal(t, 4, got.PreferredMaxQueueLength)
	assert.Equal(t, 3, got.MaxParallelExecutions)
	assert.Equal(t, 15*time.Minute, got.PodTTL)
	assert.False(t, containsStar(nil))
	assert.Nil(t, NewFunctionConfigStore().executor(testKrmFunction, "v1", runneroptions.GHCRImagePrefix))
}

func TestGarbageCollectorStartStopsWhenTheContextEnds(t *testing.T) {
	reconciler, _, _ := newHarness(t)
	collector := &GarbageCollector{
		FunctionPodRequestReconciler: reconciler,
		ScanInterval:                 time.Hour,
		WarmupOnStart:                false,
	}
	assert.True(t, collector.NeedLeaderElection())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, collector.Start(ctx))
}

func TestGarbageCollectorStartLogsAFailedScan(t *testing.T) {
	reconciler, kube, _ := newHarness(t)
	reconciler.Client = interceptor.NewClient(mustWatch(t, kube), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return assert.AnError
		},
	})
	collector := &GarbageCollector{
		FunctionPodRequestReconciler: reconciler,
		ScanInterval:                 time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	require.NoError(t, collector.Start(ctx))
}

func TestIdleStampThatCannotBeParsedIsLeftAlone(t *testing.T) {
	pod := readyPod("bad-stamp")
	pod.Annotations[fnpod.AnnotationIdleSince] = "not-a-time"
	reconciler, kube, _ := newHarness(t, pod)
	collector := &GarbageCollector{FunctionPodRequestReconciler: reconciler}

	require.NoError(t, collector.Tick(context.Background()))

	stored := &corev1.Pod{}
	require.NoError(t, kube.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "bad-stamp"}, stored))
	assert.Equal(t, "not-a-time", stored.Annotations[fnpod.AnnotationIdleSince])
}

func TestDeletingAnIdlePodWithoutAnImageLabelSkipsTheService(t *testing.T) {
	pod := readyPod("no-label")
	pod.Labels = nil
	pod.Annotations[fnpod.AnnotationIdleSince] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	reconciler, kube, _ := newHarness(t, pod)
	collector := &GarbageCollector{FunctionPodRequestReconciler: reconciler}

	require.NoError(t, collector.Tick(context.Background()))

	err := kube.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "no-label"}, &corev1.Pod{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestControllersRegisterWithTheManager(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, functionconfig.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(&rest.Config{Host: "https://127.0.0.1:1"}, ctrl.Options{Scheme: scheme})
	require.NoError(t, err)
	reconciler, _, _ := newHarness(t)

	require.NoError(t, reconciler.SetupWithManager(mgr))
	require.NoError(t, (&FunctionConfigReconciler{Client: reconciler.Client, Pods: reconciler}).SetupWithManager(mgr))
}

func mustWatch(t *testing.T, kube client.Client) client.WithWatch {
	t.Helper()
	watching, ok := kube.(client.WithWatch)
	require.True(t, ok)
	return watching
}

func TestReconcileIgnoresARequestOutsideTheNamespace(t *testing.T) {
	reconciler, _, _ := newHarness(t)
	result, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "other", Name: "missing"}})
	require.NoError(t, err)
	assert.Zero(t, result)

	_, err = reconciler.Reconcile(context.Background(), namespacedRequest("missing"))
	require.NoError(t, err)
}
