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

package main

import (
	"context"
	"testing"
	"time"

	fnreqv1alpha1 "github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const testRepository = "registry.example.com/fn"

func testImageKey(t *testing.T) string {
	t.Helper()
	key, err := fnpod.ImageKey(testRepository)
	require.NoError(t, err)
	return key
}

func TestClaimControllerTryClaim(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	fr := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{
			Name:      "req-a",
			Namespace: "ns",
			Labels:    map[string]string{fnpod.LabelImageKey: key},
		},
		Spec: fnreqv1alpha1.FunctionPodRequestSpec{
			Image: repo + ":v1",
		},
		Status: fnreqv1alpha1.FunctionPodRequestStatus{
			Phase: fnreqv1alpha1.FunctionPodRequestPhaseCreated,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fr).WithObjects(fr).Build()

	c := &ClaimController{
		Client:           cl,
		Namespace:        "ns",
		PodName:          "pod-1",
		PodIP:            "10.0.0.1",
		GRPCPort:         9446,
		FunctionImage:    repo + ":v1",
		FunctionBaseName: "fn",
		ImageKey:         key,
		ResolvedVersion:  "1.0.0",
	}

	ctx := context.Background()
	require.NoError(t, c.tryClaim(ctx, fr))

	updated := &fnreqv1alpha1.FunctionPodRequest{}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(fr), updated))
	require.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseClaimed, updated.Status.Phase)
	require.Equal(t, "pod-1", updated.Status.EvaluatorPodName)
	require.Equal(t, int32(9446), updated.Status.EvaluatorPort)
	require.Equal(t, "1.0.0", updated.Status.ResolvedVersion)
}

func TestClaimControllerClaimsEmptyPhase(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))
	fr := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{Name: "req-empty", Namespace: "ns", Labels: map[string]string{fnpod.LabelImageKey: key}},
		Spec:       fnreqv1alpha1.FunctionPodRequestSpec{Image: repo + ":v0.1.5", VersionConstraint: "v0.1.5"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fr).WithObjects(fr).Build()
	c := &ClaimController{
		Client: cl, Namespace: "ns", PodName: "pod-1", FunctionBaseName: "fn", ImageKey: key,
		ResolvedVersion: "v0.1.5", GRPCPort: 9446,
	}
	require.NoError(t, c.tryClaim(context.Background(), fr))
	updated := &fnreqv1alpha1.FunctionPodRequest{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(fr), updated))
	require.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseClaimed, updated.Status.Phase)
}

func TestClaimControllerSkipsWrongVersion(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	fr := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{Name: "req-v2", Namespace: "ns", Labels: map[string]string{fnpod.LabelImageKey: key}},
		Spec: fnreqv1alpha1.FunctionPodRequestSpec{
			Image:             repo + ":v2",
			VersionConstraint: "~2.0.0",
		},
		Status: fnreqv1alpha1.FunctionPodRequestStatus{Phase: fnreqv1alpha1.FunctionPodRequestPhaseCreated},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fr).WithObjects(fr).Build()
	c := &ClaimController{
		Client: cl, Namespace: "ns", PodName: "pod-1", FunctionBaseName: "fn", ImageKey: key,
		ResolvedVersion: "1.0.0",
	}
	require.Error(t, c.tryClaim(context.Background(), fr))
}

func TestClaimControllerValidateEvalRequest(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	fr := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{Name: "req-b", Namespace: "ns"},
		Spec: fnreqv1alpha1.FunctionPodRequestSpec{
			Image: "registry.example.com/fn:v1",
		},
		Status: fnreqv1alpha1.FunctionPodRequestStatus{
			Phase:            fnreqv1alpha1.FunctionPodRequestPhaseClaimed,
			EvaluatorPodName: "pod-1",
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fr).WithObjects(fr).Build()

	c := &ClaimController{Client: cl, Namespace: "ns", PodName: "pod-1"}
	ctx := context.Background()

	require.NoError(t, c.ValidateEvalRequest(ctx, "req-b"))
	require.Error(t, c.ValidateEvalRequest(ctx, "missing"))
}

func TestAdoptClaimsOnStartup(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	fr := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{Name: "req-c", Namespace: "ns", Labels: map[string]string{fnpod.LabelImageKey: key}},
		Spec:       fnreqv1alpha1.FunctionPodRequestSpec{Image: repo + ":v1"},
		Status: fnreqv1alpha1.FunctionPodRequestStatus{
			Phase:            fnreqv1alpha1.FunctionPodRequestPhaseClaimed,
			EvaluatorPodName: "pod-1",
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fr).WithObjects(fr).Build()

	c := &ClaimController{Client: cl, Namespace: "ns", PodName: "pod-1", ImageKey: key}
	require.NoError(t, c.AdoptClaimsOnStartup(context.Background()))
	require.NotNil(t, c.activeClaim)
	require.Equal(t, "req-c", c.activeClaim.name)
}

func TestClaimGraceReleasesToCreated(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	fr := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{Name: "req-d", Namespace: "ns", Labels: map[string]string{fnpod.LabelImageKey: key}},
		Spec:       fnreqv1alpha1.FunctionPodRequestSpec{Image: repo + ":v1"},
		Status:     fnreqv1alpha1.FunctionPodRequestStatus{Phase: fnreqv1alpha1.FunctionPodRequestPhaseCreated},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fr).WithObjects(fr).Build()

	c := &ClaimController{
		Client:           cl,
		Namespace:        "ns",
		PodName:          "pod-1",
		FunctionImage:    repo + ":v1",
		FunctionBaseName: "fn",
		ImageKey:         key,
		ResolvedVersion:  "1.0.0",
		ClaimGracePeriod: time.Millisecond,
	}
	ctx := context.Background()
	require.NoError(t, c.tryClaim(ctx, fr))
	c.activeClaim = &activeClaimState{name: "req-d", claimedAt: time.Now().Add(-time.Hour)}

	_, err := c.runClaimCycle(ctx)
	require.NoError(t, err)
	updated := &fnreqv1alpha1.FunctionPodRequest{}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(fr), updated))
	require.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseCreated, updated.Status.Phase)
	require.Empty(t, updated.Status.EvaluatorPodName)
}

func TestRunClaimCycleDoesNotClaimAnotherRequestWhenItAlreadyHoldsOne(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	older := claimFixture(key, "fnreq-older", time.Now().Add(-time.Minute))
	older.Status.Phase = fnreqv1alpha1.FunctionPodRequestPhaseClaimed
	older.Status.EvaluatorPodName = "pod-1"
	newer := claimFixture(key, "fnreq-newer", time.Now())
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&fnreqv1alpha1.FunctionPodRequest{}).WithObjects(older, newer).Build()

	c := &ClaimController{
		Client:           cl,
		Namespace:        "ns",
		PodName:          "pod-1",
		FunctionImage:    repo + ":v1",
		FunctionBaseName: "fn",
		ImageKey:         key,
		ResolvedVersion:  "1.0.0",
		activeClaim:      &activeClaimState{name: older.Name, claimedAt: time.Now()},
	}

	_, err := c.runClaimCycle(context.Background())
	require.NoError(t, err)
	require.Equal(t, older.Name, c.activeClaim.name)

	storedNewer := &fnreqv1alpha1.FunctionPodRequest{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(newer), storedNewer))
	require.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseCreated, storedNewer.Status.Phase)
	require.Empty(t, storedNewer.Status.EvaluatorPodName)
}

func TestRunClaimCycleStopsWhenTheFreshRequestIsAlreadyClaimedByThisPod(t *testing.T) {
	const repo = testRepository
	key := testImageKey(t)
	scheme := runtime.NewScheme()
	require.NoError(t, fnreqv1alpha1.AddToScheme(scheme))

	older := claimFixture(key, "fnreq-older", time.Now().Add(-time.Minute))
	newer := claimFixture(key, "fnreq-newer", time.Now())
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&fnreqv1alpha1.FunctionPodRequest{}).WithObjects(older.DeepCopy(), newer.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, raw client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name != older.Name {
					return raw.Get(ctx, key, obj, opts...)
				}
				held := older.DeepCopy()
				held.Status.Phase = fnreqv1alpha1.FunctionPodRequestPhaseClaimed
				held.Status.EvaluatorPodName = "pod-1"
				*obj.(*fnreqv1alpha1.FunctionPodRequest) = *held
				return nil
			},
		}).Build()

	c := &ClaimController{
		Client:           cl,
		Namespace:        "ns",
		PodName:          "pod-1",
		FunctionImage:    repo + ":v1",
		FunctionBaseName: "fn",
		ImageKey:         key,
		ResolvedVersion:  "1.0.0",
	}

	_, err := c.runClaimCycle(context.Background())
	require.NoError(t, err)
	require.Equal(t, older.Name, c.activeClaim.name)

	storedNewer := &fnreqv1alpha1.FunctionPodRequest{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(newer), storedNewer))
	require.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseCreated, storedNewer.Status.Phase)
}

func claimFixture(imageKey, name string, created time.Time) *fnreqv1alpha1.FunctionPodRequest {
	return &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: v1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			Labels:            map[string]string{fnpod.LabelImageKey: imageKey},
			CreationTimestamp: v1.NewTime(created),
		},
		Spec: fnreqv1alpha1.FunctionPodRequestSpec{Image: testRepository + ":v1"},
		Status: fnreqv1alpha1.FunctionPodRequestStatus{
			Phase: fnreqv1alpha1.FunctionPodRequestPhaseCreated,
		},
	}
}
