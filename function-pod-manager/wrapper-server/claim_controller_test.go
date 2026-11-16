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
)

func testImageKey(t *testing.T, repo string) string {
	t.Helper()
	key, err := fnpod.ImageKey(repo)
	require.NoError(t, err)
	return key
}

func TestClaimControllerTryClaim(t *testing.T) {
	const repo = "registry.example.com/fn"
	key := testImageKey(t, repo)
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

func TestClaimControllerSkipsWrongVersion(t *testing.T) {
	const repo = "registry.example.com/fn"
	key := testImageKey(t, repo)
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
	const repo = "registry.example.com/fn"
	key := testImageKey(t, repo)
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
	const repo = "registry.example.com/fn"
	key := testImageKey(t, repo)
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