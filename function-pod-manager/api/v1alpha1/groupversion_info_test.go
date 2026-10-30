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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestAllKindsRegistersFunctionPodRequest(t *testing.T) {
	t.Parallel()

	// given
	scheme := runtime.NewScheme()
	wantGVK := schema.GroupVersionKind{
		Group:   "kpt.dev",
		Version: "v1alpha1",
		Kind:    "FunctionPodRequest",
	}

	// when
	err := AddToScheme(scheme)

	// then
	require.NoError(t, err)
	require.Equal(t, []TypeInfo{TypeFunctionPodRequest}, AllKinds)
	require.Equal(t, wantGVK, TypeFunctionPodRequest.GVK())
	require.Equal(t, "kpt.dev/v1alpha1", TypeFunctionPodRequest.APIVersion())
	require.True(t, scheme.Recognizes(wantGVK))
	require.True(t, scheme.Recognizes(wantGVK.GroupVersion().WithKind("FunctionPodRequestList")))
}
