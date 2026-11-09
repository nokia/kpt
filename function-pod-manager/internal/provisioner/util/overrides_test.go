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

package util

import (
	"testing"

	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMergeTemplateOverridesPatchesTheFirstContainers(t *testing.T) {
	template := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: fnpod.ContainerInit}},
		Containers:     []corev1.Container{{Name: fnpod.ContainerFunction}},
	}}
	runAs := int64(1000)
	memory := resource.MustParse("256Mi")

	MergeTemplateOverrides(template, nil)
	assert.Empty(t, template.Spec.ServiceAccountName)

	MergeTemplateOverrides(template, &functionconfig.TemplateOverrides{
		ServiceAccountName: "fn",
		SecurityContext:    &corev1.PodSecurityContext{RunAsUser: &runAs},
		InitContainer: &functionconfig.ContainerOverrides{
			Env: []corev1.EnvVar{{Name: "INIT", Value: "1"}},
		},
		Container: &functionconfig.ContainerOverrides{
			Resources: &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: memory}},
			EnvFrom:   []corev1.EnvFromSource{{Prefix: "cfg"}},
			Env:       []corev1.EnvVar{{Name: "READY", Value: "1"}},
		},
	})

	assert.Equal(t, "fn", template.Spec.ServiceAccountName)
	require.NotNil(t, template.Spec.SecurityContext)
	assert.Equal(t, int64(1000), *template.Spec.SecurityContext.RunAsUser)
	assert.Equal(t, "1", template.Spec.InitContainers[0].Env[0].Value)
	assert.Equal(t, memory, template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory])
	assert.Equal(t, "cfg", template.Spec.Containers[0].EnvFrom[0].Prefix)
	assert.Equal(t, "READY", template.Spec.Containers[0].Env[0].Name)
}

func TestPatchFunctionContainerSetsGOMEMLIMITFromTheMemoryLimit(t *testing.T) {
	pod := functionPod()
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("200Mi"),
	}

	require.NoError(t, PatchFunctionContainer(pod, []string{"/fn"}, "example.com/fn:v1"))

	assert.Equal(t, "example.com/fn:v1", pod.Spec.Containers[0].Image)
	assert.Contains(t, pod.Spec.Containers[0].Args, "/fn")
	assert.True(t, hasEnv(pod.Spec.Containers[0].Env, gomemlimitEnvVar))
}

func TestPatchFunctionContainerKeepsAnExplicitGOMEMLIMIT(t *testing.T) {
	pod := functionPod()
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: gomemlimitEnvVar, Value: "already"}}
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("200Mi"),
	}

	require.NoError(t, PatchFunctionContainer(pod, nil, "example.com/fn:v1"))

	assert.Equal(t, "already", pod.Spec.Containers[0].Env[0].Value)
}

func TestPatchFunctionContainerSkipsGOMEMLIMITWhenTheLimitIsTooSmall(t *testing.T) {
	pod := functionPod()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("50Mi"),
	}

	require.NoError(t, PatchFunctionContainer(pod, nil, "example.com/fn:v1"))

	assert.False(t, hasEnv(pod.Spec.Containers[0].Env, gomemlimitEnvVar))
}

func TestPatchFunctionContainerReportsAMissingFunctionContainer(t *testing.T) {
	pod := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "sidecar"}},
	}}

	err := PatchFunctionContainer(pod, nil, "example.com/fn:v1")

	require.Error(t, err)
	assert.ErrorContains(t, err, fnpod.ContainerFunction)
}

func TestSetEnvReplacesAnExistingValue(t *testing.T) {
	env := []corev1.EnvVar{{Name: "PORT", Value: "1"}}

	setEnv(&env, "PORT", "2")

	assert.Equal(t, []corev1.EnvVar{{Name: "PORT", Value: "2"}}, env)
}

func functionPod() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: fnpod.ContainerFunction}},
	}}
}
