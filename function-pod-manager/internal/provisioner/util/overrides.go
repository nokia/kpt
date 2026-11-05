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
	"fmt"
	"slices"

	functionconfig "github.com/kptdev/kpt/function-pod-manager/api/functionconfig/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var gomemlimitSlack = resource.MustParse("100Mi")

func MergeTemplateOverrides(template *corev1.PodTemplateSpec, overrides *functionconfig.TemplateOverrides) {
	if overrides == nil {
		return
	}
	if overrides.ServiceAccountName != "" {
		template.Spec.ServiceAccountName = overrides.ServiceAccountName
	}
	if overrides.SecurityContext != nil {
		template.Spec.SecurityContext = overrides.SecurityContext
	}
	if len(template.Spec.InitContainers) > 0 {
		mergeContainerOverrides(&template.Spec.InitContainers[0], overrides.InitContainer)
	}
	if len(template.Spec.Containers) > 0 {
		mergeContainerOverrides(&template.Spec.Containers[0], overrides.Container)
	}
}

func mergeContainerOverrides(container *corev1.Container, overrides *functionconfig.ContainerOverrides) {
	if overrides == nil {
		return
	}
	if overrides.Resources != nil {
		if overrides.Resources.Requests != nil {
			container.Resources.Requests = overrides.Resources.Requests
		}
		if overrides.Resources.Limits != nil {
			container.Resources.Limits = overrides.Resources.Limits
		}
	}
	if overrides.EnvFrom != nil {
		container.EnvFrom = overrides.EnvFrom
	}
	if overrides.Env != nil {
		container.Env = overrides.Env
	}
}

func applyGOMEMLIMIT(container *corev1.Container) {
	if hasEnv(container.Env, gomemlimitEnvVar) {
		return
	}
	mem := memoryForGOMEMLIMIT(container)
	if mem == nil {
		return
	}
	gomemBytes := mem.Value() - gomemlimitSlack.Value()
	if gomemBytes <= 0 {
		return
	}
	setEnv(&container.Env, gomemlimitEnvVar, fmt.Sprintf("%dMiB", gomemBytes/(1024*1024)))
}

func memoryForGOMEMLIMIT(container *corev1.Container) *resource.Quantity {
	if mem := container.Resources.Limits.Memory(); mem != nil && !mem.IsZero() {
		return mem
	}
	if mem := container.Resources.Requests.Memory(); mem != nil && !mem.IsZero() {
		return mem
	}
	return nil
}

func hasEnv(env []corev1.EnvVar, name string) bool {
	return slices.ContainsFunc(env, func(e corev1.EnvVar) bool { return e.Name == name })
}

func setEnv(env *[]corev1.EnvVar, name, value string) {
	for i := range *env {
		if (*env)[i].Name == name {
			(*env)[i].Value = value
			return
		}
	}
	*env = append(*env, corev1.EnvVar{Name: name, Value: value})
}

func PatchFunctionContainer(pod *corev1.PodTemplateSpec, entrypoint []string, image string) error {
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		if container.Name != fnpod.ContainerFunction {
			continue
		}
		container.Args = append(container.Args, "--port", fmt.Sprint(wrapperServerPort), "--")
		container.Args = append(container.Args, entrypoint...)
		container.Image = image
		applyGOMEMLIMIT(container)
		return nil
	}
	return fmt.Errorf("pod template has no %s container", fnpod.ContainerFunction)
}
