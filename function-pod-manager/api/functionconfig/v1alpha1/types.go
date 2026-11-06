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

// Package v1alpha1 holds the porch FunctionConfig and ServiceTemplate types
// the pod manager reads. They match the JSON of config.porch.kpt.dev/v1alpha1
// without importing the porch module.
//
// +kubebuilder:object:generate=false
// +kubebuilder:ac:generate=false
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the porch config API the manager watches.
var GroupVersion = schema.GroupVersion{Group: "config.porch.kpt.dev", Version: "v1alpha1"}

// FunctionConfig is the porch configuration for one function image.
type FunctionConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec FunctionConfigSpec `json:"spec,omitempty"`
}

// FunctionConfigSpec is the portion of a porch FunctionConfig the manager uses.
type FunctionConfigSpec struct {
	Image       string             `json:"image"`
	PodExecutor *PodExecutorConfig `json:"podExecutor,omitempty"`
}

// PodExecutorConfig is the pod pool configuration for one function.
type PodExecutorConfig struct {
	Tags                    []string           `json:"tags,omitempty"`
	TimeToLive              metav1.Duration    `json:"timeToLive,omitempty"`
	MaxParallelExecutions   int                `json:"maxParallelExecutions,omitempty"`
	PreferredMaxQueueLength int                `json:"preferredMaxQueueLength,omitempty"`
	TemplateOverrides       *TemplateOverrides `json:"templateOverrides,omitempty"`
}

// TemplateOverrides patches the base pod template for one function.
type TemplateOverrides struct {
	ServiceAccountName string                     `json:"serviceAccountName,omitempty"`
	SecurityContext    *corev1.PodSecurityContext `json:"securityContext,omitempty"`
	InitContainer      *ContainerOverrides        `json:"initContainer,omitempty"`
	Container          *ContainerOverrides        `json:"container,omitempty"`
}

// ContainerOverrides patches one container in the base pod template.
type ContainerOverrides struct {
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	EnvFrom   []corev1.EnvFromSource       `json:"envFrom,omitempty"`
	Env       []corev1.EnvVar              `json:"env,omitempty"`
}

// FunctionConfigList is a list of FunctionConfig.
type FunctionConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []FunctionConfig `json:"items"`
}

// ServiceTemplate is the porch template for evaluator Services.
type ServiceTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Template ServiceTemplateSpec `json:"template"`
}

// ServiceTemplateSpec is the object meta and spec applied to a new Service.
type ServiceTemplateSpec struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              corev1.ServiceSpec `json:"spec"`
}

// ServiceTemplateList is a list of ServiceTemplate.
type ServiceTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ServiceTemplate `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (in *FunctionConfig) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(FunctionConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *FunctionConfig) DeepCopyInto(out *FunctionConfig) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

// DeepCopyInto copies the spec into out.
func (in *FunctionConfigSpec) DeepCopyInto(out *FunctionConfigSpec) {
	*out = *in
	if in.PodExecutor != nil {
		out.PodExecutor = in.PodExecutor.DeepCopy()
	}
}

// DeepCopy returns a copy of the pod executor config.
func (in *PodExecutorConfig) DeepCopy() *PodExecutorConfig {
	if in == nil {
		return nil
	}
	out := new(PodExecutorConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the pod executor config into out.
func (in *PodExecutorConfig) DeepCopyInto(out *PodExecutorConfig) {
	*out = *in
	if in.Tags != nil {
		out.Tags = append([]string(nil), in.Tags...)
	}
	out.TemplateOverrides = in.TemplateOverrides.DeepCopy()
}

// DeepCopy returns a copy of the template overrides.
func (in *TemplateOverrides) DeepCopy() *TemplateOverrides {
	if in == nil {
		return nil
	}
	out := new(TemplateOverrides)
	out.ServiceAccountName = in.ServiceAccountName
	if in.SecurityContext != nil {
		out.SecurityContext = in.SecurityContext.DeepCopy()
	}
	out.InitContainer = in.InitContainer.DeepCopy()
	out.Container = in.Container.DeepCopy()
	return out
}

// DeepCopy returns a copy of the container overrides.
func (in *ContainerOverrides) DeepCopy() *ContainerOverrides {
	if in == nil {
		return nil
	}
	out := new(ContainerOverrides)
	if in.Resources != nil {
		out.Resources = in.Resources.DeepCopy()
	}
	if in.EnvFrom != nil {
		out.EnvFrom = make([]corev1.EnvFromSource, len(in.EnvFrom))
		for i := range in.EnvFrom {
			in.EnvFrom[i].DeepCopyInto(&out.EnvFrom[i])
		}
	}
	if in.Env != nil {
		out.Env = make([]corev1.EnvVar, len(in.Env))
		for i := range in.Env {
			in.Env[i].DeepCopyInto(&out.Env[i])
		}
	}
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *FunctionConfigList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(FunctionConfigList)
	out.TypeMeta = in.TypeMeta
	in.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]FunctionConfig, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *ServiceTemplate) DeepCopyInto(out *ServiceTemplate) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Template.DeepCopyInto(&out.Template)
}

// DeepCopyObject implements runtime.Object.
func (in *ServiceTemplate) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ServiceTemplate)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the service template spec into out.
func (in *ServiceTemplateSpec) DeepCopyInto(out *ServiceTemplateSpec) {
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

// DeepCopyObject implements runtime.Object.
func (in *ServiceTemplateList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ServiceTemplateList)
	out.TypeMeta = in.TypeMeta
	in.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]ServiceTemplate, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
	return out
}

// AddToScheme registers FunctionConfig and ServiceTemplate.
func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&FunctionConfig{},
		&FunctionConfigList{},
		&ServiceTemplate{},
		&ServiceTemplateList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
