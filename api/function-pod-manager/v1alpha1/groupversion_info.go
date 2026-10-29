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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "config.porch.kpt.dev", Version: "v1alpha1"}

	TypeFunctionPodRequest = TypeInfo{
		Kind:     "FunctionPodRequest",
		Resource: GroupVersion.WithResource("functionpodrequests"),
		objects:  []runtime.Object{&FunctionPodRequest{}, &FunctionPodRequestList{}},
	}

	// AllKinds is every kind in this group version.
	AllKinds = []TypeInfo{
		TypeFunctionPodRequest,
	}
)

// +kubebuilder:object:generate=false

// TypeInfo holds type meta-information.
type TypeInfo struct {
	Kind     string
	Resource schema.GroupVersionResource
	objects  []runtime.Object
}

// GVK returns the schema.GroupVersionKind for the type.
func (t *TypeInfo) GVK() schema.GroupVersionKind {
	return t.Resource.GroupVersion().WithKind(t.Kind)
}

// APIVersion returns the apiVersion for the type.
func (t *TypeInfo) APIVersion() string {
	return t.Resource.GroupVersion().Identifier()
}

// GroupResource returns the GroupResource for the kind.
func (t *TypeInfo) GroupResource() schema.GroupResource {
	return t.Resource.GroupResource()
}

// AddToScheme registers every kind in AllKinds.
func AddToScheme(scheme *runtime.Scheme) error {
	for _, kind := range AllKinds {
		scheme.AddKnownTypes(GroupVersion, kind.objects...)
	}
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
