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

// Package v1alpha1 contains API schema definitions for the function pod manager.
//
// +kubebuilder:object:generate=true
// +groupName=config.porch.kpt.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FunctionPodRequest is a client-owned request to evaluate a KRM function.
// Spec is immutable after create. Status is a subresource.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=functionpodrequests,singular=functionpodrequest,scope=Namespaced
// +kubebuilder:validation:XValidation:rule="self.spec == oldSelf.spec",message="spec is immutable"
type FunctionPodRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FunctionPodRequestSpec   `json:"spec,omitempty"`
	Status FunctionPodRequestStatus `json:"status,omitempty"`
}

// FunctionPodRequestList contains a list of FunctionPodRequest.
//
// +kubebuilder:object:root=true
type FunctionPodRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []FunctionPodRequest `json:"items"`
}

// FunctionPodRequestSpec is the client-owned, immutable desired evaluation.
type FunctionPodRequestSpec struct {
	// Image is the KRM function image to evaluate.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// VersionConstraint is the semver constraint from the Kptfile pipeline step, carried verbatim.
	VersionConstraint string `json:"versionConstraint,omitempty"`
	// Deadline is the absolute RFC3339 time after which the request must not stay Created or Claimed.
	Deadline *metav1.Time `json:"deadline,omitempty"`
}

// FunctionPodRequestPhase is the status phase of a FunctionPodRequest.
// +kubebuilder:validation:Enum=Created;Claimed;PodManagementError;EvaluationError
type FunctionPodRequestPhase string

const (
	// FunctionPodRequestPhaseCreated means the request is not yet claimed.
	FunctionPodRequestPhaseCreated FunctionPodRequestPhase = "Created"
	// FunctionPodRequestPhaseClaimed means a Function Evaluator Pod has taken the request.
	FunctionPodRequestPhaseClaimed FunctionPodRequestPhase = "Claimed"
	// FunctionPodRequestPhasePodManagementError means the Pod Manager cannot ensure a claim.
	FunctionPodRequestPhasePodManagementError FunctionPodRequestPhase = "PodManagementError"
	// FunctionPodRequestPhaseEvaluationError means the gRPC call cannot be completed against the claim.
	FunctionPodRequestPhaseEvaluationError FunctionPodRequestPhase = "EvaluationError"
)

// FunctionPodRequestStatus is written through the status subresource.
type FunctionPodRequestStatus struct {
	// Phase is Created, Claimed, PodManagementError, or EvaluationError.
	Phase FunctionPodRequestPhase `json:"phase,omitempty"`
	// EvaluatorPodName is the name of the Function Evaluator Pod that claimed this request.
	EvaluatorPodName string `json:"evaluatorPodName,omitempty"`
	// EvaluatorPodIP is the IP of the Function Evaluator Pod that claimed this request.
	EvaluatorPodIP string `json:"evaluatorPodIP,omitempty"`
	// EvaluatorPort is the FunctionEvaluator gRPC port of the claiming pod.
	EvaluatorPort int32 `json:"evaluatorPort,omitempty"`
	// ResolvedVersion is the concrete version the claiming Function Evaluator Pod runs.
	ResolvedVersion string `json:"resolvedVersion,omitempty"`
	// Message is infrastructure failure text for PodManagementError and EvaluationError.
	Message string `json:"message,omitempty"`
}
