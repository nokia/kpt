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

// Package fnpod holds identifiers shared by the pod manager.
package fnpod

const (
	// LabelImage selects every evaluator pod of one function. Its value is the Service name.
	LabelImage = "fn.kpt.dev/image"
	// AnnotationTemplateVersion records the PodTemplate resourceVersion used to build the pod.
	AnnotationTemplateVersion = "fn.kpt.dev/template-version"
	// AnnotationResolvedVersion is the concrete tag the evaluator pod was started with.
	AnnotationResolvedVersion = "fn.kpt.dev/resolved-version"
	// AnnotationPodIndex is the postfix in the pod name. The lowest free index is reused.
	AnnotationPodIndex = "fn.kpt.dev/pod-index"
	// AnnotationFunctionImage is the repository, without tag or digest, the evaluator pod runs.
	AnnotationFunctionImage = "fn.kpt.dev/function-image"
	// AnnotationIdleSince is when the manager first observed the pod with no Claimed request naming it.
	AnnotationIdleSince = "fn.kpt.dev/idle-since"
	// AnnotationErrorSince is when the manager moved a request into an error phase.
	AnnotationErrorSince = "fn.kpt.dev/error-since"
	// AnnotationMatchedPod marks that the request was already told an evaluator can serve it.
	AnnotationMatchedPod = "fn.kpt.dev/matched-pod"

	// DefaultNamespace is the function namespace porch uses for evaluator pods.
	DefaultNamespace = "porch-fn-system"
	// WrapperServerImageEnv is the environment variable porch uses for the wrapper-server image.
	WrapperServerImageEnv = "WRAPPER_SERVER_IMAGE"

	// ContainerFunction is the KRM function container name in the base pod template.
	ContainerFunction = "function"
	// ContainerInit is the init container that copies the wrapper server.
	ContainerInit = "copy-wrapper-server"

	// BasePodTemplateName is the PodTemplate created when none exists.
	BasePodTemplateName = "base-pod-template"
	// BaseServiceTemplateName is the ServiceTemplate created when none exists.
	BaseServiceTemplateName = "base-service-template"

	// FieldManager is the field manager used for pods and Services.
	FieldManager = "function-pod-manager"
	// LeaderElectionID is the lease name for the single active manager.
	LeaderElectionID = "function-pod-manager"

	// EventStarting is recorded when a Function Evaluator Pod is created.
	EventStarting = "StartingPod"
	// EventStartFailed is recorded when a Function Evaluator Pod does not become Ready.
	EventStartFailed = "StartFailed"
	// EventDeletedIdle is recorded when an idle Function Evaluator Pod is deleted.
	EventDeletedIdle = "DeletedIdlePod"
	// EventUnsatisfiable is recorded when no published tag satisfies the constraint.
	EventUnsatisfiable = "UnsatisfiableConstraint"
	// EventReaped is recorded when a request moves to an error phase.
	EventReaped = "RequestReaped"
	// EventPodAvailable is recorded once when an existing evaluator can serve the request.
	EventPodAvailable = "PodAvailable"
)
