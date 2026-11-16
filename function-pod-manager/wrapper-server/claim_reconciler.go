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

	fnreqv1alpha1 "github.com/kptdev/kpt/function-pod-manager/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// ClaimReconciler wires claim selection to controller-runtime watch events (label-scoped cache).
type ClaimReconciler struct {
	Claims *ClaimController
}

// Reconcile runs the oldest-first claim cycle when a matching FunctionPodRequest changes.
func (r *ClaimReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return r.Claims.runClaimCycle(ctx)
}

// SetupWithManager registers a reconciler on FunctionPodRequest (namespace and image-key filtered via cache).
func (r *ClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fnreqv1alpha1.FunctionPodRequest{}).
		Complete(r)
}