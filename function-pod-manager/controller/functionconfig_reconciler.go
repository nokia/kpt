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

package controller

import (
	"context"

	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FunctionConfigReconciler warms a pool when a FunctionConfig changes.
type FunctionConfigReconciler struct {
	client.Client
	Pods   *FunctionPodRequestReconciler
	Store  *FunctionConfigStore
	Warmup bool
}

// SetupWithManager watches FunctionConfig.
func (r *FunctionConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&functionconfig.FunctionConfig{}).
		Complete(r)
}

// Reconcile stores the FunctionConfig for later lookups and, when warmup is on,
// ensures a pod for a concrete podExecutor tag.
func (r *FunctionConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Pods == nil || req.Namespace != r.Pods.PodNamespace {
		return ctrl.Result{}, nil
	}
	cfg := &functionconfig.FunctionConfig{}
	if err := r.Get(ctx, req.NamespacedName, cfg); err != nil {
		if apierrors.IsNotFound(err) {
			r.Store.delete(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !cfg.DeletionTimestamp.IsZero() {
		r.Store.delete(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	r.Store.upsert(cfg)
	if !r.Warmup {
		return ctrl.Result{}, nil
	}
	pods, err := r.Pods.listPods(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.Pods.Provisioner.Warmup(ctx, cfg, pods)
}
