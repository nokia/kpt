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
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	fnreqv1alpha1 "github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"github.com/google/go-containerregistry/pkg/name"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClaimController reconciles FunctionPodRequest objects in Created phase into Claimed
// for this pod's image base and resolved version.
type ClaimController struct {
	Client    client.Client
	Namespace string

	PodName           string
	PodIP             string
	GRPCPort          int32
	FunctionImage     string
	FunctionBaseName  string
	ImageKey          string
	ResolvedVersion   string
	ClaimGracePeriod  time.Duration
	requestRepository string

	reconcileMu   sync.Mutex
	activeClaim   *activeClaimState
	capacityWake  chan struct{}
	wakeReconcile func()
}

type activeClaimState struct {
	name        string
	claimedAt   time.Time
	evalStarted bool
}

const claimFieldOwner = "function-evaluator-pod"

// errAlreadyHeld means this pod already owns the request, so this cycle must not claim another one.
var errAlreadyHeld = errors.New("already claimed by this pod")

// AdoptClaimsOnStartup recovers in-memory state from Claimed requests still owned by this pod.
func (c *ClaimController) AdoptClaimsOnStartup(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is required")
	}
	list := &fnreqv1alpha1.FunctionPodRequestList{}
	if err := c.Client.List(ctx, list,
		client.InNamespace(c.Namespace),
		client.MatchingLabels{fnpod.LabelImageKey: c.ImageKey},
	); err != nil {
		return err
	}
	for i := range list.Items {
		if list.Items[i].Status.Phase != fnreqv1alpha1.FunctionPodRequestPhaseClaimed {
			continue
		}
		if list.Items[i].Status.EvaluatorPodName != c.PodName {
			continue
		}
		c.reconcileMu.Lock()
		if c.activeClaim == nil {
			c.activeClaim = &activeClaimState{name: list.Items[i].Name, claimedAt: time.Now()}
		}
		c.reconcileMu.Unlock()
		return nil
	}
	return nil
}

// StartCapacityWatcher reacts when a gRPC evaluation ends (study: capacity-freed signal).
func (c *ClaimController) StartCapacityWatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.capacityWake:
			if c.wakeReconcile != nil {
				c.wakeReconcile()
				continue
			}
			if _, err := c.runClaimCycle(ctx); err != nil {
				klog.Errorf("claim after capacity freed: %v", err)
			}
		}
	}
}

func (c *ClaimController) runClaimCycle(ctx context.Context) (ctrl.Result, error) {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()

	now := time.Now()
	var requeue time.Duration
	if c.activeClaim != nil && !c.activeClaim.evalStarted && c.ClaimGracePeriod > 0 {
		elapsed := now.Sub(c.activeClaim.claimedAt)
		if elapsed >= c.ClaimGracePeriod {
			name := c.activeClaim.name
			c.activeClaim = nil
			if err := c.releaseClaim(ctx, name); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		requeue = c.ClaimGracePeriod - elapsed
	}

	if c.activeClaim != nil {
		req := &fnreqv1alpha1.FunctionPodRequest{}
		key := types.NamespacedName{Namespace: c.Namespace, Name: c.activeClaim.name}
		if err := c.Client.Get(ctx, key, req); err != nil {
			klog.V(4).InfoS("active claim missing, clearing", "request", c.activeClaim.name, "err", err)
			c.activeClaim = nil
		} else if req.Status.Phase != fnreqv1alpha1.FunctionPodRequestPhaseClaimed ||
			req.Status.EvaluatorPodName != c.PodName {
			c.activeClaim = nil
		}
	}

	if c.activeClaim != nil {
		if requeue == 0 && c.ClaimGracePeriod > 0 && !c.activeClaim.evalStarted {
			requeue = time.Until(c.activeClaim.claimedAt.Add(c.ClaimGracePeriod))
			if requeue < 0 {
				requeue = 0
			}
		}
		klog.InfoS("claim cycle busy", "request", c.activeClaim.name, "pod", c.PodName, "requeue", requeue.String())
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	claimable, err := c.listClaimable(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(claimable) == 0 {
		return ctrl.Result{}, nil
	}

	sort.Slice(claimable, func(i, j int) bool {
		ti := claimable[i].CreationTimestamp.Time
		tj := claimable[j].CreationTimestamp.Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return claimable[i].Name < claimable[j].Name
	})

	for i := range claimable {
		switch err := c.tryClaim(ctx, &claimable[i]); {
		case err == nil || errors.Is(err, errAlreadyHeld):
			klog.InfoS("claimed FunctionPodRequest", "request", claimable[i].Name, "pod", c.PodName)
			c.activeClaim = &activeClaimState{name: claimable[i].Name, claimedAt: now}
			return ctrl.Result{}, nil
		case apierrors.IsConflict(err):
			// Lost race: normal, not an error (Decision 2 / study).
			klog.InfoS("claim conflict", "request", claimable[i].Name, "namespace", claimable[i].Namespace, "pod", c.PodName)
			continue
		default:
			klog.InfoS("drop FunctionPodRequest",
				"request", claimable[i].Name,
				"namespace", claimable[i].Namespace,
				"image", claimable[i].Spec.Image,
				"versionConstraint", claimable[i].Spec.VersionConstraint,
				"phase", claimable[i].Status.Phase,
				"pod", c.PodName,
				"err", err,
			)
		}
	}
	return ctrl.Result{}, nil
}

func (c *ClaimController) listClaimable(ctx context.Context) ([]fnreqv1alpha1.FunctionPodRequest, error) {
	list := &fnreqv1alpha1.FunctionPodRequestList{}
	if err := c.Client.List(ctx, list,
		client.InNamespace(c.Namespace),
		client.MatchingLabels{fnpod.LabelImageKey: c.ImageKey},
	); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]fnreqv1alpha1.FunctionPodRequest, 0, len(list.Items))
	for i := range list.Items {
		req := &list.Items[i]
		if reason := c.skipReason(req, now); reason != "" {
			klog.InfoS("skip FunctionPodRequest",
				"request", req.Name,
				"namespace", req.Namespace,
				"reason", reason,
				"phase", req.Status.Phase,
				"image", req.Spec.Image,
				"versionConstraint", req.Spec.VersionConstraint,
				"requestImageKey", requestImageKey(req),
				"deadline", deadlineText(req),
				"pod", c.PodName,
				"podImage", c.FunctionImage,
				"podImageKey", c.ImageKey,
				"podVersion", c.ResolvedVersion,
			)
			continue
		}
		out = append(out, *req)
	}
	klog.InfoS("FunctionPodRequest claim scan",
		"listed", len(list.Items),
		"claimable", len(out),
		"namespace", c.Namespace,
		"pod", c.PodName,
		"podImage", c.FunctionImage,
		"podImageKey", c.ImageKey,
		"podVersion", c.ResolvedVersion,
	)
	return out, nil
}

func (c *ClaimController) skipReason(req *fnreqv1alpha1.FunctionPodRequest, now time.Time) string {
	if !phaseIsCreated(req.Status.Phase) {
		return fmt.Sprintf("phase %q is not Created", req.Status.Phase)
	}
	if req.Spec.Deadline != nil && req.Spec.Deadline.Before(&metav1.Time{Time: now}) {
		return "deadline passed"
	}
	if key := requestImageKey(req); key != "" && c.ImageKey != "" && key != c.ImageKey {
		return fmt.Sprintf("image-key %q != pod %q", key, c.ImageKey)
	}
	if base := fnpod.ImageBaseName(req.Spec.Image); base != c.FunctionBaseName {
		return fmt.Sprintf("image base %q != pod %q", base, c.FunctionBaseName)
	}
	if !fnpod.VersionSatisfies(c.ResolvedVersion, req.Spec.VersionConstraint) {
		return fmt.Sprintf("pod version %q does not satisfy %q", c.ResolvedVersion, req.Spec.VersionConstraint)
	}
	return ""
}

func requestImageKey(req *fnreqv1alpha1.FunctionPodRequest) string {
	if req.Labels == nil {
		return ""
	}
	return strings.TrimSpace(req.Labels[fnpod.LabelImageKey])
}

func deadlineText(req *fnreqv1alpha1.FunctionPodRequest) string {
	if req.Spec.Deadline == nil {
		return ""
	}
	return req.Spec.Deadline.UTC().Format(time.RFC3339)
}

func phaseIsCreated(phase fnreqv1alpha1.FunctionPodRequestPhase) bool {
	return phase == "" || phase == fnreqv1alpha1.FunctionPodRequestPhaseCreated
}

func (c *ClaimController) tryClaim(ctx context.Context, req *fnreqv1alpha1.FunctionPodRequest) error {
	key := types.NamespacedName{Namespace: req.Namespace, Name: req.Name}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh := &fnreqv1alpha1.FunctionPodRequest{}
		if err := c.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Status.Phase == fnreqv1alpha1.FunctionPodRequestPhaseClaimed && fresh.Status.EvaluatorPodName == c.PodName {
			return errAlreadyHeld
		}
		now := time.Now().UTC()
		if reason := c.skipReason(fresh, now); reason != "" {
			return fmt.Errorf("%s", reason)
		}
		base := fresh.DeepCopy()
		fresh.Status.Phase = fnreqv1alpha1.FunctionPodRequestPhaseClaimed
		fresh.Status.EvaluatorPodName = c.PodName
		fresh.Status.EvaluatorPodIP = c.PodIP
		fresh.Status.EvaluatorPort = c.GRPCPort
		fresh.Status.ResolvedVersion = c.ResolvedVersion
		fresh.Status.Message = ""
		return c.Client.Status().Patch(ctx, fresh, client.MergeFrom(base), client.FieldOwner(claimFieldOwner))
	})
}

func (c *ClaimController) releaseClaim(ctx context.Context, name string) error {
	key := types.NamespacedName{Namespace: c.Namespace, Name: name}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh := &fnreqv1alpha1.FunctionPodRequest{}
		if err := c.Client.Get(ctx, key, fresh); err != nil {
			return client.IgnoreNotFound(err)
		}
		if fresh.Status.Phase != fnreqv1alpha1.FunctionPodRequestPhaseClaimed || fresh.Status.EvaluatorPodName != c.PodName {
			return nil
		}
		base := fresh.DeepCopy()
		fresh.Status.Phase = fnreqv1alpha1.FunctionPodRequestPhaseCreated
		fresh.Status.EvaluatorPodName = ""
		fresh.Status.EvaluatorPodIP = ""
		fresh.Status.EvaluatorPort = 0
		fresh.Status.ResolvedVersion = ""
		fresh.Status.Message = ""
		return c.Client.Status().Patch(ctx, fresh, client.MergeFrom(base), client.FieldOwner(claimFieldOwner))
	})
}

// ValidateEvalRequest checks the FunctionPodRequest is claimed by this pod and records that evaluation started.
func (c *ClaimController) ValidateEvalRequest(ctx context.Context, requestName string) error {
	if requestName == "" {
		return status.Errorf(codes.InvalidArgument, "request_name is required")
	}
	fr := &fnreqv1alpha1.FunctionPodRequest{}
	key := types.NamespacedName{Namespace: c.Namespace, Name: requestName}
	if err := c.Client.Get(ctx, key, fr); err != nil {
		if apierrors.IsNotFound(err) {
			return status.Errorf(codes.NotFound, "FunctionPodRequest %q not found", requestName)
		}
		return status.Errorf(codes.Internal, "get FunctionPodRequest: %v", err)
	}
	if fr.Status.Phase != fnreqv1alpha1.FunctionPodRequestPhaseClaimed {
		return status.Errorf(codes.FailedPrecondition, "request %q phase is %q, want Claimed", requestName, fr.Status.Phase)
	}
	if fr.Status.EvaluatorPodName != c.PodName {
		return status.Errorf(codes.PermissionDenied, "request %q is not claimed by this pod", requestName)
	}

	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	if c.activeClaim == nil || c.activeClaim.name != requestName {
		c.activeClaim = &activeClaimState{name: requestName, claimedAt: time.Now(), evalStarted: true}
	} else {
		c.activeClaim.evalStarted = true
	}
	return nil
}

// OnEvaluateEnd clears the in-memory claim after an EvaluateFunction RPC completes.
func (c *ClaimController) OnEvaluateEnd(requestName string) {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	if c.activeClaim == nil || c.activeClaim.name != requestName {
		return
	}
	c.activeClaim = nil
	select {
	case c.capacityWake <- struct{}{}:
	default:
	}
}

// DefaultResolvedVersion returns VERSION, or "unknown" if unset.
func DefaultResolvedVersion() string {
	v := strings.TrimSpace(os.Getenv("VERSION"))
	if v == "" {
		return "unknown"
	}
	return v
}

// DefaultPodIP returns POD_IP env or status.podIP from downward API volume.
func DefaultPodIP() string {
	if v := strings.TrimSpace(os.Getenv("POD_IP")); v != "" {
		return v
	}
	b, err := os.ReadFile("/etc/podinfo/pod_ip")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func functionRepository(imageRef string) (string, error) {
	ref := strings.TrimSpace(imageRef)
	if ref == "" {
		return "", fmt.Errorf("empty image")
	}
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	n, err := name.ParseReference(ref, name.WeakValidation)
	if err != nil {
		return "", err
	}
	return n.Context().Name(), nil
}

// ClaimReconciler wires claim selection to controller-runtime watch events (label-scoped cache).
type ClaimReconciler struct {
	Claims *ClaimController
}

// Reconcile runs the oldest-first claim cycle when a matching FunctionPodRequest changes.
func (r *ClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.InfoS("FunctionPodRequest event", "request", req.Name, "namespace", req.Namespace, "pod", r.Claims.PodName)
	return r.Claims.runClaimCycle(ctx)
}

// SetupWithManager registers a reconciler on FunctionPodRequest (namespace and image-key filtered via cache).
func (r *ClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fnreqv1alpha1.FunctionPodRequest{}).
		Complete(r)
}
