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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	fnreqv1alpha1 "github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	pb "github.com/kptdev/kpt/wrapper-server/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	claimTestPodName = "sleep-eval-0"
	claimTestPodIP   = "10.20.30.40"
	claimTestImage   = "example.com/krm/sleep:v1.2.3"
	claimTestVersion = "v1.2.3"
	claimTestRequest = "fnreq-happy"
)

var (
	kube           client.Client
	kubeconfigFile string
	claimImageKey  string
)

func TestMain(m *testing.M) {
	os.Setenv("OTEL_METRICS_EXPORTER", "none")
	assets, err := envtest.SetupEnvtestDefaultBinaryAssetsDirectory()
	exitOnError(nil, err)
	env := &envtest.Environment{
		CRDDirectoryPaths:        []string{filepath.Join("..", "function-pod-manager", "api", "generated", "crds")},
		ErrorIfCRDPathMissing:    true,
		DownloadBinaryAssets:     true,
		BinaryAssetsDirectory:    assets,
		ControlPlaneStartTimeout: time.Minute,
	}
	cfg, err := env.Start()
	exitOnError(nil, err)
	dir, err := os.MkdirTemp("", "wrapper-envtest")
	exitOnError(env, err)
	kubeconfigFile = filepath.Join(dir, "kubeconfig")
	err = os.WriteFile(kubeconfigFile, env.KubeConfig, 0o600)
	exitOnError(env, err)
	scheme := runtime.NewScheme()
	err = errors.Join(clientgoscheme.AddToScheme(scheme), fnreqv1alpha1.AddToScheme(scheme))
	exitOnError(env, err)
	kube, err = client.New(cfg, client.Options{Scheme: scheme})
	exitOnError(env, err)
	repository, err := functionRepository(claimTestImage)
	exitOnError(env, err)
	claimImageKey, err = fnpod.ImageKey(repository)
	exitOnError(env, err)

	code := m.Run()
	if err := env.Stop(); err != nil && code == 0 {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func exitOnError(env *envtest.Environment, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if env != nil {
			_ = env.Stop()
		}
		os.Exit(1)
	}
}

// TestWrapperClaimsACreatedFunctionPodRequest starts the wrapper the way a pod does
// and checks that a Created request is claimed, then evaluated by a pass-through function.
func TestWrapperClaimsACreatedFunctionPodRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	createRequest(t, ctx, requestSpec{name: claimTestRequest, namespace: ns})

	t.Setenv("POD_NAMESPACE", ns)
	t.Setenv("POD_NAME", claimTestPodName)
	t.Setenv("POD_IP", claimTestPodIP)
	t.Setenv(fnpod.EnvFunctionImage, claimTestImage)
	t.Setenv(fnpod.EnvVersion, claimTestVersion)
	run := startWrapper(t, ctx, Options{})

	port := waitClaimed(t, ctx, run, ns, claimTestRequest)

	resourceList := []byte(`{"apiVersion":"config.kubernetes.io/v1","kind":"ResourceList","items":[]}`)
	var evaluated *pb.EvaluateFunctionResponse
	assert.Eventually(t, func() bool {
		require.NoError(t, run.err())
		resp, err := evaluate(ctx, port, &pb.EvaluateFunctionRequest{
			RequestName:  claimTestRequest,
			Image:        claimTestImage,
			ResourceList: resourceList,
		})
		if err != nil {
			return false
		}
		evaluated = resp
		return true
	}, 15*time.Second, 200*time.Millisecond)
	assert.Equal(t, resourceList, evaluated.GetResourceList())

	got := getRequest(t, ctx, ns, claimTestRequest)
	assert.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseClaimed, got.Status.Phase)
}

func TestWrapperClaimsTheOldestRequestAndLeavesTheNewerCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	createRequest(t, ctx, requestSpec{name: "fnreq-older", namespace: ns})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-newer", namespace: ns})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	waitClaimed(t, ctx, run, ns, "fnreq-older")
	requireStaysUnclaimed(t, ctx, run, ns, "fnreq-newer")
}

func TestWrapperSkipsAMismatchedVersionConstraint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	createRequest(t, ctx, requestSpec{name: "fnreq-other-version", namespace: ns, constraint: "~9.0.0"})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-match", namespace: ns})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	waitClaimed(t, ctx, run, ns, "fnreq-match")
	requireUnclaimed(t, ctx, ns, "fnreq-other-version")
}

func TestWrapperClaimsAnExactVersionConstraint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	createRequest(t, ctx, requestSpec{name: "fnreq-exact", namespace: ns, constraint: claimTestVersion})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	waitClaimed(t, ctx, run, ns, "fnreq-exact")
}

func TestWrapperIgnoresAnotherImageKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	otherKey, err := fnpod.ImageKey("example.com/krm/other")
	require.NoError(t, err)
	createRequest(t, ctx, requestSpec{name: "fnreq-other-key", namespace: ns, imageKey: otherKey})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-this-key", namespace: ns})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	waitClaimed(t, ctx, run, ns, "fnreq-this-key")
	requireUnclaimed(t, ctx, ns, "fnreq-other-key")
}

func TestWrapperSkipsARequestPastItsDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	createRequest(t, ctx, requestSpec{name: "fnreq-expired", namespace: ns, deadline: &past})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-open", namespace: ns})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	waitClaimed(t, ctx, run, ns, "fnreq-open")
	requireUnclaimed(t, ctx, ns, "fnreq-expired")
}

func TestWrapperReleasesTheClaimAfterGraceAndClaimsTheNextRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	// The first request must be claimable at startup and unclaimable by the time grace
	// fires, so the waiting request is the one that gets the freed slot.
	expires := metav1.NewTime(time.Now().Add(4 * time.Second))
	createRequest(t, ctx, requestSpec{name: "fnreq-first", namespace: ns, deadline: &expires})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-second", namespace: ns})

	opts := wrapperOptions(ns)
	opts.ClaimGracePeriod = 5 * time.Second
	run := startWrapper(t, ctx, opts)
	port := waitClaimedWithin(t, ctx, run, ns, "fnreq-first", 2*time.Second)

	var observed string
	assert.Eventually(t, func() bool {
		require.NoError(t, run.err())
		first := getRequest(t, ctx, ns, "fnreq-first")
		second := getRequest(t, ctx, ns, "fnreq-second")
		observed = fmt.Sprintf("first=%s/%s second=%s/%s", first.Status.Phase, first.Status.EvaluatorPodName, second.Status.Phase, second.Status.EvaluatorPodName)
		return first.Status.Phase == fnreqv1alpha1.FunctionPodRequestPhaseCreated &&
			first.Status.EvaluatorPodName == "" &&
			second.Status.Phase == fnreqv1alpha1.FunctionPodRequestPhaseClaimed &&
			second.Status.EvaluatorPodName == claimTestPodName &&
			second.Status.EvaluatorPodIP == claimTestPodIP &&
			second.Status.EvaluatorPort == int32(port)
	}, 12*time.Second, 200*time.Millisecond, "last: %s", &observed)
}

func TestWrapperKeepsTheClaimItAdoptedOnStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	createRequest(t, ctx, requestSpec{
		name:         "fnreq-adopted",
		namespace:    ns,
		phase:        fnreqv1alpha1.FunctionPodRequestPhaseClaimed,
		evaluatorPod: claimTestPodName,
	})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-waiting", namespace: ns})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	requireStaysUnclaimed(t, ctx, run, ns, "fnreq-waiting")
	got := getRequest(t, ctx, ns, "fnreq-adopted")
	assert.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseClaimed, got.Status.Phase)
	assert.Equal(t, claimTestPodName, got.Status.EvaluatorPodName)
}

func TestWrapperClaimsACreatedRequestWhileAnotherPodHoldsOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ns := newNamespace(t, ctx)
	createRequest(t, ctx, requestSpec{
		name:         "fnreq-theirs",
		namespace:    ns,
		phase:        fnreqv1alpha1.FunctionPodRequestPhaseClaimed,
		evaluatorPod: "other-eval",
	})
	time.Sleep(time.Second)
	createRequest(t, ctx, requestSpec{name: "fnreq-ours", namespace: ns})

	run := startWrapper(t, ctx, wrapperOptions(ns))
	waitClaimed(t, ctx, run, ns, "fnreq-ours")
	theirs := getRequest(t, ctx, ns, "fnreq-theirs")
	assert.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseClaimed, theirs.Status.Phase)
	assert.Equal(t, "other-eval", theirs.Status.EvaluatorPodName)
}

type requestSpec struct {
	name         string
	namespace    string
	imageKey     string
	constraint   string
	deadline     *metav1.Time
	phase        fnreqv1alpha1.FunctionPodRequestPhase
	evaluatorPod string
}

func createRequest(t *testing.T, ctx context.Context, spec requestSpec) {
	t.Helper()
	imageKey := spec.imageKey
	if imageKey == "" {
		imageKey = claimImageKey
	}
	phase := spec.phase
	if phase == "" {
		phase = fnreqv1alpha1.FunctionPodRequestPhaseCreated
	}
	req := &fnreqv1alpha1.FunctionPodRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.name,
			Namespace: spec.namespace,
			Labels:    map[string]string{fnpod.LabelImageKey: imageKey},
		},
		Spec: fnreqv1alpha1.FunctionPodRequestSpec{
			Image:             claimTestImage,
			VersionConstraint: spec.constraint,
			Deadline:          spec.deadline,
		},
	}
	require.NoError(t, kube.Create(ctx, req))
	req.Status.Phase = phase
	req.Status.EvaluatorPodName = spec.evaluatorPod
	require.NoError(t, kube.Status().Update(ctx, req))
}

func wrapperOptions(namespace string) Options {
	return Options{
		Namespace:       namespace,
		PodName:         claimTestPodName,
		PodIP:           claimTestPodIP,
		FunctionImage:   claimTestImage,
		ResolvedVersion: claimTestVersion,
		Entrypoint:      []string{"/bin/cat"},
	}
}

func newNamespace(t *testing.T, ctx context.Context) string {
	t.Helper()
	ns := fmt.Sprintf("fn-%d", time.Now().UnixNano())
	require.NoError(t, kube.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	return ns
}

func startWrapper(t *testing.T, ctx context.Context, opts Options) *serverRun {
	t.Helper()
	opts.Kubeconfig = kubeconfigFile
	opts.DisableMetrics = true
	if len(opts.Entrypoint) == 0 {
		opts.Entrypoint = []string{"/bin/cat"}
	}
	run := &serverRun{}
	go func() {
		run.finish(Run(ctx, &opts))
	}()
	t.Cleanup(func() {
		run.wait(20 * time.Second)
	})
	return run
}

func waitClaimed(t *testing.T, ctx context.Context, run *serverRun, namespace, name string) int {
	t.Helper()
	return waitClaimedWithin(t, ctx, run, namespace, name, 20*time.Second)
}

func waitClaimedWithin(t *testing.T, ctx context.Context, run *serverRun, namespace, name string, timeout time.Duration) int {
	t.Helper()
	var observed fnreqv1alpha1.FunctionPodRequestStatus
	require.Eventually(t, func() bool {
		require.NoError(t, run.err())
		got := getRequest(t, ctx, namespace, name)
		observed = got.Status
		return got.Status.Phase == fnreqv1alpha1.FunctionPodRequestPhaseClaimed &&
			got.Status.EvaluatorPodName == claimTestPodName &&
			got.Status.EvaluatorPodIP == claimTestPodIP &&
			got.Status.EvaluatorPort != 0 &&
			got.Status.ResolvedVersion == claimTestVersion
	}, timeout, 100*time.Millisecond, "last status: %+v", &observed)
	return int(observed.EvaluatorPort)
}

func requireUnclaimed(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	got := getRequest(t, ctx, namespace, name)
	assert.Equal(t, fnreqv1alpha1.FunctionPodRequestPhaseCreated, got.Status.Phase)
	assert.Empty(t, got.Status.EvaluatorPodName)
}

func requireStaysUnclaimed(t *testing.T, ctx context.Context, run *serverRun, namespace, name string) {
	t.Helper()
	assert.Never(t, func() bool {
		require.NoError(t, run.err())
		got := getRequest(t, ctx, namespace, name)
		return got.Status.Phase == fnreqv1alpha1.FunctionPodRequestPhaseClaimed
	}, 2*time.Second, 100*time.Millisecond)
	requireUnclaimed(t, ctx, namespace, name)
}

func getRequest(t *testing.T, ctx context.Context, namespace, name string) *fnreqv1alpha1.FunctionPodRequest {
	t.Helper()
	got := &fnreqv1alpha1.FunctionPodRequest{}
	require.NoError(t, kube.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, got))
	return got
}

type serverRun struct {
	mu   sync.Mutex
	done bool
	e    error
}

func (r *serverRun) finish(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
	r.e = err
}

func (r *serverRun) err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.done || errors.Is(r.e, context.Canceled) {
		return nil
	}
	return r.e
}

func (r *serverRun) wait(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		done := r.done
		r.mu.Unlock()
		if done {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func evaluate(ctx context.Context, port int, req *pb.EvaluateFunctionRequest) (*pb.EvaluateFunctionResponse, error) {
	conn, err := grpc.NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return pb.Evaluate(callCtx, pb.NewFunctionEvaluatorClient(conn), req)
}
