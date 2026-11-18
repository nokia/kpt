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
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	fnreqv1alpha1 "github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
)

// Options configure the wrapper server process.
type Options struct {
	Kubeconfig       string
	Port             int
	ClaimGracePeriod time.Duration
	Namespace        string
	PodName          string
	PodIP            string
	GRPCPort         int32
	FunctionImage    string
	ImageKey         string
	ResolvedVersion  string
	Entrypoint       []string
	// DisableMetrics turns off the controller-runtime metrics listener.
	// The wrapper Prometheus endpoint is unchanged.
	DisableMetrics bool
}

// wrapperPod is the pod Run serves, resolved from flags and environment.
type wrapperPod struct {
	namespace  string
	podName    string
	podIP      string
	image      string
	repository string
	imageKey   string
	version    string
	grpcPort   int32
}

// Run starts the controller-runtime manager, the claim reconciler,
// and the gRPC FunctionEvaluator server.
func Run(parent context.Context, op *Options) error {
	ctrl.SetLogger(klog.Background())
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	pod, err := wrapperPodFrom(op)
	if err != nil {
		return err
	}

	cfg, err := loadRESTConfig(op.Kubeconfig)
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(fnreqv1alpha1.AddToScheme(scheme))

	// Watch only this namespace and this pod's image-key. Other functions never enter the cache.
	labelSelector := labels.SelectorFromSet(labels.Set{fnpod.LabelImageKey: pod.imageKey})
	// The controller name is recorded in a process-global set that is not cleared when the
	// manager stops. Tests start Run more than once. This process registers one claim controller.
	skipNameValidation := true
	managerOpts := ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
		Controller:     config.Controller{SkipNameValidation: &skipNameValidation},
		// FunctionPodRequest reads go to the API server. The informer still wakes the
		// reconciler, but a cached read can miss a claim this pod just wrote.
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&fnreqv1alpha1.FunctionPodRequest{}},
			},
		},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				pod.namespace: {},
			},
			ByObject: map[client.Object]cache.ByObject{
				&fnreqv1alpha1.FunctionPodRequest{}: {
					Label: labelSelector,
				},
			},
		},
	}
	if op.DisableMetrics {
		managerOpts.Metrics.BindAddress = "0"
	}
	mgr, err := ctrl.NewManager(cfg, managerOpts)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	claims := &ClaimController{
		Client:            mgr.GetClient(),
		Namespace:         pod.namespace,
		PodName:           pod.podName,
		PodIP:             pod.podIP,
		GRPCPort:          pod.grpcPort,
		FunctionImage:     pod.image,
		FunctionBaseName:  fnpod.ImageBaseName(pod.repository),
		ImageKey:          pod.imageKey,
		ResolvedVersion:   pod.version,
		ClaimGracePeriod:  op.ClaimGracePeriod,
		requestRepository: pod.repository,
		capacityWake:      make(chan struct{}, 1),
	}

	claimReconciler := &ClaimReconciler{Claims: claims}
	if err := claimReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup claim reconciler: %w", err)
	}

	if err := claims.AdoptClaimsOnStartup(ctx); err != nil {
		return fmt.Errorf("adopt claims on startup: %w", err)
	}

	evaluator := &SingleFunctionEvaluator{Entrypoint: op.Entrypoint}

	meter, metricsAddr, err := startPrometheus(ctx)
	if err != nil {
		return fmt.Errorf("prometheus metrics: %w", err)
	}

	grpcCtx, cancelGRPC := context.WithCancel(ctx)
	defer cancelGRPC()
	// Port 0 asks the kernel for an unused port. Record that port before the
	// manager starts, so a claim advertises the port this process is bound to.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", op.Port))
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	if claims.GRPCPort == 0 {
		claims.GRPCPort = int32(lis.Addr().(*net.TCPAddr).Port)
	}
	addr := lis.Addr().String()
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- ServeGRPC(grpcCtx, lis, claims, evaluator, meter)
	}()

	go claims.StartCapacityWatcher(ctx)

	mgrErr := make(chan error, 1)
	go func() {
		mgrErr <- mgr.Start(ctx)
	}()

	klog.InfoS("wrapper-server listening",
		"addr", addr,
		"namespace", pod.namespace,
		"pod", pod.podName,
		"image", pod.image,
		"repository", pod.repository,
		"version", pod.version,
		"imageKey", pod.imageKey,
	)
	if pod.image == fnpod.PlaceholderFunctionImage {
		klog.ErrorS(fmt.Errorf("FUNCTION_IMAGE is %s", fnpod.PlaceholderFunctionImage),
			"porch placeholder was not replaced; this pod will skip real FunctionPodRequests")
	}
	if metricsAddr != "" {
		klog.Infof("prometheus metrics listening on %s", metricsAddr)
	}

	select {
	case err := <-mgrErr:
		cancelGRPC()
		if gErr := <-grpcErr; gErr != nil && err == nil {
			err = gErr
		}
		if err != nil {
			return fmt.Errorf("manager: %w", err)
		}
		return ctx.Err()
	case err := <-grpcErr:
		cancel()
		if mErr := <-mgrErr; mErr != nil && err == nil {
			err = mErr
		}
		return err
	}
}

func wrapperPodFrom(op *Options) (wrapperPod, error) {
	ns := strings.TrimSpace(op.Namespace)
	if ns == "" {
		ns = strings.TrimSpace(os.Getenv("FUNCTION_POD_REQUEST_NAMESPACE"))
	}
	if ns == "" {
		ns = strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	}
	podName := strings.TrimSpace(op.PodName)
	if podName == "" {
		podName = strings.TrimSpace(os.Getenv("POD_NAME"))
	}
	resolved := strings.TrimSpace(op.ResolvedVersion)
	if resolved == "" {
		resolved = DefaultResolvedVersion()
	}
	fnImage := strings.TrimSpace(op.FunctionImage)
	if fnImage == "" {
		fnImage = strings.TrimSpace(os.Getenv(fnpod.EnvFunctionImage))
	}
	fnImage = resolveFunctionImage(fnImage, strings.TrimSpace(os.Getenv(fnpod.EnvFunctionRepository)), resolved)
	podIP := strings.TrimSpace(op.PodIP)
	if podIP == "" {
		podIP = DefaultPodIP()
	}
	imageKey := strings.TrimSpace(op.ImageKey)
	if imageKey == "" {
		imageKey = strings.TrimSpace(os.Getenv(fnpod.EnvImageKey))
	}
	if ns == "" {
		return wrapperPod{}, fmt.Errorf("namespace is required (set --namespace or POD_NAMESPACE)")
	}
	if podName == "" {
		return wrapperPod{}, fmt.Errorf("pod name is required (set --pod-name or POD_NAME)")
	}
	if fnImage == "" {
		return wrapperPod{}, fmt.Errorf("function image is required (set --function-image or FUNCTION_IMAGE)")
	}
	if len(op.Entrypoint) == 0 {
		return wrapperPod{}, fmt.Errorf("entrypoint is required after -- on the command line")
	}
	repository, err := functionRepository(fnImage)
	if err != nil {
		return wrapperPod{}, fmt.Errorf("function image %q: %w", fnImage, err)
	}
	if imageKey == "" {
		imageKey, err = fnpod.ImageKey(repository)
		if err != nil {
			return wrapperPod{}, fmt.Errorf("image key for %q: %w", repository, err)
		}
	}
	grpcPort := op.GRPCPort
	if grpcPort == 0 {
		grpcPort = int32(op.Port)
	}
	return wrapperPod{
		namespace:  ns,
		podName:    podName,
		podIP:      podIP,
		image:      fnImage,
		repository: repository,
		imageKey:   imageKey,
		version:    resolved,
		grpcPort:   grpcPort,
	}, nil
}

// resolveFunctionImage replaces the porch template placeholder using repository and tag.
func resolveFunctionImage(fnImage, repository, resolvedVersion string) string {
	fnImage = strings.TrimSpace(fnImage)
	if fnImage != "" && fnImage != fnpod.PlaceholderFunctionImage {
		return fnImage
	}
	repository = strings.TrimSpace(repository)
	if repository == "" {
		return fnImage
	}
	if resolvedVersion != "" && resolvedVersion != "unknown" {
		return repository + ":" + resolvedVersion
	}
	return repository
}

func loadRESTConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("kubeconfig %s: %w", kubeconfigPath, err)
		}
		return cfg, nil
	}
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("cluster config: %w", err)
	}
	return cfg, nil
}
