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
	"os"
	"strings"
	"time"

	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	fnreqv1alpha1 "github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
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
}

// Run starts the controller-runtime manager, the claim reconciler,
// and the gRPC FunctionEvaluator server.
func Run(parent context.Context, op *Options) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
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
	fnImage := strings.TrimSpace(op.FunctionImage)
	if fnImage == "" {
		fnImage = strings.TrimSpace(os.Getenv("FUNCTION_IMAGE"))
	}
	resolved := strings.TrimSpace(op.ResolvedVersion)
	if resolved == "" {
		resolved = DefaultResolvedVersion()
	}
	podIP := strings.TrimSpace(op.PodIP)
	if podIP == "" {
		podIP = DefaultPodIP()
	}
	imageKey := strings.TrimSpace(op.ImageKey)
	if imageKey == "" {
		imageKey = strings.TrimSpace(os.Getenv(fnpod.EnvImageKey))
	}

	if ns == "" {
		return fmt.Errorf("namespace is required (set --namespace or POD_NAMESPACE)")
	}
	if podName == "" {
		return fmt.Errorf("pod name is required (set --pod-name or POD_NAME)")
	}
	if fnImage == "" {
		return fmt.Errorf("function image is required (set --function-image or FUNCTION_IMAGE)")
	}
	if len(op.Entrypoint) == 0 {
		return fmt.Errorf("entrypoint is required after -- on the command line")
	}

	repository, err := functionRepository(fnImage)
	if err != nil {
		return fmt.Errorf("function image %q: %w", fnImage, err)
	}
	if imageKey == "" {
		imageKey, err = fnpod.ImageKey(repository)
		if err != nil {
			return fmt.Errorf("image key for %q: %w", repository, err)
		}
	}

	cfg, err := loadRESTConfig(op.Kubeconfig)
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(fnreqv1alpha1.AddToScheme(scheme))

	labelSelector := labels.SelectorFromSet(labels.Set{fnpod.LabelImageKey: imageKey})
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				ns: {},
			},
			ByObject: map[client.Object]cache.ByObject{
				&fnreqv1alpha1.FunctionPodRequest{}: {
					Label: labelSelector,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	grpcPort := op.GRPCPort
	if grpcPort == 0 {
		grpcPort = int32(op.Port)
	}

	claims := &ClaimController{
		Client:           mgr.GetClient(),
		Namespace:        ns,
		PodName:          podName,
		PodIP:            podIP,
		GRPCPort:         grpcPort,
		FunctionImage:    fnImage,
		FunctionBaseName: fnpod.ImageBaseName(repository),
		ImageKey:         imageKey,
		ResolvedVersion:  resolved,
		ClaimGracePeriod: op.ClaimGracePeriod,
		requestRepository: repository,
		capacityWake:     make(chan struct{}, 1),
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
	addr := fmt.Sprintf(":%d", op.Port)
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- ServeGRPC(grpcCtx, addr, claims, evaluator, meter)
	}()

	go claims.StartCapacityWatcher(ctx)

	mgrErr := make(chan error, 1)
	go func() {
		mgrErr <- mgr.Start(ctx)
	}()

	klog.Infof("wrapper-server listening on %s (namespace=%s pod=%s image=%s image-key=%s)", addr, ns, podName, fnImage, imageKey)
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