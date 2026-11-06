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

// Package server starts the function pod manager.
package server

import (
	"context"
	"fmt"
	"os"
	"time"

	functionconfig "github.com/kptdev/kpt/function-pod-manager/api/functionconfig/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/controller"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options is the pod manager process configuration.
type Options struct {
	Kubeconfig                 string
	RequestNamespace           string
	PodNamespace               string
	RequestRetention           time.Duration
	ScaleUpDebounce            time.Duration
	ScanInterval               time.Duration
	PodTTL                     time.Duration
	Warmup                     bool
	EnablePrivateRegistries    bool
	RegistryAuthSecretPath     string
	RegistryAuthSecretName     string
	EnablePrivateRegistriesTLS bool
	TLSSecretPath              string
	MaxWaitlistLength          int
	MaxParallelPods            int
	PodReadyTimeout            time.Duration
	DefaultImagePrefix         string
	WrapperServerImage         string
	LeaderElection             bool
	HealthProbeBindAddress     string
	LeaseDuration              time.Duration
	RenewDeadline              time.Duration
	RetryPeriod                time.Duration
}

// DefaultOptions returns the porch pod-lifecycle defaults plus the pod manager flags.
func DefaultOptions() Options {
	return Options{
		RequestNamespace:       fnpod.DefaultNamespace,
		PodNamespace:           fnpod.DefaultNamespace,
		RequestRetention:       10 * time.Minute,
		ScaleUpDebounce:        5 * time.Second,
		ScanInterval:           time.Minute,
		PodTTL:                 30 * time.Minute,
		Warmup:                 true,
		RegistryAuthSecretPath: "/var/tmp/config-secret/.dockerconfigjson",
		RegistryAuthSecretName: "auth-secret",
		TLSSecretPath:          "/var/tmp/tls-secret/",
		MaxWaitlistLength:      2,
		MaxParallelPods:        1,
		PodReadyTimeout:        60 * time.Second,
		DefaultImagePrefix:     fnpod.DefaultImagePrefix,
		WrapperServerImage:     os.Getenv(fnpod.WrapperServerImageEnv),
		LeaderElection:         true,
		HealthProbeBindAddress: ":8081",
	}
}

// Start runs the leader-elected controller manager until ctx is canceled.
func Start(ctx context.Context, opts Options) error {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register core types: %w", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register function pod request types: %w", err)
	}
	if err := functionconfig.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register porch config types: %w", err)
	}
	restConfig, err := loadConfig(opts.Kubeconfig)
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(restConfig, managerOptions(opts, scheme))
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	requests := newFunctionPodRequestReconciler(mgr, opts)
	if err := requests.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup request controller: %w", err)
	}
	configs := &controller.FunctionConfigReconciler{
		Client: mgr.GetClient(),
		Pods:   requests,
		Warmup: opts.Warmup,
	}
	if err := configs.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup function config controller: %w", err)
	}
	collector := &controller.GarbageCollector{
		FunctionPodRequestReconciler: requests,
		ScanInterval:                 opts.ScanInterval,
		WarmupOnStart:                opts.Warmup,
	}
	if err := mgr.Add(collector); err != nil {
		return fmt.Errorf("add garbage collector: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add healthz check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readyz check: %w", err)
	}
	return mgr.Start(ctx)
}

func newFunctionPodRequestReconciler(mgr ctrl.Manager, opts Options) *controller.FunctionPodRequestReconciler {
	prov := provisioner.New(mgr.GetClient(), provisioner.Config{
		Namespace:                  opts.PodNamespace,
		WrapperServerImage:         opts.WrapperServerImage,
		DefaultImagePrefix:         opts.DefaultImagePrefix,
		EnablePrivateRegistries:    opts.EnablePrivateRegistries,
		RegistryAuthSecretPath:     opts.RegistryAuthSecretPath,
		RegistryAuthSecretName:     opts.RegistryAuthSecretName,
		EnablePrivateRegistriesTLS: opts.EnablePrivateRegistriesTLS,
		TLSSecretPath:              opts.TLSSecretPath,
	})
	return &controller.FunctionPodRequestReconciler{
		Client: mgr.GetClient(),
		// The legacy recorder matches record.EventRecorder, which the unit tests implement.
		Recorder:         mgr.GetEventRecorderFor(fnpod.FieldManager), //nolint:staticcheck
		Provisioner:      prov,
		RequestNamespace: opts.RequestNamespace,
		PodNamespace:     opts.PodNamespace,
		ImagePrefix:      opts.DefaultImagePrefix,
		Debounce:         opts.ScaleUpDebounce,
		Retention:        opts.RequestRetention,
		Limits: controller.Limits{
			PreferredQueue:  opts.MaxWaitlistLength,
			MaxParallel:     opts.MaxParallelPods,
			PodTTL:          opts.PodTTL,
			PodReadyTimeout: opts.PodReadyTimeout,
		},
	}
}

func managerOptions(opts Options, scheme *runtime.Scheme) ctrl.Options {
	namespaces := map[string]cache.Config{
		opts.PodNamespace:     {},
		opts.RequestNamespace: {},
	}
	healthProbe := opts.HealthProbeBindAddress
	if healthProbe == "" {
		healthProbe = ":8081"
	}
	return ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress:     healthProbe,
		LeaderElection:             opts.LeaderElection,
		LeaderElectionID:           fnpod.LeaderElectionID,
		LeaderElectionNamespace:    opts.PodNamespace,
		LeaderElectionResourceLock: "leases",
		LeaseDuration:              positiveDuration(opts.LeaseDuration),
		RenewDeadline:              positiveDuration(opts.RenewDeadline),
		RetryPeriod:                positiveDuration(opts.RetryPeriod),
		Cache: cache.Options{
			DefaultNamespaces: namespaces,
		},
	}
}

func positiveDuration(d time.Duration) *time.Duration {
	if d <= 0 {
		return nil
	}
	return &d
}

func loadConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		cfg, err := rest.InClusterConfig()
		if err == nil {
			return cfg, nil
		}
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig: %w", err)
		}
		return cfg, nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfig, err)
	}
	return cfg, nil
}
