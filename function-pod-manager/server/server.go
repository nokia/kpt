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

	"github.com/kptdev/kpt/api/runneroptions"
	"github.com/kptdev/kpt/function-pod-manager/api/functionpodrequest/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/controller"
	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/provisioner"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
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
	Kubeconfig string
	// RequestNamespace is the namespace that holds FunctionPodRequest objects.
	RequestNamespace string
	// PodNamespace is the namespace for evaluator pods, Services, templates, and FunctionConfig.
	PodNamespace string

	// RequestRetention is how long a PodManagementError or EvaluationError request is kept.
	RequestRetention time.Duration
	// ScaleUpDebounce is how long a request must stay Created before a new evaluator pod is started.
	ScaleUpDebounce time.Duration

	// ScanInterval is how often the garbage collector reaps idle pods and expired requests.
	ScanInterval time.Duration
	// ReapParallelism is how many requests one scan reaps at once. Zero uses 8.
	ReapParallelism int
	// PodTTL is how long an idle evaluator pod is kept.
	PodTTL time.Duration
	// Warmup starts a pod for each concrete FunctionConfig tag when the manager starts.
	Warmup bool
	// MaxWaitlistLength is how many Created requests one pod may cover before another pod of that function is started.
	MaxWaitlistLength int
	// MaxParallelPods is the most evaluator pods one function may run.
	MaxParallelPods int
	// PodReadyTimeout is how long a new pod may stay unready before the request fails.
	PodReadyTimeout time.Duration

	// DefaultImagePrefix is prepended to a function name that is not a full image reference.
	DefaultImagePrefix string
	// WrapperServerImage is the image that provides the wrapper-server binary copied into the evaluator pod.
	WrapperServerImage         string
	EnablePrivateRegistries    bool
	RegistryAuthSecretPath     string
	RegistryAuthSecretName     string
	EnablePrivateRegistriesTLS bool
	TLSSecretPath              string

	LeaderElection         bool
	HealthProbeBindAddress string
	// LeaseDuration is how long non-leaders wait before taking leadership. Zero uses the controller-runtime default.
	LeaseDuration time.Duration
	// RenewDeadline is how long the leader retries refreshing leadership. Zero uses the controller-runtime default.
	RenewDeadline time.Duration
	// RetryPeriod is how long leader election waits between retries. Zero uses the controller-runtime default.
	RetryPeriod time.Duration
}

// DefaultOptions returns the porch pod-lifecycle defaults plus the pod manager flags.
func DefaultOptions() Options {
	return Options{
		RequestNamespace:       fnpod.DefaultNamespace,
		PodNamespace:           fnpod.DefaultNamespace,
		RequestRetention:       10 * time.Minute,
		ScaleUpDebounce:        5 * time.Second,
		ScanInterval:           time.Minute,
		ReapParallelism:        controller.DefaultReapParallelism,
		PodTTL:                 30 * time.Minute,
		Warmup:                 true,
		RegistryAuthSecretPath: "/var/tmp/config-secret/.dockerconfigjson",
		RegistryAuthSecretName: "auth-secret",
		TLSSecretPath:          "/var/tmp/tls-secret/",
		MaxWaitlistLength:      2,
		MaxParallelPods:        1,
		PodReadyTimeout:        60 * time.Second,
		DefaultImagePrefix:     runneroptions.GHCRImagePrefix,
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
	store := controller.NewFunctionConfigStore()
	requests.FunctionConfigStore = store
	if err := requests.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup request controller: %w", err)
	}
	configs := &controller.FunctionConfigReconciler{
		Client: mgr.GetClient(),
		Pods:   requests,
		Store:  store,
		Warmup: opts.Warmup,
	}
	if err := configs.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup function config controller: %w", err)
	}
	collector := &controller.GarbageCollector{
		FunctionPodRequestReconciler: requests,
		ScanInterval:                 opts.ScanInterval,
		WarmupOnStart:                opts.Warmup,
		ReapParallelism:              opts.ReapParallelism,
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
		PodPoolPolicy: controller.PodPoolPolicy{
			PreferredMaxQueueLength: opts.MaxWaitlistLength,
			MaxParallelExecutions:   opts.MaxParallelPods,
			PodTTL:                  opts.PodTTL,
			PodReadyTimeout:         opts.PodReadyTimeout,
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
