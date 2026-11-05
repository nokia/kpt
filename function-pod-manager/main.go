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
	"flag"
	"os"

	"github.com/kptdev/kpt/function-pod-manager/internal/server"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	klog.InitFlags(nil)
	opts := server.DefaultOptions()
	cmd := newCommand(&opts)
	if err := cmd.Execute(); err != nil {
		klog.ErrorS(err, "function pod manager failed")
		os.Exit(1)
	}
}

func newCommand(opts *server.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "pod-manager",
		Short:         "Scale Function Evaluator pods for FunctionPodRequest objects",
		Long:          "A leader-elected controller that keeps a pool of Function Evaluator pods for KRM function evaluation requests.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return server.Start(ctrl.SetupSignalHandler(), *opts)
		},
	}
	cmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	addFlags(cmd, opts)
	return cmd
}

func addFlags(cmd *cobra.Command, opts *server.Options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.Kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flags.StringVar(&opts.RequestNamespace, "fn-request-namespace", opts.RequestNamespace, "Namespace holding FunctionPodRequest objects.")
	flags.StringVar(&opts.PodNamespace, "pod-namespace", opts.PodNamespace, "Namespace for Function Evaluator pods, Services, templates, and FunctionConfig.")
	flags.DurationVar(&opts.RequestRetention, "request-retention", opts.RequestRetention, "How long error-phase requests are kept.")
	flags.DurationVar(&opts.ScaleUpDebounce, "scale-up-debounce", opts.ScaleUpDebounce, "How long a request must stay Created before a new pod is started.")
	flags.DurationVar(&opts.ScanInterval, "scan-interval", opts.ScanInterval, "The interval of GC between scans.")
	flags.DurationVar(&opts.PodTTL, "pod-ttl", opts.PodTTL, "TTL for pods before GC.")
	flags.BoolVar(&opts.Warmup, "warm-up-pod-cache", opts.Warmup, "If true, pods for concrete FunctionConfig tags are deployed at startup.")
	flags.BoolVar(&opts.EnablePrivateRegistries, "enable-private-registries", opts.EnablePrivateRegistries,
		"If true, enables private registries and their authentication.")
	flags.StringVar(&opts.RegistryAuthSecretPath, "registry-auth-secret-path", opts.RegistryAuthSecretPath,
		"Path of the secret used for authenticating to custom registries.")
	flags.StringVar(&opts.RegistryAuthSecretName, "registry-auth-secret-name", opts.RegistryAuthSecretName,
		"Name of the secret used for authenticating to custom registries.")
	flags.BoolVar(&opts.EnablePrivateRegistriesTLS, "enable-private-registries-tls", opts.EnablePrivateRegistriesTLS,
		"If enabled, use the provided TLS secret when accessing registries.")
	flags.StringVar(&opts.TLSSecretPath, "tls-secret-path", opts.TLSSecretPath,
		"Path of the secret used in TLS configuration.")
	flags.IntVar(&opts.MaxWaitlistLength, "max-waitlist-length", opts.MaxWaitlistLength,
		"Maximum waitlist length per pod.")
	flags.IntVar(&opts.MaxParallelPods, "max-parallel-pods-per-function", opts.MaxParallelPods,
		"Maximum parallel pods per function.")
	flags.DurationVar(&opts.PodReadyTimeout, "pod-ready-timeout", opts.PodReadyTimeout,
		"How long to wait for a function evaluator pod to become Ready.")
	flags.StringVar(&opts.DefaultImagePrefix, "default-image-prefix", opts.DefaultImagePrefix,
		"Default prefix for unqualified function names.")
	flags.BoolVar(&opts.LeaderElection, "leader-elect", opts.LeaderElection,
		"Enable leader election so only one manager creates and deletes evaluator pods.")
	flags.DurationVar(&opts.LeaseDuration, "leader-lease-duration", opts.LeaseDuration,
		"How long non-leaders wait before forcing leadership. Zero uses the controller-runtime default.")
	flags.DurationVar(&opts.RenewDeadline, "leader-renew-deadline", opts.RenewDeadline,
		"How long the leader retries refreshing leadership. Zero uses the controller-runtime default.")
	flags.DurationVar(&opts.RetryPeriod, "leader-retry-period", opts.RetryPeriod,
		"How long leader election waits between retries. Zero uses the controller-runtime default.")
}
