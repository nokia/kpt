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

	"github.com/spf13/cobra"
	contextsignal "k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"
)

// exit, setupSignals, and runServer are the process edges. Tests replace them
// so a failed command does not terminate the test binary.
var (
	exit         = os.Exit
	setupSignals = contextsignal.SetupSignalContext
	runServer    = Run
)

func main() {
	if err := execute(os.Args[1:]); err != nil {
		klog.ErrorS(err, "wrapper-server failed")
		exit(1)
	}
}

func execute(args []string) error {
	cmd := buildRootCommand()
	cmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	cmd.InitDefaultHelpCmd()
	cmd.SetArgs(args)
	return cmd.Execute()
}

func buildRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "wrapper-server",
		Short:         "Function Evaluator Pod: gRPC server with FunctionPodRequest claim controller",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          runWrapperServer,
		Args:          cobra.MinimumNArgs(1),
	}

	klogFlagSet := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFlagSet)
	root.PersistentFlags().AddGoFlagSet(klogFlagSet)

	root.Flags().String("kubeconfig", "", "Path to kubeconfig (empty uses in-cluster config)")
	root.Flags().Int("port", 9446, "gRPC listen port")
	root.Flags().Duration("claim-grace-period", 0, "If set, fail the claim when no EvaluateFunction arrives within this duration (0 disables)")
	root.Flags().String("namespace", "", "Namespace for FunctionPodRequests (default: POD_NAMESPACE)")
	root.Flags().String("pod-name", "", "This pod's name (default: POD_NAME)")
	root.Flags().String("pod-ip", "", "This pod's IP (default: POD_IP or /etc/podinfo/pod_ip)")
	root.Flags().Int32("grpc-port", 0, "Port recorded in FunctionPodRequest status (default: same as --port)")
	root.Flags().String("function-image", "", "KRM function image this pod runs (default: FUNCTION_IMAGE)")
	root.Flags().String("resolved-version", "", "Concrete version in status (default: VERSION env or \"unknown\")")

	return root
}

func runWrapperServer(cmd *cobra.Command, args []string) error {
	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return err
	}
	claimGrace, err := cmd.Flags().GetDuration("claim-grace-period")
	if err != nil {
		return err
	}
	kubeconfig, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return err
	}
	namespace, err := cmd.Flags().GetString("namespace")
	if err != nil {
		return err
	}
	podName, err := cmd.Flags().GetString("pod-name")
	if err != nil {
		return err
	}
	podIP, err := cmd.Flags().GetString("pod-ip")
	if err != nil {
		return err
	}
	grpcPort, err := cmd.Flags().GetInt32("grpc-port")
	if err != nil {
		return err
	}
	functionImage, err := cmd.Flags().GetString("function-image")
	if err != nil {
		return err
	}
	resolvedVersion, err := cmd.Flags().GetString("resolved-version")
	if err != nil {
		return err
	}

	op := &Options{
		Kubeconfig:       kubeconfig,
		Port:             port,
		ClaimGracePeriod: claimGrace,
		Namespace:        namespace,
		PodName:          podName,
		PodIP:            podIP,
		GRPCPort:         grpcPort,
		FunctionImage:    functionImage,
		ResolvedVersion:  resolvedVersion,
		Entrypoint:       args,
	}
	return runServer(setupSignals(), op)
}
