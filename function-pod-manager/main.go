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
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	var kubeconfig *string

	cmd := &cobra.Command{
		Use:          "pod-manager",
		Short:        "A manager for KRM function pods", // TODO
		Long:         "A manager for KRM function pods", // TODO
		SilenceUsage: true,
		// We handle all errors in main after return from cobra so we can
		// adjust the error message coming from libraries
		SilenceErrors: true,
		// TODO: replace RunE with starting the manager
		RunE: func(cmd *cobra.Command, _ []string) error {
			if kubeconfig != nil && *kubeconfig != "" {
				fmt.Println("Using kubeconfig:", *kubeconfig)
			} else {
				fmt.Println("Hello World!")
			}
			return nil
		},
	}

	// TODO: copied from cli/run.go, check if needed
	cmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	cmd.InitDefaultHelpCmd()

	// TODO: add more flags
	kubeconfig = cmd.Flags().String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")

	err := cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
