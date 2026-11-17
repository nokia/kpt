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
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const (
	mainTestImage      = "example.com/krm/sleep:v1.2.3"
	mainTestNamespace  = "fn-system"
	mainTestPodName    = "eval-0"
	mainTestPodIP      = "10.0.0.8"
	mainTestKubeconfig = "/tmp/kube.config"
	mainTestVersion    = "v1.2.3"
	mainTestEntrypoint = "/bin/cat"
)

func TestRootCommandDefaultsTheListenPort(t *testing.T) {
	cmd := buildRootCommand()

	port, portErr := cmd.Flags().GetInt("port")
	grace, graceErr := cmd.Flags().GetDuration("claim-grace-period")

	require.NoError(t, portErr)
	require.NoError(t, graceErr)
	require.Equal(t, 9446, port)
	require.Equal(t, time.Duration(0), grace)
	require.True(t, cmd.SilenceUsage)
	require.True(t, cmd.SilenceErrors)
}

func TestMainExitsWhenTheEntrypointIsMissing(t *testing.T) {
	restoreArgs(t, []string{"wrapper-server"})

	code, exited := callMain(t)

	require.True(t, exited)
	require.Equal(t, 1, code)
}

func TestMainStartsTheWrapperWhenFlagsAreValid(t *testing.T) {
	var got *Options
	stubRunner(t, func(_ context.Context, op *Options) error {
		got = op
		return nil
	})
	restoreArgs(t, wrapperArgs())

	_, exited := callMain(t)

	require.False(t, exited)
	requireOptionsMatchFlags(t, got)
}

func TestRunWrapperServerPassesFlagsToTheServer(t *testing.T) {
	var got *Options
	stubRunner(t, func(_ context.Context, op *Options) error {
		got = op
		return nil
	})
	cmd := buildRootCommand()
	cmd.SetArgs(wrapperArgs()[1:])

	err := cmd.Execute()

	require.NoError(t, err)
	requireOptionsMatchFlags(t, got)
}

func TestRunWrapperServerReturnsTheServerError(t *testing.T) {
	stubRunner(t, func(context.Context, *Options) error {
		return errors.New("bind failed")
	})
	cmd := buildRootCommand()
	cmd.SetArgs([]string{"--", mainTestEntrypoint})

	err := cmd.Execute()

	require.EqualError(t, err, "bind failed")
}

func TestRunWrapperServerReportsAnUnreadableFlag(t *testing.T) {
	flagsInOrder := []string{
		"port",
		"claim-grace-period",
		"kubeconfig",
		"namespace",
		"pod-name",
		"pod-ip",
		"grpc-port",
		"function-image",
		"resolved-version",
	}
	for i, missing := range flagsInOrder {
		t.Run(missing, func(t *testing.T) {
			cmd := commandWithFlags(flagsInOrder[:i]...)

			err := runWrapperServer(cmd, []string{mainTestEntrypoint})

			require.ErrorContains(t, err, missing)
		})
	}
}

func wrapperArgs() []string {
	return []string{
		"wrapper-server",
		"--kubeconfig=" + mainTestKubeconfig,
		"--port=9447",
		"--claim-grace-period=2s",
		"--namespace=" + mainTestNamespace,
		"--pod-name=" + mainTestPodName,
		"--pod-ip=" + mainTestPodIP,
		"--grpc-port=9448",
		"--function-image=" + mainTestImage,
		"--resolved-version=" + mainTestVersion,
		"--",
		mainTestEntrypoint,
	}
}

func requireOptionsMatchFlags(t *testing.T, op *Options) {
	t.Helper()
	require.Equal(t, &Options{
		Kubeconfig:       mainTestKubeconfig,
		Port:             9447,
		ClaimGracePeriod: 2 * time.Second,
		Namespace:        mainTestNamespace,
		PodName:          mainTestPodName,
		PodIP:            mainTestPodIP,
		GRPCPort:         9448,
		FunctionImage:    mainTestImage,
		ResolvedVersion:  mainTestVersion,
		Entrypoint:       []string{mainTestEntrypoint},
	}, op)
}

func stubRunner(t *testing.T, fn func(context.Context, *Options) error) {
	t.Helper()
	origRun := runServer
	origSignals := setupSignals
	runServer = fn
	setupSignals = context.Background
	t.Cleanup(func() {
		runServer = origRun
		setupSignals = origSignals
	})
}

func restoreArgs(t *testing.T, args []string) {
	t.Helper()
	orig := os.Args
	os.Args = args
	t.Cleanup(func() { os.Args = orig })
}

type exitPanic struct{ code int }

func callMain(t *testing.T) (status int, exited bool) {
	t.Helper()
	orig := exit
	exit = func(code int) {
		status = code
		exited = true
		panic(exitPanic{code: code})
	}
	t.Cleanup(func() { exit = orig })
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		if _, ok := recovered.(exitPanic); !ok {
			panic(recovered)
		}
	}()
	main()
	return status, exited
}

func commandWithFlags(names ...string) *cobra.Command {
	cmd := &cobra.Command{Use: "wrapper-server"}
	for _, name := range names {
		switch name {
		case "port":
			cmd.Flags().Int(name, 9446, "")
		case "claim-grace-period":
			cmd.Flags().Duration(name, 0, "")
		case "grpc-port":
			cmd.Flags().Int32(name, 0, "")
		default:
			cmd.Flags().String(name, "", "")
		}
	}
	return cmd
}
