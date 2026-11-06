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

package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
)

const testKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:1
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    token: test
`

func TestDefaultOptionsReadsWrapperImage(t *testing.T) {
	t.Setenv(fnpod.WrapperServerImageEnv, "wrapper:from-env")

	opts := DefaultOptions()

	assert.Equal(t, "wrapper:from-env", opts.WrapperServerImage)
	assert.Equal(t, fnpod.DefaultNamespace, opts.RequestNamespace)
	assert.Equal(t, 10*time.Minute, opts.RequestRetention)
	assert.True(t, opts.LeaderElection)
	assert.Equal(t, ":8081", opts.HealthProbeBindAddress)
	assert.Equal(t, 2, opts.MaxWaitlistLength)
	assert.Equal(t, 1, opts.MaxParallelPods)
}

func TestLoadConfigUsesExplicitFileAndRejectsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(testKubeconfig), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "https://127.0.0.1:1", cfg.Host)

	_, err = loadConfig(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "load kubeconfig")
}

func TestLoadConfigFallsBackWhenRunningOutsideTheCluster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(testKubeconfig), 0o600))
	t.Setenv("KUBECONFIG", path)

	cfg, err := loadConfig("")

	require.NoError(t, err)
	assert.Equal(t, "https://127.0.0.1:1", cfg.Host)

	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	_, err = loadConfig("")
	require.Error(t, err)
	assert.ErrorContains(t, err, "load kubeconfig")
}

func TestManagerOptionsOmitNonPositiveLeaseDurations(t *testing.T) {
	opts := DefaultOptions()
	opts.PodNamespace = "pods"
	opts.RequestNamespace = "requests"
	opts.LeaderElection = false
	opts.LeaseDuration = time.Second
	opts.RenewDeadline = 0
	opts.HealthProbeBindAddress = ""
	opts.RetryPeriod = -1
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	got := managerOptions(opts, scheme)

	assert.False(t, got.LeaderElection)
	assert.Equal(t, fnpod.LeaderElectionID, got.LeaderElectionID)
	assert.Equal(t, "pods", got.LeaderElectionNamespace)
	assert.Equal(t, ":8081", got.HealthProbeBindAddress)
	assert.Equal(t, "0", got.Metrics.BindAddress)
	require.NotNil(t, got.LeaseDuration)
	assert.Equal(t, time.Second, *got.LeaseDuration)
	assert.Nil(t, got.RenewDeadline)
	assert.Nil(t, got.RetryPeriod)
	assert.Contains(t, got.Cache.DefaultNamespaces, "pods")
	assert.Contains(t, got.Cache.DefaultNamespaces, "requests")
}

func TestNewReconcilerCopiesProcessOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(testKubeconfig), 0o600))
	restConfig, err := loadConfig(path)
	require.NoError(t, err)
	opts := DefaultOptions()
	opts.PodNamespace = "fn"
	opts.RequestNamespace = "requests"
	opts.WrapperServerImage = "wrapper:test"
	opts.DefaultImagePrefix = "example.com/functions"
	opts.EnablePrivateRegistries = true
	opts.MaxParallelPods = 3
	opts.PodTTL = 12 * time.Minute
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(restConfig, managerOptions(opts, scheme))
	require.NoError(t, err)

	reconciler := newFunctionPodRequestReconciler(mgr, opts)

	assert.Equal(t, "requests", reconciler.RequestNamespace)
	assert.Equal(t, "fn", reconciler.PodNamespace)
	assert.Equal(t, "example.com/functions", reconciler.ImagePrefix)
	assert.Equal(t, 3, reconciler.Limits.MaxParallel)
	assert.Equal(t, 12*time.Minute, reconciler.Limits.PodTTL)
	assert.NotNil(t, reconciler.Provisioner)
	assert.NotNil(t, reconciler.Client)
	assert.NotNil(t, reconciler.Recorder)
}

func TestStartStopsWhenTheContextEnds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(testKubeconfig), 0o600))
	opts := DefaultOptions()
	opts.Kubeconfig = path
	opts.LeaderElection = false
	opts.WrapperServerImage = "wrapper:test"
	opts.PodNamespace = "fn"
	opts.RequestNamespace = "fn"
	opts.HealthProbeBindAddress = "127.0.0.1:0"
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := Start(ctx, opts)

	assert.NoError(t, err)
}

func TestStartRejectsAMissingKubeconfig(t *testing.T) {
	opts := DefaultOptions()
	opts.Kubeconfig = filepath.Join(t.TempDir(), "missing")

	err := Start(context.Background(), opts)

	require.Error(t, err)
	assert.ErrorContains(t, err, "load kubeconfig")
}
