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
	"testing"
	"time"

	"github.com/kptdev/kpt/api/runneroptions"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlagDefaults(t *testing.T) {
	opts := server.DefaultOptions()
	cmd := newCommand(&opts)

	assert.Equal(t, fnpod.DefaultNamespace, opts.RequestNamespace)
	assert.Equal(t, fnpod.DefaultNamespace, opts.PodNamespace)
	assert.Equal(t, 10*time.Minute, opts.RequestRetention)
	assert.Equal(t, 5*time.Second, opts.ScaleUpDebounce)
	assert.Equal(t, time.Minute, opts.ScanInterval)
	assert.Equal(t, 8, opts.ReapParallelism)
	assert.Equal(t, 30*time.Minute, opts.PodTTL)
	assert.Equal(t, 2, opts.MaxWaitlistLength)
	assert.Equal(t, 1, opts.MaxParallelPods)
	assert.Equal(t, 60*time.Second, opts.PodReadyTimeout)
	assert.True(t, opts.Warmup)
	assert.True(t, opts.LeaderElection)
	assert.Equal(t, runneroptions.GHCRImagePrefix, opts.DefaultImagePrefix)

	flags := cmd.Flags()
	require.NotNil(t, flags.Lookup("fn-request-namespace"))
	require.NotNil(t, flags.Lookup("request-retention"))
	require.NotNil(t, flags.Lookup("leader-elect"))
	require.NotNil(t, flags.Lookup("pod-ttl"))
	require.NotNil(t, flags.Lookup("reap-parallelism"))
	require.NotNil(t, flags.Lookup("warm-up-pod-cache"))
	assert.Nil(t, flags.Lookup("max-function-retries"))
	assert.Nil(t, flags.Lookup("function-retry-interval"))
	assert.Equal(t, "true", flags.Lookup("leader-elect").DefValue)
}
