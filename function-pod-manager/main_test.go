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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"
)

func Test(t *testing.T) {
	const kubeconfig = "my-kubeconfig.config"
	origState := klog.CaptureState()
	t.Cleanup(func() {
		klog.Flush()
		origState.Restore()
	})

	buffer := &bytes.Buffer{}
	klog.LogToStderr(false)
	klog.SetOutput(buffer)

	err := runPodManager(kubeconfig)

	require.NoError(t, err)
	assert.Contains(t, buffer.String(), kubeconfig)
}
