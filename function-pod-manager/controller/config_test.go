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

package controller

import (
	"testing"

	"github.com/kptdev/kpt/api/runneroptions"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestExecutorMatchesMirrorRepository(t *testing.T) {
	const defaultPrefix = runneroptions.GHCRImagePrefix
	mirror := "nc-docker-dependencies.repo.cci.nokia.net/kptdev/krm-functions-catalog"

	cfg := &functionconfig.FunctionConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "sleep", Namespace: "porch-fn-system"},
		Spec: functionconfig.FunctionConfigSpec{
			Image: "sleep",
			Prefixes: []string{
				"",
				mirror,
			},
			PodExecutor: &functionconfig.PodExecutorConfig{
				Tags: []string{"*"},
				TemplateOverrides: &functionconfig.TemplateOverrides{
					ServiceAccountName: "test-service-account",
				},
			},
		},
	}

	store := NewFunctionConfigStore()
	store.upsert(cfg)

	ghcrRepo := repositoryOf("sleep", defaultPrefix)
	mirrorRepo := mirror + "/sleep"

	got := store.executor(mirrorRepo, "latest", defaultPrefix)
	assert.NotNil(t, got)
	assert.Equal(t, "test-service-account", got.TemplateOverrides.ServiceAccountName)

	assert.NotNil(t, store.executor(ghcrRepo, "latest", defaultPrefix))
	assert.Nil(t, store.executor("ghcr.io/kptdev/krm-functions-catalog/set-namespace", "latest", defaultPrefix))
}
