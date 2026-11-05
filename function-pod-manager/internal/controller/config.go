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
	functionconfig "github.com/kptdev/kpt/function-pod-manager/api/functionconfig/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner/util"
)

func limitsFor(configs []functionconfig.FunctionConfig, repository, version, prefix string, defaults Limits) Limits {
	exec := executorFor(configs, repository, version, prefix)
	if exec == nil {
		return defaults
	}
	out := defaults
	if exec.PreferredMaxQueueLength > 0 {
		out.PreferredQueue = exec.PreferredMaxQueueLength
	}
	if exec.MaxParallelExecutions > 0 {
		out.MaxParallel = exec.MaxParallelExecutions
	}
	if exec.TimeToLive.Duration > 0 {
		out.PodTTL = exec.TimeToLive.Duration
	}
	return out
}

func executorFor(configs []functionconfig.FunctionConfig, repository, version, prefix string) *functionconfig.PodExecutorConfig {
	var wildcard *functionconfig.PodExecutorConfig
	for i := range configs {
		exec := configs[i].Spec.PodExecutor
		if exec == nil {
			continue
		}
		repo, err := repositoryOf(configs[i].Spec.Image, prefix)
		if err != nil || repo != repository {
			continue
		}
		if util.TagsMatch(version, exec.Tags) {
			return exec
		}
		if len(exec.Tags) == 0 || containsStar(exec.Tags) {
			wildcard = exec
		}
	}
	return wildcard
}

func containsStar(tags []string) bool {
	for _, tag := range tags {
		if tag == "*" {
			return true
		}
	}
	return false
}
