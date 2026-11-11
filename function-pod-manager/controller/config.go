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
	"sync"

	"github.com/kptdev/kpt/function-pod-manager/provisioner/util"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// FunctionConfigStore is the in-memory copy of FunctionConfig pod executors.
// The FunctionConfig reconciler is the writer. Scale and idle cleanup read it.
type FunctionConfigStore struct {
	mu     sync.RWMutex
	synced bool
	items  []cachedFunctionConfig
}

type cachedFunctionConfig struct {
	key   types.NamespacedName
	image string
	exec  *functionconfig.PodExecutorConfig
}

// NewFunctionConfigStore returns an empty store. synced is false until the first sync.
func NewFunctionConfigStore() *FunctionConfigStore {
	return &FunctionConfigStore{}
}

func (s *FunctionConfigStore) sync(list func() ([]functionconfig.FunctionConfig, error)) error {
	s.mu.Lock()
	if s.synced {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	items, err := list()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.synced {
		return nil
	}
	for i := range items {
		key := types.NamespacedName{Namespace: items[i].Namespace, Name: items[i].Name}
		if !s.hasLocked(key) {
			s.insertLocked(&items[i])
		}
	}
	s.synced = true
	return nil
}

func (s *FunctionConfigStore) upsert(obj *functionconfig.FunctionConfig) {
	if s == nil || obj == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := types.NamespacedName{Namespace: obj.Namespace, Name: obj.Name}
	if s.hasLocked(key) {
		s.replaceLocked(obj)
		return
	}
	s.insertLocked(obj)
}

func (s *FunctionConfigStore) delete(key types.NamespacedName) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		if s.items[i].key != key {
			continue
		}
		s.items = append(s.items[:i], s.items[i+1:]...)
		return
	}
}

func (s *FunctionConfigStore) getPodPoolPolicy(repository, version, prefix string, defaults PodPoolPolicy) PodPoolPolicy {
	exec := s.executor(repository, version, prefix)
	if exec == nil {
		return defaults
	}
	out := defaults
	if exec.PreferredMaxQueueLength > 0 {
		out.PreferredMaxQueueLength = exec.PreferredMaxQueueLength
	}
	if exec.MaxParallelExecutions > 0 {
		out.MaxParallelExecutions = exec.MaxParallelExecutions
	}
	if exec.TimeToLive.Duration > 0 {
		out.PodTTL = exec.TimeToLive.Duration
	}
	return out
}

func (s *FunctionConfigStore) executor(repository, version, prefix string) *functionconfig.PodExecutorConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var wildcard *functionconfig.PodExecutorConfig
	for i := range s.items {
		exec := s.items[i].exec
		if exec == nil || repositoryOf(s.items[i].image, prefix) != repository {
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

func (s *FunctionConfigStore) hasLocked(key types.NamespacedName) bool {
	for i := range s.items {
		if s.items[i].key == key {
			return true
		}
	}
	return false
}

func (s *FunctionConfigStore) replaceLocked(obj *functionconfig.FunctionConfig) {
	key := types.NamespacedName{Namespace: obj.Namespace, Name: obj.Name}
	for i := range s.items {
		if s.items[i].key == key {
			s.items[i] = cachedFrom(obj)
			return
		}
	}
}

func (s *FunctionConfigStore) insertLocked(obj *functionconfig.FunctionConfig) {
	s.items = append(s.items, cachedFrom(obj))
}

func cachedFrom(obj *functionconfig.FunctionConfig) cachedFunctionConfig {
	var exec *functionconfig.PodExecutorConfig
	if obj.Spec.PodExecutor != nil {
		exec = obj.Spec.PodExecutor.DeepCopy()
	}
	return cachedFunctionConfig{
		key:   types.NamespacedName{Namespace: obj.Namespace, Name: obj.Name},
		image: obj.Spec.Image,
		exec:  exec,
	}
}

func containsStar(tags []string) bool {
	for _, tag := range tags {
		if tag == "*" {
			return true
		}
	}
	return false
}
