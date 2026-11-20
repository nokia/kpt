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

package fnpod

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionSatisfiesExactCatalogTag(t *testing.T) {
	assert.True(t, VersionSatisfies("v0.1.5", "v0.1.5"))
	assert.False(t, VersionSatisfies("v0.1.5", ""))
	assert.False(t, VersionSatisfies("v0.1.5", "~2.0.0"))
	assert.True(t, VersionSatisfies(LatestTag, LatestTag))
}

func TestGetVersionConstraint(t *testing.T) {
	assert.Equal(t, "~1.0.0", GetVersionConstraint("~1.0.0", "registry/sleep:latest"))
	assert.Equal(t, LatestTag, GetVersionConstraint("", "registry/sleep:latest"))
	assert.Equal(t, "v1.2.3", GetVersionConstraint("", "registry/sleep:v1.2.3"))
	assert.Empty(t, GetVersionConstraint("", "registry/sleep"))
	assert.Empty(t, GetVersionConstraint("", "registry/sleep@sha256:abc"))
}
