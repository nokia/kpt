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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectTagPicksHighestPrefixedVersion(t *testing.T) {
	tag, err := SelectTag([]string{"v1.0.7", "v1.1.2", "1.9.0", "latest"}, "~1.0.3")
	require.NoError(t, err)
	assert.Equal(t, "v1.0.7", tag)

	tag, err = SelectTag([]string{"v1.0.7", "v1.1.2", "1.9.0"}, "^1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "v1.1.2", tag)

	_, err = SelectTag([]string{"v1.0.7"}, "~9.0.0")
	require.ErrorIs(t, err, ErrNoMatchingTag)
	assert.True(t, VersionSatisfies("v1.0.7", "~1.0.3"))
	assert.False(t, VersionSatisfies("v1.0.7", "~1.1.0"))
	assert.True(t, ConcreteVersion("v1.2.3"))
	assert.False(t, ConcreteVersion("~1.0.3"))
}
