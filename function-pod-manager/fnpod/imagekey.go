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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
)

// LabelImageKey is set on FunctionPodRequest by clients. Evaluator pods watch requests with the same value.
const LabelImageKey = "fn.kpt.dev/image-key"

// EnvImageKey is the label value this evaluator pod watches (injected by the Pod Manager).
const EnvImageKey = "IMAGE_KEY"

// ImageKey returns a DNS-label-safe key for repository-only function identity (NCARTBL-1280).
func ImageKey(repository string) (string, error) {
	repo, err := name.NewRepository(repository)
	if err != nil {
		return "", fmt.Errorf("parse repository %q: %w", repository, err)
	}
	parts := strings.Split(repo.RepositoryStr(), "/")
	base := strings.ToLower(strings.ReplaceAll(parts[len(parts)-1], "_", "-"))
	base = strings.Trim(base, "-.")
	if base == "" {
		return "", fmt.Errorf("repository %q has no function name", repository)
	}
	sum := sha256.Sum256([]byte(repo.RepositoryStr()))
	suffix := hex.EncodeToString(sum[:])[:8]
	key := base + "-" + suffix
	if len(key) > 63 {
		key = strings.Trim(key[:63], "-.")
	}
	return key, nil
}
