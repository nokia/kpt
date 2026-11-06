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

// Package util holds image, version, and pod-template helpers for the provisioner.
package util

import (
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
)

// ResolveImage prepends the default prefix to an unqualified function name.
func ResolveImage(image, prefix string) string {
	if prefix == "" || strings.Contains(image, "/") {
		return image
	}
	return strings.TrimRight(prefix, "/") + "/" + image
}

// Repository returns the image repository without a tag or digest.
func Repository(image string) (string, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return "", fmt.Errorf("parse image %q: %w", image, err)
	}
	return ref.Context().Name(), nil
}

// Tag returns the image tag. A digest reference has an empty tag.
func Tag(image string) (string, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return "", fmt.Errorf("parse image %q: %w", image, err)
	}
	if _, ok := ref.(name.Tag); !ok {
		return "", nil
	}
	return ref.Identifier(), nil
}

// DisplayName is the function base name, optionally with a concrete tag.
func DisplayName(repository, version string) string {
	base := repository
	if i := strings.LastIndex(repository, "/"); i >= 0 {
		base = repository[i+1:]
	}
	if version == "" {
		return base
	}
	return base + ":" + version
}
