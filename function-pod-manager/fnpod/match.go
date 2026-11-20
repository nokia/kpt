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
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/name"
)

// ImageBaseName is the final path segment of an image reference (repository or repository:tag).
func ImageBaseName(imageRef string) string {
	ref := strings.TrimSpace(imageRef)
	if ref == "" {
		return ""
	}
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if n, err := name.ParseReference(ref, name.WeakValidation); err == nil {
		parts := strings.Split(n.Context().Name(), "/")
		return parts[len(parts)-1]
	}
	parts := strings.Split(ref, "/")
	last := parts[len(parts)-1]
	if i := strings.LastIndex(last, ":"); i >= 0 {
		last = last[:i]
	}
	return last
}

// imageTag returns an explicitly specified tag (for example from :latest), not an implicit default.
func imageTag(imageRef string) string {
	ref := strings.TrimSpace(imageRef)
	if ref == "" {
		return ""
	}
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	slash := strings.LastIndex(ref, "/")
	colon := strings.LastIndex(ref, ":")
	if colon <= slash {
		return ""
	}
	return strings.TrimSpace(ref[colon+1:])
}

// GetVersionConstraint is the constraint used for matching and provisioning.
// storedConstraint is spec.versionConstraint (Kptfile Function.tag when set by porch).
// When it is empty, a tag on imageRef (for example :latest) becomes the constraint.
func GetVersionConstraint(storedConstraint, imageRef string) string {
	if t := strings.TrimSpace(storedConstraint); t != "" {
		return t
	}
	return imageTag(imageRef)
}

// VersionSatisfies reports whether concreteVersion satisfies the request constraint.
// An empty constraint never matches.
func VersionSatisfies(concreteVersion, constraint string) bool {
	if constraint == "" {
		return false
	}
	if concreteVersion == constraint {
		return true
	}
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return false
	}
	parsed, err := semver.NewVersion(concreteVersion)
	if err != nil {
		return false
	}
	return c.Check(parsed)
}
