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

// Package util holds version and pod-template helpers for the provisioner.
package util

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// ErrNoMatchingTag means no published tag satisfies the constraint.
var ErrNoMatchingTag = errors.New("no published tag satisfies the constraint")

// VersionSatisfies reports whether a concrete pod version satisfies a request constraint.
// An empty constraint matches every version.
func VersionSatisfies(version, constraint string) bool {
	if constraint == "" {
		return true
	}
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return false
	}
	parsed, err := semver.NewVersion(version)
	if err != nil {
		return false
	}
	return c.Check(parsed)
}

// ConcreteVersion reports whether tag is a single semver version rather than a range.
func ConcreteVersion(tag string) bool {
	_, err := semver.NewVersion(tag)
	return err == nil
}

// SelectTag picks the highest v-prefixed tag that satisfies constraint.
// This follows porch filterByConstraint.
func SelectTag(tags []string, constraint string) (string, error) {
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return "", fmt.Errorf("tag %q is not a valid semver constraint: %w", constraint, err)
	}
	type candidate struct {
		tag     string
		version *semver.Version
	}
	var matches []candidate
	for _, tag := range tags {
		if !strings.HasPrefix(tag, "v") {
			continue
		}
		parsed, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}
		if c.Check(parsed) {
			matches = append(matches, candidate{tag: tag, version: parsed})
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("%w: %s", ErrNoMatchingTag, constraint)
	}
	slices.SortFunc(matches, func(a, b candidate) int {
		return a.version.Compare(b.version)
	})
	return matches[len(matches)-1].tag, nil
}

// HighestVersion returns the highest semver string in versions.
func HighestVersion(versions []string) string {
	var best string
	var bestParsed *semver.Version
	for _, version := range versions {
		parsed, err := semver.NewVersion(version)
		if err != nil {
			continue
		}
		if bestParsed == nil || parsed.Compare(bestParsed) > 0 {
			best = version
			bestParsed = parsed
		}
	}
	return best
}

// TagsMatch reports whether version matches a FunctionConfig podExecutor tag list.
// An empty list does not match. "*" matches every version.
func TagsMatch(version string, tags []string) bool {
	if len(tags) == 0 {
		return false
	}
	if version == "" || slices.Contains(tags, "*") {
		return true
	}
	parsed, err := semver.NewVersion(version)
	if err != nil {
		return slices.Contains(tags, version)
	}
	for _, tag := range tags {
		c, err := semver.NewConstraint(tag)
		if err != nil {
			continue
		}
		if c.Check(parsed) {
			return true
		}
	}
	return false
}
