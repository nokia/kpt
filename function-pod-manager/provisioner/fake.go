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

package provisioner

import "context"

// FakeImageRegistry is an ImageRegistry used by tests.
type FakeImageRegistry struct {
	Tags       map[string][]string
	Digest     string
	Entrypoint []string
	Err        error
}

// ListTags returns the tags configured for repository.
func (f *FakeImageRegistry) ListTags(_ context.Context, repository string) ([]string, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return append([]string(nil), f.Tags[repository]...), nil
}

// Metadata returns the configured digest and entrypoint.
func (f *FakeImageRegistry) Metadata(_ context.Context, _ string) (ImageMetadata, error) {
	if f.Err != nil {
		return ImageMetadata{}, f.Err
	}
	return ImageMetadata{Digest: f.Digest, Entrypoint: append([]string(nil), f.Entrypoint...)}, nil
}
