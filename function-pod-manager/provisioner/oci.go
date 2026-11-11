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

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/kptdev/kpt/api/runneroptions"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ImageMetadata is the digest and entrypoint of a function image.
type ImageMetadata struct {
	Digest     string
	Entrypoint []string
}

// ImageRegistry lists tags and reads metadata from an OCI image registry.
type ImageRegistry interface {
	ListTags(ctx context.Context, repository string) ([]string, error)
	Metadata(ctx context.Context, image string) (ImageMetadata, error)
}

type ociImageRegistry struct {
	client client.Client
	cfg    Config
}

func (o *ociImageRegistry) ListTags(ctx context.Context, repository string) ([]string, error) {
	repo, err := name.NewRepository(repository)
	if err != nil {
		return nil, fmt.Errorf("parse repository %q: %w", repository, err)
	}
	opts, err := o.remoteOptions(ctx, repository)
	if err != nil {
		return nil, err
	}
	tags, err := remote.List(repo, opts...)
	if err != nil {
		return nil, fmt.Errorf("list tags for %q: %w", repository, err)
	}
	return tags, nil
}

func (o *ociImageRegistry) Metadata(ctx context.Context, image string) (ImageMetadata, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return ImageMetadata{}, fmt.Errorf("parse image %q: %w", image, err)
	}
	opts, err := o.remoteOptions(ctx, image)
	if err != nil {
		return ImageMetadata{}, err
	}
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return ImageMetadata{}, fmt.Errorf("pull image metadata %q: %w", image, err)
	}
	return metadataFromImage(img)
}

func metadataFromImage(img containerregistry.Image) (ImageMetadata, error) {
	hash, err := img.Digest()
	if err != nil {
		return ImageMetadata{}, err
	}
	configFile, err := img.ConfigFile()
	if err != nil {
		return ImageMetadata{}, err
	}
	entrypoint := configFile.Config.Entrypoint
	if len(entrypoint) == 0 {
		entrypoint = configFile.Config.Cmd
	}
	return ImageMetadata{Digest: hash.Hex, Entrypoint: entrypoint}, nil
}

func (o *ociImageRegistry) remoteOptions(ctx context.Context, image string) ([]remote.Option, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return nil, err
	}
	auth, err := o.authenticator(ctx, ref, image)
	if err != nil {
		return nil, err
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithAuth(auth)}
	if !o.cfg.EnablePrivateRegistries || strings.HasPrefix(image, runneroptions.GHCRImagePrefix) || !o.cfg.EnablePrivateRegistriesTLS {
		return opts, nil
	}
	transport, err := o.privateTransport()
	if err != nil {
		return nil, err
	}
	opts = append(opts, remote.WithTransport(transport))
	return opts, nil
}
