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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
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

// dockerConfig is a docker config.json auth file.
type dockerConfig struct {
	Auths map[string]authn.AuthConfig `json:"auths"`
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
	if !o.cfg.EnablePrivateRegistries || strings.HasPrefix(image, fnpod.DefaultRegistry) || !o.cfg.EnablePrivateRegistriesTLS {
		return opts, nil
	}
	transport, err := o.privateTransport()
	if err != nil {
		return nil, err
	}
	opts = append(opts, remote.WithTransport(transport))
	return opts, nil
}

func (o *ociImageRegistry) authenticator(ctx context.Context, ref name.Reference, image string) (authn.Authenticator, error) {
	if !o.cfg.EnablePrivateRegistries || strings.HasPrefix(image, fnpod.DefaultRegistry) {
		auth, err := authn.DefaultKeychain.Resolve(ref.Context())
		if err != nil {
			return nil, fmt.Errorf("resolve default keychain: %w", err)
		}
		return auth, nil
	}
	if err := ensureImageRegistrySecret(ctx, o.client, o.cfg); err != nil {
		return nil, err
	}
	return authFromFile(ref, o.cfg.RegistryAuthSecretPath)
}

func (o *ociImageRegistry) privateTransport() (*http.Transport, error) {
	caPath, err := tlsCACertPath(o.cfg.TLSSecretPath)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := loadTLSConfig(caPath)
	if err != nil {
		return nil, err
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{TLSClientConfig: tlsConfig}, nil
	}
	transport := base.Clone()
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}

func authFromFile(ref name.Reference, path string) (authn.Authenticator, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read registry auth %q: %w", path, err)
	}
	var cfg dockerConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse registry auth %q: %w", path, err)
	}
	return authn.FromConfig(cfg.Auths[ref.Context().RegistryStr()]), nil
}

func ensureImageRegistrySecret(ctx context.Context, kube client.Client, cfg Config) error {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Name: cfg.RegistryAuthSecretName, Namespace: cfg.Namespace}
	err := kube.Get(ctx, key, secret)
	raw, readErr := os.ReadFile(cfg.RegistryAuthSecretPath)
	if readErr != nil {
		return fmt.Errorf("read registry auth %q: %w", cfg.RegistryAuthSecretPath, readErr)
	}
	if apierrors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: raw},
			Type:       corev1.SecretTypeDockerConfigJson,
		}
		if err := kube.Create(ctx, secret); err != nil {
			return fmt.Errorf("create registry secret %s: %w", key.Name, err)
		}
		klog.InfoS("created private registry secret", "namespace", key.Namespace, "name", key.Name)
		return nil
	}
	if err != nil {
		return err
	}
	if string(secret.Data[corev1.DockerConfigJsonKey]) == string(raw) {
		return nil
	}
	secret.Data[corev1.DockerConfigJsonKey] = raw
	if err := kube.Update(ctx, secret); err != nil {
		return fmt.Errorf("update registry secret %s: %w", key.Name, err)
	}
	return nil
}

func tlsCACertPath(dir string) (string, error) {
	if _, err := os.Stat(dir); err != nil {
		return "", fmt.Errorf("tls secret path %q: %w", dir, err)
	}
	crt := filepath.Join(dir, "ca.crt")
	if _, err := os.Stat(crt); err == nil {
		return crt, nil
	}
	pem := filepath.Join(dir, "ca.pem")
	if _, err := os.Stat(pem); err == nil {
		return pem, nil
	}
	return "", fmt.Errorf("ca.crt and ca.pem not found in %q", dir)
}

func loadTLSConfig(caCertPath string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("append certificates from %q", caCertPath)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}
