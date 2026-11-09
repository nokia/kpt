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
	"github.com/kptdev/kpt/api/runneroptions"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// dockerConfig is a docker config.json auth file.
type dockerConfig struct {
	Auths map[string]authn.AuthConfig `json:"auths"`
}

func (o *ociImageRegistry) authenticator(ctx context.Context, ref name.Reference, image string) (authn.Authenticator, error) {
	if !o.cfg.EnablePrivateRegistries || strings.HasPrefix(image, runneroptions.GHCRImagePrefix) {
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
