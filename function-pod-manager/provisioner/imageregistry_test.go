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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/kptdev/kpt/api/runneroptions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestMetadataFromImageUsesEntrypointThenCommand(t *testing.T) {
	withEntry, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{
		Config: v1.Config{Entrypoint: []string{"/fn"}, Cmd: []string{"ignored"}},
	})
	require.NoError(t, err)
	meta, err := metadataFromImage(withEntry)
	require.NoError(t, err)
	assert.NotEmpty(t, meta.Digest)
	assert.Equal(t, []string{"/fn"}, meta.Entrypoint)

	withCmd, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{
		Config: v1.Config{Cmd: []string{"/cmd"}},
	})
	require.NoError(t, err)
	meta, err = metadataFromImage(withCmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"/cmd"}, meta.Entrypoint)
}

func TestRemoteOptionsSkipPrivateTLSForThePublicCatalog(t *testing.T) {
	registry := &ociImageRegistry{cfg: Config{
		EnablePrivateRegistries:    true,
		EnablePrivateRegistriesTLS: true,
		TLSSecretPath:              t.TempDir(),
	}}

	opts, err := registry.remoteOptions(context.Background(), runneroptions.GHCRImagePrefix+"/apply-replacements:v1.0.7")

	require.NoError(t, err)
	assert.NotEmpty(t, opts)
}

func TestRemoteOptionsUsePrivateTLSWhenTheCAIsPresent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), testCAPEM(t), 0o600))
	authPath := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(authPath, []byte(`{"auths":{"example.com":{"username":"user","password":"pass"}}}`), 0o600))
	registry := &ociImageRegistry{
		client: newFakeClient(t),
		cfg: Config{
			Namespace:                  "fn-test",
			EnablePrivateRegistries:    true,
			EnablePrivateRegistriesTLS: true,
			RegistryAuthSecretName:     "auth-secret",
			RegistryAuthSecretPath:     authPath,
			TLSSecretPath:              dir,
		},
	}

	opts, err := registry.remoteOptions(context.Background(), "example.com/functions/apply-replacements:v1.0.7")

	require.NoError(t, err)
	assert.NotEmpty(t, opts)
	secret := &corev1.Secret{}
	require.NoError(t, registry.client.Get(context.Background(), client.ObjectKey{Namespace: "fn-test", Name: "auth-secret"}, secret))
	assert.Equal(t, corev1.SecretTypeDockerConfigJson, secret.Type)
}

func TestRemoteOptionsFailWhenThePrivateCAIsMissing(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(authPath, []byte(`{"auths":{}}`), 0o600))
	registry := &ociImageRegistry{
		client: newFakeClient(t),
		cfg: Config{
			Namespace:                  "fn-test",
			EnablePrivateRegistries:    true,
			EnablePrivateRegistriesTLS: true,
			RegistryAuthSecretName:     "auth-secret",
			RegistryAuthSecretPath:     authPath,
			TLSSecretPath:              dir,
		},
	}

	_, err := registry.ListTags(context.Background(), "example.com/functions/apply-replacements")

	require.Error(t, err)
	assert.ErrorContains(t, err, "ca.crt and ca.pem")
}

func TestListTagsAndMetadataRejectAnUnparseableImage(t *testing.T) {
	registry := &ociImageRegistry{cfg: Config{}}

	_, err := registry.ListTags(context.Background(), "not a repository")
	require.Error(t, err)
	assert.ErrorContains(t, err, "parse repository")

	_, err = registry.Metadata(context.Background(), "not an image")
	require.Error(t, err)
	assert.ErrorContains(t, err, "parse image")
}

func TestAuthFromFileReadsTheRegistryEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"auths":{"example.com":{"username":"user","password":"pass"}}}`), 0o600))
	ref, err := name.ParseReference("example.com/functions/apply-replacements:v1")
	require.NoError(t, err)

	auth, err := authFromFile(ref, path)

	require.NoError(t, err)
	cfg, err := auth.Authorization()
	require.NoError(t, err)
	assert.Equal(t, "user", cfg.Username)

	_, err = authFromFile(ref, filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "read registry auth")

	bad := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))
	_, err = authFromFile(ref, bad)
	require.Error(t, err)
	assert.ErrorContains(t, err, "parse registry auth")
}

func TestEnsureRegistrySecretUpdatesChangedAuth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"auths":{"a":{}}}`), 0o600))
	kube := newFakeClient(t)
	cfg := Config{Namespace: "fn-test", RegistryAuthSecretName: "auth-secret", RegistryAuthSecretPath: path}
	require.NoError(t, ensureImageRegistrySecret(context.Background(), kube, cfg))
	require.NoError(t, ensureImageRegistrySecret(context.Background(), kube, cfg))

	require.NoError(t, os.WriteFile(path, []byte(`{"auths":{"b":{}}}`), 0o600))
	require.NoError(t, ensureImageRegistrySecret(context.Background(), kube, cfg))

	secret := &corev1.Secret{}
	require.NoError(t, kube.Get(context.Background(), client.ObjectKey{Namespace: "fn-test", Name: "auth-secret"}, secret))
	assert.Equal(t, []byte(`{"auths":{"b":{}}}`), secret.Data[corev1.DockerConfigJsonKey])
}

func TestEnsureRegistrySecretReportsAMissingAuthFile(t *testing.T) {
	err := ensureImageRegistrySecret(context.Background(), newFakeClient(t), Config{
		Namespace:              "fn-test",
		RegistryAuthSecretName: "auth-secret",
		RegistryAuthSecretPath: filepath.Join(t.TempDir(), "missing"),
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "read registry auth")
}

func TestEnsureRegistrySecretReturnsAClientGetError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"auths":{}}`), 0o600))
	kube := interceptor.NewClient(watchingClient(t), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return assert.AnError
		},
	})

	err := ensureImageRegistrySecret(context.Background(), kube, Config{
		Namespace:              "fn-test",
		RegistryAuthSecretName: "auth-secret",
		RegistryAuthSecretPath: path,
	})

	require.ErrorIs(t, err, assert.AnError)
}

func TestTLSCACertPathAcceptsEitherFileName(t *testing.T) {
	crtDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(crtDir, "ca.crt"), []byte("crt"), 0o600))
	got, err := tlsCACertPath(crtDir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(crtDir, "ca.crt"), got)

	pemDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(pemDir, "ca.pem"), []byte("pem"), 0o600))
	got, err = tlsCACertPath(pemDir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(pemDir, "ca.pem"), got)

	_, err = tlsCACertPath(t.TempDir())
	require.Error(t, err)
	assert.ErrorContains(t, err, "not found")

	_, err = tlsCACertPath(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "tls secret path")
}

func TestLoadTLSConfigRejectsAFileThatIsNotACertificate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))

	_, err := loadTLSConfig(path)

	require.Error(t, err)
	assert.ErrorContains(t, err, "append certificates")

	cfg, err := loadTLSConfig(writePEM(t, testCAPEM(t)))
	require.NoError(t, err)
	assert.NotNil(t, cfg.RootCAs)
}

func testCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func watchingClient(t *testing.T) client.WithWatch {
	t.Helper()
	watching, ok := newFakeClient(t).(client.WithWatch)
	require.True(t, ok)
	return watching
}

func writePEM(t *testing.T, raw []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}
