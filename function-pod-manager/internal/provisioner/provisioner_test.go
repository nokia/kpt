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
	"os"
	"path/filepath"
	"testing"

	functionconfig "github.com/kptdev/kpt/function-pod-manager/api/functionconfig/v1alpha1"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreateEvaluatorUsesInlineTemplate(t *testing.T) {
	ctx := context.Background()
	kube := newFakeClient(t)
	created, err := newTestProvisioner(kube, false).CreateEvaluator(ctx, "ghcr.io/example/merge-netconf", "v1.2.3", nil, 0)
	require.NoError(t, err)

	require.NotEmpty(t, created.Spec.InitContainers)
	initContainer := created.Spec.InitContainers[0]
	assert.Equal(t, fnpod.ContainerInit, initContainer.Name)
	assert.Equal(t, "wrapper:test", initContainer.Image)
	assert.Equal(t, []string{"cp", "-a", "/home/nonroot/wrapper-server/.", "/wrapper-server-tools"}, initContainer.Command)
	assert.Equal(t, "wrapper-server-tools", initContainer.VolumeMounts[0].Name)

	require.NotEmpty(t, created.Spec.Containers)
	function := created.Spec.Containers[0]
	assert.Equal(t, fnpod.ContainerFunction, function.Name)
	assert.Equal(t, "ghcr.io/example/merge-netconf:v1.2.3", function.Image)
	assert.Contains(t, function.Command[0], "wrapper-server")
	assert.Contains(t, function.Args, "--port")
	assert.Contains(t, function.Args, "/fn")
	assert.Equal(t, "wrapper-server-tools", created.Spec.Volumes[0].Name)
	assert.Equal(t, "v1.2.3", created.Annotations[fnpod.AnnotationResolvedVersion])
	assert.Equal(t, "ghcr.io/example/merge-netconf", created.Annotations[fnpod.AnnotationFunctionImage])

	template := &corev1.PodTemplate{}
	require.NoError(t, kube.Get(ctx, client.ObjectKey{Namespace: "fn-test", Name: fnpod.BasePodTemplateName}, template))
	serviceTemplate := &functionconfig.ServiceTemplate{}
	require.NoError(t, kube.Get(ctx, client.ObjectKey{Namespace: "fn-test", Name: fnpod.BaseServiceTemplateName}, serviceTemplate))
	service := &corev1.Service{}
	require.NoError(t, kube.Get(ctx, client.ObjectKey{Namespace: "fn-test", Name: created.Labels[fnpod.LabelImage]}, service))
	assert.Equal(t, created.Labels[fnpod.LabelImage], service.Spec.Selector[fnpod.LabelImage])
}

func TestCreateEvaluatorAddsImagePullSecret(t *testing.T) {
	kube := newFakeClient(t)
	prov := newTestProvisioner(kube, true)
	created, err := prov.CreateEvaluator(context.Background(), "ghcr.io/example/merge-netconf", "v1.2.3", nil, 0)
	require.NoError(t, err)
	require.Len(t, created.Spec.ImagePullSecrets, 1)
	assert.Equal(t, "auth-secret", created.Spec.ImagePullSecrets[0].Name)
}

func TestCreateEvaluatorReusesTheServiceAndAppliesOverrides(t *testing.T) {
	ctx := context.Background()
	kube := newFakeClient(t)
	prov := newTestProvisioner(kube, true)
	runAs := int64(65532)
	exec := &functionconfig.PodExecutorConfig{TemplateOverrides: &functionconfig.TemplateOverrides{
		ServiceAccountName: "function",
		SecurityContext:    &corev1.PodSecurityContext{RunAsUser: &runAs},
	}}

	first, err := prov.CreateEvaluator(ctx, "ghcr.io/example/merge-netconf", "v1.2.3", exec, 1)
	require.NoError(t, err)
	second, err := prov.CreateEvaluator(ctx, "ghcr.io/example/merge-netconf", "v1.2.3", exec, 1)
	require.NoError(t, err)

	assert.Equal(t, "function", first.Spec.ServiceAccountName)
	assert.Equal(t, first.Labels[fnpod.LabelImage], second.Labels[fnpod.LabelImage])
	services := &corev1.ServiceList{}
	require.NoError(t, kube.List(ctx, services, client.InNamespace("fn-test")))
	assert.Len(t, services.Items, 1)
}

func TestCreateEvaluatorRejectsAnEmptyWrapperImage(t *testing.T) {
	prov := newTestProvisioner(newFakeClient(t), false)
	prov.cfg.WrapperServerImage = ""

	_, err := prov.CreateEvaluator(context.Background(), "ghcr.io/example/merge-netconf", "v1.2.3", nil, 0)

	require.Error(t, err)
	assert.ErrorContains(t, err, fnpod.WrapperServerImageEnv)
}

func TestCreateEvaluatorReportsRegistryAndImageErrors(t *testing.T) {
	kube := newFakeClient(t)
	prov := newTestProvisioner(kube, false)
	prov.cfg.ImageRegistry = &FakeImageRegistry{Err: assert.AnError}

	_, err := prov.CreateEvaluator(context.Background(), "ghcr.io/example/merge-netconf", "v1.2.3", nil, 0)
	require.ErrorIs(t, err, assert.AnError)

	prov.cfg.ImageRegistry = &FakeImageRegistry{Digest: "abc"}
	_, err = prov.CreateEvaluator(context.Background(), "not an image", "v1", nil, 0)
	require.Error(t, err)
	assert.ErrorContains(t, err, "parse image")
}

func TestCreateEvaluatorDoesNotAddAPullSecretForTheCatalog(t *testing.T) {
	prov := newTestProvisioner(newFakeClient(t), true)
	created, err := prov.CreateEvaluator(context.Background(), fnpod.DefaultRegistry+"/apply-replacements", "v1.0.7", nil, 0)
	require.NoError(t, err)
	assert.Empty(t, created.Spec.ImagePullSecrets)
}

func TestWarmupSkipsAConstraintAndAnExistingPod(t *testing.T) {
	ctx := context.Background()
	kube := newFakeClient(t)
	prov := newTestProvisioner(kube, false)
	require.NoError(t, prov.Warmup(ctx, nil, nil))
	require.NoError(t, prov.Warmup(ctx, &functionconfig.FunctionConfig{
		Spec: functionconfig.FunctionConfigSpec{Image: "merge-netconf", PodExecutor: &functionconfig.PodExecutorConfig{Tags: []string{"~1.0.0"}}},
	}, nil))

	cfg := &functionconfig.FunctionConfig{Spec: functionconfig.FunctionConfigSpec{
		Image:       "merge-netconf",
		PodExecutor: &functionconfig.PodExecutorConfig{Tags: []string{"v1.2.3"}},
	}}
	require.NoError(t, prov.Warmup(ctx, cfg, nil))
	pods := &corev1.PodList{}
	require.NoError(t, kube.List(ctx, pods))
	require.Len(t, pods.Items, 1)
	require.NoError(t, prov.Warmup(ctx, cfg, pods.Items))
	require.NoError(t, kube.List(ctx, pods))
	assert.Len(t, pods.Items, 1)
}

func TestCreateEvaluatorReportsATemplateWithoutAFunctionContainer(t *testing.T) {
	kube := newFakeClient(t)
	require.NoError(t, kube.Create(context.Background(), &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: fnpod.BasePodTemplateName, Namespace: "fn-test"},
		Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: fnpod.ContainerInit}},
			Containers:     []corev1.Container{{Name: "sidecar"}},
		}},
	}))

	_, err := newTestProvisioner(kube, false).CreateEvaluator(context.Background(), "ghcr.io/example/merge-netconf", "v1.2.3", nil, 0)

	require.Error(t, err)
	assert.ErrorContains(t, err, fnpod.ContainerFunction)
}

func TestEnsureRegistrySecret(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"auths":{}}`), 0o600))
	kube := newFakeClient(t)
	err := ensureImageRegistrySecret(context.Background(), kube, Config{
		Namespace:              "fn-test",
		RegistryAuthSecretName: "auth-secret",
		RegistryAuthSecretPath: path,
	})
	require.NoError(t, err)
	secret := &corev1.Secret{}
	require.NoError(t, kube.Get(context.Background(), client.ObjectKey{Namespace: "fn-test", Name: "auth-secret"}, secret))
	assert.Equal(t, corev1.SecretTypeDockerConfigJson, secret.Type)
	assert.Equal(t, []byte(`{"auths":{}}`), secret.Data[corev1.DockerConfigJsonKey])
}

func newTestProvisioner(kube client.Client, private bool) *Provisioner {
	return New(kube, Config{
		Namespace:               "fn-test",
		WrapperServerImage:      "wrapper:test",
		DefaultImagePrefix:      fnpod.DefaultImagePrefix,
		EnablePrivateRegistries: private,
		RegistryAuthSecretName:  "auth-secret",
		ImageRegistry: &FakeImageRegistry{
			Digest:     "0123456789abcdef",
			Entrypoint: []string{"/fn"},
		},
	})
}

func newFakeClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, functionconfig.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}
