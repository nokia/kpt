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

// Package provisioner creates Function Evaluator pods and Services from porch templates.
package provisioner

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/kptdev/kpt/api/runneroptions"
	"github.com/kptdev/kpt/api/utils/image"
	"github.com/kptdev/kpt/function-pod-manager/internal/fnpod"
	"github.com/kptdev/kpt/function-pod-manager/internal/provisioner/util"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Config is the pod-lifecycle configuration relocated from porch-server.
type Config struct {
	Namespace                  string
	WrapperServerImage         string
	DefaultImagePrefix         string
	EnablePrivateRegistries    bool
	RegistryAuthSecretPath     string
	RegistryAuthSecretName     string
	EnablePrivateRegistriesTLS bool
	TLSSecretPath              string
	ImageRegistry              ImageRegistry
}

// Provisioner creates evaluator pods and Services.
type Provisioner struct {
	client client.Client
	cfg    Config
}

// New returns a provisioner. A nil ImageRegistry talks to the OCI image registry.
func New(kube client.Client, cfg Config) *Provisioner {
	if cfg.ImageRegistry == nil {
		cfg.ImageRegistry = &ociImageRegistry{client: kube, cfg: cfg}
	}
	return &Provisioner{client: kube, cfg: cfg}
}

// ListTags lists tags for a repository that has already had the default prefix applied.
func (p *Provisioner) ListTags(ctx context.Context, repository string) ([]string, error) {
	return p.cfg.ImageRegistry.ListTags(ctx, repository)
}

// CreateEvaluator creates a Function Evaluator pod and its Service for repository:version.
func (p *Provisioner) CreateEvaluator(ctx context.Context, repository, version string, exec *functionconfig.PodExecutorConfig, postfix int) (*corev1.Pod, error) {
	if p.cfg.WrapperServerImage == "" {
		return nil, fmt.Errorf("wrapper server image is empty; set %s", fnpod.WrapperServerImageEnv)
	}
	image := repository + ":" + version
	meta, err := p.cfg.ImageRegistry.Metadata(ctx, image)
	if err != nil {
		return nil, err
	}
	podID, err := podID(image, meta.Digest, strconv.Itoa(postfix))
	if err != nil {
		return nil, err
	}
	template, err := p.basePodTemplate(ctx)
	if err != nil {
		return nil, err
	}
	spec := template.DeepCopy().Template
	if len(spec.Spec.InitContainers) == 0 {
		return nil, fmt.Errorf("pod template %s has no init containers", fnpod.BasePodTemplateName)
	}
	spec.Spec.InitContainers[0].Image = p.cfg.WrapperServerImage
	p.appendImagePullSecret(image, &spec)
	if err := util.PatchFunctionContainer(&spec, meta.Entrypoint, image); err != nil {
		return nil, err
	}
	if exec != nil {
		util.MergeTemplateOverrides(&spec, exec.TemplateOverrides)
	}
	patchPodIdentity(&spec, p.cfg.Namespace, podID, template.ResourceVersion, repository, version)
	spec.GenerateName = podID + "-"
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: spec.ObjectMeta,
		Spec:       spec.Spec,
	}
	if err := p.client.Create(ctx, pod); err != nil {
		return nil, fmt.Errorf("create evaluator pod for %s: %w", image, err)
	}
	if _, err := p.ensureService(ctx, podID); err != nil {
		return nil, err
	}
	klog.InfoS("created function evaluator pod", "namespace", pod.Namespace, "name", pod.Name, "image", image)
	return pod, nil
}

// Warmup ensures one pod when the first podExecutor tag is a concrete version.
func (p *Provisioner) Warmup(ctx context.Context, cfg *functionconfig.FunctionConfig, existing []corev1.Pod) error {
	if cfg == nil || cfg.Spec.PodExecutor == nil || len(cfg.Spec.PodExecutor.Tags) == 0 {
		return nil
	}
	tag := cfg.Spec.PodExecutor.Tags[0]
	if !util.ConcreteVersion(tag) {
		klog.V(3).InfoS("skipping warmup for a semver constraint", "image", cfg.Spec.Image, "tag", tag)
		return nil
	}
	repository := image.Parse(image.WithPrefix(cfg.Spec.Image, p.cfg.DefaultImagePrefix)).Repository()
	for i := range existing {
		if existing[i].Annotations[fnpod.AnnotationFunctionImage] == repository &&
			existing[i].Annotations[fnpod.AnnotationResolvedVersion] == tag {
			return nil
		}
	}
	_, err := p.CreateEvaluator(ctx, repository, tag, cfg.Spec.PodExecutor, len(existing))
	return err
}

func (p *Provisioner) basePodTemplate(ctx context.Context) (*corev1.PodTemplate, error) {
	template := &corev1.PodTemplate{}
	key := client.ObjectKey{Namespace: p.cfg.Namespace, Name: fnpod.BasePodTemplateName}
	err := p.client.Get(ctx, key, template)
	if err == nil {
		return template, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	template = util.InlinePodTemplate(p.cfg.Namespace, p.cfg.WrapperServerImage)
	if err := p.client.Create(ctx, template); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create pod template %s: %w", fnpod.BasePodTemplateName, err)
		}
		if err := p.client.Get(ctx, key, template); err != nil {
			return nil, err
		}
	}
	return template, nil
}

func (p *Provisioner) ensureService(ctx context.Context, serviceName string) (*corev1.Service, error) {
	existing := &corev1.Service{}
	key := client.ObjectKey{Namespace: p.cfg.Namespace, Name: serviceName}
	err := p.client.Get(ctx, key, existing)
	if err == nil {
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	template, err := p.baseServiceTemplate(ctx)
	if err != nil {
		return nil, err
	}
	spec := template.Template
	if spec.Labels == nil {
		spec.Labels = map[string]string{}
	}
	spec.Labels["app.kubernetes.io/name"] = serviceName
	if spec.Spec.Selector == nil {
		spec.Spec.Selector = map[string]string{}
	}
	spec.Spec.Selector[fnpod.LabelImage] = serviceName
	service := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: p.cfg.Namespace, Labels: spec.Labels, Annotations: spec.Annotations},
		Spec:       spec.Spec,
	}
	if err := p.client.Create(ctx, service, client.FieldOwner(fnpod.FieldManager)); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err := p.client.Get(ctx, key, existing); err != nil {
				return nil, err
			}
			return existing, nil
		}
		return nil, fmt.Errorf("create service %s: %w", serviceName, err)
	}
	return service, nil
}

func (p *Provisioner) baseServiceTemplate(ctx context.Context) (*functionconfig.ServiceTemplate, error) {
	template := &functionconfig.ServiceTemplate{}
	key := client.ObjectKey{Namespace: p.cfg.Namespace, Name: fnpod.BaseServiceTemplateName}
	err := p.client.Get(ctx, key, template)
	if err == nil {
		return template, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	template = util.InlineServiceTemplate(p.cfg.Namespace)
	if err := p.client.Create(ctx, template); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create service template %s: %w", fnpod.BaseServiceTemplateName, err)
		}
		if err := p.client.Get(ctx, key, template); err != nil {
			return nil, err
		}
	}
	return template, nil
}

func (p *Provisioner) appendImagePullSecret(image string, pod *corev1.PodTemplateSpec) {
	if !p.cfg.EnablePrivateRegistries || strings.HasPrefix(image, runneroptions.GHCRImagePrefix) {
		return
	}
	for _, secret := range pod.Spec.ImagePullSecrets {
		if secret.Name == p.cfg.RegistryAuthSecretName {
			return
		}
	}
	pod.Spec.ImagePullSecrets = append(pod.Spec.ImagePullSecrets, corev1.LocalObjectReference{
		Name: p.cfg.RegistryAuthSecretName,
	})
}

func patchPodIdentity(pod *corev1.PodTemplateSpec, namespace, podID, templateVersion, repository, version string) {
	pod.Namespace = namespace
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[fnpod.AnnotationTemplateVersion] = templateVersion
	pod.Annotations[fnpod.AnnotationFunctionImage] = repository
	pod.Annotations[fnpod.AnnotationResolvedVersion] = version
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[fnpod.LabelImage] = podID
}

func podID(image, digest, postfix string) (string, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return "", fmt.Errorf("parse image %q: %w", image, err)
	}
	parts := strings.Split(ref.Context().Name(), "/")
	base := strings.ReplaceAll(parts[len(parts)-1], "_", "-")
	tag := strings.ReplaceAll(ref.Identifier(), ".", "")
	short := digest
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("%s-%s-%s-%s", base, tag, postfix, short), nil
}
