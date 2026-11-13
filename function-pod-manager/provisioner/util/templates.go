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

package util

import (
	"fmt"
	"net"
	"path/filepath"

	"github.com/kptdev/kpt/function-pod-manager/fnpod"
	functionconfig "github.com/kptdev/porch/api/porchconfig/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	wrapperServerPort int32 = 9446
	volumeName              = "wrapper-server-tools"
	volumeMountPath         = "/wrapper-server-tools"
	wrapperServerBin        = "wrapper-server"
	grpcProbeBin            = "grpc-health-probe"
	gomemlimitEnvVar        = "GOMEMLIMIT"
)

func InlinePodTemplate(namespace, wrapperImage string) *corev1.PodTemplate {
	probeAddr := net.JoinHostPort("localhost", fmt.Sprint(wrapperServerPort))
	return &corev1.PodTemplate{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodTemplate"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fnpod.BasePodTemplateName,
			Namespace: namespace,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					"cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
				},
			},
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{
					Name:    fnpod.ContainerInit,
					Image:   wrapperImage,
					Command: []string{"cp", "-r", "/home/nonroot/wrapper-server/.", volumeMountPath},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      volumeName,
						MountPath: volumeMountPath,
					}},
				}},
				Containers: []corev1.Container{{
					Name:    fnpod.ContainerFunction,
					Image:   "to-be-replaced",
					Command: []string{filepath.Join(volumeMountPath, wrapperServerBin)},
					Env: []corev1.EnvVar{
						{Name: "OTEL_METRICS_EXPORTER", Value: "prometheus"},
						{Name: "OTEL_TRACES_EXPORTER", Value: "none"},
						{Name: "OTEL_EXPORTER_PROMETHEUS_HOST", Value: "0.0.0.0"},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{
									filepath.Join(volumeMountPath, grpcProbeBin),
									"-addr", probeAddr,
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      volumeName,
						MountPath: volumeMountPath,
					}},
				}},
				Volumes: []corev1.Volume{{
					Name: volumeName,
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				}},
			},
		},
	}
}

func InlineServiceTemplate(namespace string) *functionconfig.ServiceTemplate {
	return &functionconfig.ServiceTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: functionconfig.GroupVersion.String(),
			Kind:       "ServiceTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fnpod.BaseServiceTemplateName,
			Namespace: namespace,
		},
		Template: functionconfig.ServiceTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					"internal.kpt.dev/upstream-identifier": "|Service|porch-system|function-runner",
				},
				Labels: map[string]string{
					"app.kubernetes.io/name": "to-be-replaced",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					fnpod.LabelImage: "to-be-replaced",
				},
				Ports: []corev1.ServicePort{{
					Protocol:   corev1.ProtocolTCP,
					Port:       wrapperServerPort,
					TargetPort: intstr.FromInt32(wrapperServerPort),
				}},
				Type: corev1.ServiceTypeClusterIP,
			},
		},
	}
}
