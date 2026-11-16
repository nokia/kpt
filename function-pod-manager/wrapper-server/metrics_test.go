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

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kptdev/kpt/function-pod-manager/wrapper-server/healthchecker"
)

func TestPrometheusDisabledWithoutExporter(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "")

	meter, addr, err := startPrometheus(context.Background())

	require.NoError(t, err)
	require.Nil(t, meter)
	require.Empty(t, addr)
}

func TestPrometheusEndpointExposesRuntimeAndRPCMetrics(t *testing.T) {
	port := freeTCPPort(t)
	t.Setenv("OTEL_METRICS_EXPORTER", "prometheus")
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_HOST", "127.0.0.1")
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_PORT", strconv.Itoa(port))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	meter, addr, err := startPrometheus(ctx)
	require.NoError(t, err)
	require.NotNil(t, meter)

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(meter))))
	healthpb.RegisterHealthServer(srv, healthchecker.NewHealthChecker())
	go func() {
		_ = srv.Serve(grpcLis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		body, err := scrape(addr)
		if err != nil {
			return false
		}
		return strings.Contains(body, "go_") && strings.Contains(body, "rpc_server_")
	}, 5*time.Second, 50*time.Millisecond)
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

func scrape(addr string) (string, error) {
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
