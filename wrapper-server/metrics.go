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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"k8s.io/klog/v2"
)

const (
	defaultPrometheusHost = "0.0.0.0"
	defaultPrometheusPort = "9464"
)

// startPrometheus serves /metrics when OTEL_METRICS_EXPORTER includes prometheus.
// The MeterProvider is nil when that exporter is not enabled.
func startPrometheus(ctx context.Context) (metric.MeterProvider, string, error) {
	if !prometheusEnabled() {
		return nil, "", nil
	}

	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, "", fmt.Errorf("register go collector: %w", err)
	}
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, "", fmt.Errorf("prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))

	srv := &http.Server{
		Addr:              prometheusListenAddr(),
		Handler:           promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, "", fmt.Errorf("listen %s: %w", srv.Addr, err)
	}
	go serveMetrics(ctx, srv, ln, provider)
	return provider, ln.Addr().String(), nil
}

func serveMetrics(ctx context.Context, srv *http.Server, ln net.Listener, provider *sdkmetric.MeterProvider) {
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.ErrorS(err, "prometheus metrics server stopped")
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		klog.ErrorS(err, "shutdown prometheus metrics server")
	}
	if err := provider.Shutdown(shutdownCtx); err != nil {
		klog.ErrorS(err, "shutdown meter provider")
	}
}

func prometheusEnabled() bool {
	for part := range strings.SplitSeq(os.Getenv("OTEL_METRICS_EXPORTER"), ",") {
		if strings.TrimSpace(part) == "prometheus" {
			return true
		}
	}
	return false
}

func prometheusListenAddr() string {
	host := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_PROMETHEUS_HOST"))
	if host == "" {
		host = defaultPrometheusHost
	}
	port := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_PROMETHEUS_PORT"))
	if port == "" {
		port = defaultPrometheusPort
	}
	return net.JoinHostPort(host, port)
}
