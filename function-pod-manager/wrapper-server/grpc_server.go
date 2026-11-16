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
	"fmt"
	"net"

	pb "github.com/kptdev/kpt/function-pod-manager/wrapper-server/evaluator"
	"github.com/kptdev/kpt/function-pod-manager/wrapper-server/healthchecker"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ServeGRPC listens on addr and serves FunctionEvaluator and gRPC health.
// meter records rpc.server.* metrics; nil disables that instrumentation.
func ServeGRPC(ctx context.Context, addr string, claims *ClaimController, unary *SingleFunctionEvaluator, meter metric.MeterProvider) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	srv := grpc.NewServer(grpcServerOptions(meter)...)
	pb.RegisterFunctionEvaluatorServer(srv, newClaimAwareServer(claims, unary))
	healthpb.RegisterHealthServer(srv, healthchecker.NewHealthChecker())

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

func grpcServerOptions(meter metric.MeterProvider) []grpc.ServerOption {
	if meter == nil {
		return nil
	}
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(meter))),
	}
}

type claimAwareServer struct {
	pb.UnimplementedFunctionEvaluatorServer
	claims   *ClaimController
	delegate pb.UnaryEvaluator
}

func newClaimAwareServer(claims *ClaimController, unary *SingleFunctionEvaluator) pb.FunctionEvaluatorServer {
	return &claimAwareServer{claims: claims, delegate: unary}
}

func (s *claimAwareServer) EvaluateFunction(stream grpc.BidiStreamingServer[pb.EvaluateFunctionRequest, pb.EvaluateFunctionResponse]) error {
	req, err := pb.RecvRequest(stream)
	if err != nil {
		return err
	}
	if err := s.claims.ValidateEvalRequest(stream.Context(), req.GetRequestName()); err != nil {
		return err
	}
	defer s.claims.OnEvaluateEnd(req.GetRequestName())

	resp, err := s.delegate.EvaluateFunction(stream.Context(), req)
	if err != nil {
		return err
	}
	return pb.SendResponse(stream, resp)
}
