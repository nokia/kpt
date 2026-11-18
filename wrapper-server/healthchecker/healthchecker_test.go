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

package healthchecker

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCheckReportsTheServerIsServing(t *testing.T) {
	checker := NewHealthChecker()

	resp, err := checker.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: "evaluator"})

	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestWatchSendsAServingStatus(t *testing.T) {
	checker := NewHealthChecker()
	stream := &watchStream{}

	err := checker.Watch(&grpc_health_v1.HealthCheckRequest{Service: "evaluator"}, stream)

	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, stream.sent.GetStatus())
}

func TestWatchReturnsTheStreamSendError(t *testing.T) {
	checker := NewHealthChecker()
	stream := &watchStream{err: errors.New("client closed")}

	err := checker.Watch(&grpc_health_v1.HealthCheckRequest{}, stream)

	assert.EqualError(t, err, "client closed")
}

func TestListIsNotImplemented(t *testing.T) {
	checker := NewHealthChecker()

	resp, err := checker.List(context.Background(), &grpc_health_v1.HealthListRequest{})

	assert.Nil(t, resp)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
	assert.ErrorContains(t, err, "List is not implemented")
}

type watchStream struct {
	sent *grpc_health_v1.HealthCheckResponse
	err  error
}

func (s *watchStream) Send(resp *grpc_health_v1.HealthCheckResponse) error {
	if s.err != nil {
		return s.err
	}
	s.sent = resp
	return nil
}

func (s *watchStream) SetHeader(metadata.MD) error  { return nil }
func (s *watchStream) SendHeader(metadata.MD) error { return nil }
func (s *watchStream) SetTrailer(metadata.MD)       {}
func (s *watchStream) Context() context.Context     { return context.Background() }
func (s *watchStream) SendMsg(any) error            { return nil }
func (s *watchStream) RecvMsg(any) error            { return io.EOF }
