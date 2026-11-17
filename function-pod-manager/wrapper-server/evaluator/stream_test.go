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

package evaluator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	streamTestImage   = "example.com/krm/sleep"
	streamTestTag     = "v1.2.3"
	streamTestExec    = "/bin/sleep"
	streamTestRequest = "fnreq-1"
)

func TestSendRequestPutsMetadataOnlyOnTheFirstChunk(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), maxChunkSize+8)
	req := testRequest(payload)
	sent := &requestCapture{}

	err := SendRequest(sent, req)

	require.NoError(t, err)
	require.Len(t, sent.msgs, 2)
	require.Equal(t, streamTestImage, sent.msgs[0].GetImage())
	require.Equal(t, streamTestTag, sent.msgs[0].GetTag())
	require.Equal(t, streamTestExec, sent.msgs[0].GetExecPath())
	require.Equal(t, streamTestRequest, sent.msgs[0].GetRequestName())
	require.Empty(t, sent.msgs[1].GetImage())
	require.Empty(t, sent.msgs[1].GetRequestName())
	require.Equal(t, payload[:maxChunkSize], sent.msgs[0].GetResourceList())
	require.Equal(t, payload[maxChunkSize:], sent.msgs[1].GetResourceList())
}

func TestSendRequestSendsOneMessageForAnEmptyResourceList(t *testing.T) {
	sent := &requestCapture{}

	err := SendRequest(sent, testRequest(nil))

	require.NoError(t, err)
	require.Len(t, sent.msgs, 1)
	require.Empty(t, sent.msgs[0].GetResourceList())
	require.Equal(t, streamTestRequest, sent.msgs[0].GetRequestName())
}

func TestRecvRequestReassemblesChunksAndKeepsTheFirstMetadata(t *testing.T) {
	payload := bytes.Repeat([]byte("b"), maxChunkSize+3)
	sent := &requestCapture{}
	require.NoError(t, SendRequest(sent, testRequest(payload)))
	sent.msgs[1].Image = "ignored"

	got, err := RecvRequest(&requestSource{msgs: sent.msgs})

	require.NoError(t, err)
	require.Equal(t, payload, got.GetResourceList())
	require.Equal(t, streamTestImage, got.GetImage())
	require.Equal(t, streamTestTag, got.GetTag())
	require.Equal(t, streamTestExec, got.GetExecPath())
	require.Equal(t, streamTestRequest, got.GetRequestName())
}

func TestRecvRequestReturnsAnEmptyRequestWhenTheStreamIsClosed(t *testing.T) {
	got, err := RecvRequest(&requestSource{})

	require.NoError(t, err)
	require.Empty(t, got.GetResourceList())
	require.Empty(t, got.GetRequestName())
}

func TestSendRequestReturnsTheStreamError(t *testing.T) {
	sent := &requestCapture{failAt: 1, err: errors.New("send closed")}

	err := SendRequest(sent, testRequest(bytes.Repeat([]byte("a"), maxChunkSize+1)))

	require.EqualError(t, err, "send closed")
	require.Len(t, sent.msgs, 1)
}

func TestRecvRequestReturnsTheStreamError(t *testing.T) {
	got, err := RecvRequest(&requestSource{err: errors.New("recv closed")})

	require.Nil(t, got)
	require.EqualError(t, err, "recv closed")
}

func TestSendResponseSplitsTheResourceListAndTheLog(t *testing.T) {
	body := bytes.Repeat([]byte("r"), maxChunkSize+2)
	log := bytes.Repeat([]byte("l"), maxChunkSize+4)
	sent := &responseCapture{}

	err := SendResponse(sent, &EvaluateFunctionResponse{ResourceList: body, Log: log})

	require.NoError(t, err)
	require.Len(t, sent.msgs, 4)
	require.Equal(t, body[:maxChunkSize], sent.msgs[0].GetResourceList())
	require.Equal(t, body[maxChunkSize:], sent.msgs[1].GetResourceList())
	require.Empty(t, sent.msgs[2].GetResourceList())
	require.Equal(t, log[:maxChunkSize], sent.msgs[2].GetLog())
	require.Equal(t, log[maxChunkSize:], sent.msgs[3].GetLog())
}

func TestSendResponseSendsOneMessageWhenTheResponseIsEmpty(t *testing.T) {
	sent := &responseCapture{}

	err := SendResponse(sent, nil)

	require.NoError(t, err)
	require.Len(t, sent.msgs, 1)
	require.Empty(t, sent.msgs[0].GetResourceList())
	require.Empty(t, sent.msgs[0].GetLog())
}

func TestSendResponseReturnsTheStreamError(t *testing.T) {
	sent := &responseCapture{failAt: 1, err: errors.New("response closed")}

	err := SendResponse(sent, &EvaluateFunctionResponse{ResourceList: []byte("body"), Log: []byte("log")})

	require.EqualError(t, err, "response closed")
	require.Len(t, sent.msgs, 1)
	require.Equal(t, []byte("body"), sent.msgs[0].GetResourceList())
}

func TestRecvResponseConcatenatesTheResourceListAndTheLog(t *testing.T) {
	body := bytes.Repeat([]byte("r"), maxChunkSize+1)
	log := bytes.Repeat([]byte("l"), maxChunkSize+1)
	sent := &responseCapture{}
	require.NoError(t, SendResponse(sent, &EvaluateFunctionResponse{ResourceList: body, Log: log}))

	got, err := RecvResponse(&responseSource{msgs: sent.msgs})

	require.NoError(t, err)
	require.Equal(t, body, got.GetResourceList())
	require.Equal(t, log, got.GetLog())
}

func TestRecvResponseReturnsAnEmptyResponseWhenTheStreamIsClosed(t *testing.T) {
	got, err := RecvResponse(&responseSource{})

	require.NoError(t, err)
	require.Empty(t, got.GetResourceList())
	require.Empty(t, got.GetLog())
}

func TestRecvResponseReturnsTheStreamError(t *testing.T) {
	got, err := RecvResponse(&responseSource{err: errors.New("response recv closed")})

	require.Nil(t, got)
	require.EqualError(t, err, "response recv closed")
}

func TestEvaluateReassemblesTheStreamedResponse(t *testing.T) {
	payload := bytes.Repeat([]byte("q"), maxChunkSize+2)
	stream := &clientStream{responses: []*EvaluateFunctionResponse{
		{ResourceList: []byte("ab")},
		{ResourceList: []byte("c"), Log: []byte("l")},
		{Log: []byte("og")},
	}}

	got, err := Evaluate(context.Background(), fakeEvaluator{stream: stream}, testRequest(payload))

	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got.GetResourceList())
	require.Equal(t, []byte("log"), got.GetLog())
	require.Len(t, stream.sent, 2)
	require.Equal(t, streamTestRequest, stream.sent[0].GetRequestName())
	require.Empty(t, stream.sent[1].GetRequestName())
	require.True(t, stream.closed)
}

func TestEvaluateReturnsAnOpenStreamError(t *testing.T) {
	got, err := Evaluate(context.Background(), fakeEvaluator{err: errors.New("unavailable")}, testRequest(nil))

	require.Nil(t, got)
	require.EqualError(t, err, "unavailable")
}

func TestEvaluateReturnsASendError(t *testing.T) {
	got, err := Evaluate(context.Background(), fakeEvaluator{stream: &clientStream{sendErr: errors.New("send failed")}}, testRequest(nil))

	require.Nil(t, got)
	require.EqualError(t, err, "send failed")
}

func TestEvaluateReturnsACloseError(t *testing.T) {
	got, err := Evaluate(context.Background(), fakeEvaluator{stream: &clientStream{closeErr: errors.New("close failed")}}, testRequest(nil))

	require.Nil(t, got)
	require.EqualError(t, err, "close failed")
}

func TestStreamingServerEvaluatesTheReassembledRequest(t *testing.T) {
	payload := []byte("resources")
	sent := &requestCapture{}
	require.NoError(t, SendRequest(sent, testRequest(payload)))
	ctx := context.WithValue(context.Background(), streamCtxKey{}, "pod")
	stream := &serverStream{ctx: ctx, incoming: sent.msgs}
	srv := NewStreamingServer(func(gotCtx context.Context, req *EvaluateFunctionRequest) (*EvaluateFunctionResponse, error) {
		require.Equal(t, "pod", gotCtx.Value(streamCtxKey{}))
		require.Equal(t, payload, req.GetResourceList())
		require.Equal(t, streamTestRequest, req.GetRequestName())
		return &EvaluateFunctionResponse{ResourceList: []byte("out"), Log: []byte("done")}, nil
	})

	err := srv.EvaluateFunction(stream)

	require.NoError(t, err)
	require.Len(t, stream.sent, 2)
	require.Equal(t, []byte("out"), stream.sent[0].GetResourceList())
	require.Equal(t, []byte("done"), stream.sent[1].GetLog())
}

func TestStreamingServerReturnsAReceiveError(t *testing.T) {
	srv := NewStreamingServer(func(context.Context, *EvaluateFunctionRequest) (*EvaluateFunctionResponse, error) {
		return &EvaluateFunctionResponse{}, nil
	})

	err := srv.EvaluateFunction(&serverStream{recvErr: errors.New("client gone")})

	require.EqualError(t, err, "client gone")
}

func TestStreamingServerReturnsTheFunctionError(t *testing.T) {
	srv := NewStreamingServer(func(context.Context, *EvaluateFunctionRequest) (*EvaluateFunctionResponse, error) {
		return nil, errors.New("function failed")
	})

	err := srv.EvaluateFunction(&serverStream{incoming: []*EvaluateFunctionRequest{{}}})

	require.EqualError(t, err, "function failed")
}

func TestStreamingServerReturnsASendError(t *testing.T) {
	srv := NewStreamingServer(func(context.Context, *EvaluateFunctionRequest) (*EvaluateFunctionResponse, error) {
		return &EvaluateFunctionResponse{ResourceList: []byte("out")}, nil
	})

	err := srv.EvaluateFunction(&serverStream{
		incoming: []*EvaluateFunctionRequest{{}},
		sendErr:  errors.New("client closed"),
	})

	require.EqualError(t, err, "client closed")
}

func testRequest(payload []byte) *EvaluateFunctionRequest {
	return &EvaluateFunctionRequest{
		ResourceList: payload,
		Image:        streamTestImage,
		Tag:          streamTestTag,
		ExecPath:     streamTestExec,
		RequestName:  streamTestRequest,
	}
}

type streamCtxKey struct{}

type requestCapture struct {
	msgs   []*EvaluateFunctionRequest
	failAt int
	err    error
}

func (c *requestCapture) Send(req *EvaluateFunctionRequest) error {
	if c.err != nil && len(c.msgs) == c.failAt {
		return c.err
	}
	c.msgs = append(c.msgs, req)
	return nil
}

type requestSource struct {
	msgs []*EvaluateFunctionRequest
	i    int
	err  error
}

func (s *requestSource) Recv() (*EvaluateFunctionRequest, error) {
	if s.err != nil && s.i >= len(s.msgs) {
		return nil, s.err
	}
	if s.i >= len(s.msgs) {
		return nil, io.EOF
	}
	msg := s.msgs[s.i]
	s.i++
	return msg, nil
}

type responseCapture struct {
	msgs   []*EvaluateFunctionResponse
	failAt int
	err    error
}

func (c *responseCapture) Send(resp *EvaluateFunctionResponse) error {
	if c.err != nil && len(c.msgs) == c.failAt {
		return c.err
	}
	c.msgs = append(c.msgs, resp)
	return nil
}

type responseSource struct {
	msgs []*EvaluateFunctionResponse
	i    int
	err  error
}

func (s *responseSource) Recv() (*EvaluateFunctionResponse, error) {
	if s.err != nil && s.i >= len(s.msgs) {
		return nil, s.err
	}
	if s.i >= len(s.msgs) {
		return nil, io.EOF
	}
	msg := s.msgs[s.i]
	s.i++
	return msg, nil
}

type fakeEvaluator struct {
	stream grpc.BidiStreamingClient[EvaluateFunctionRequest, EvaluateFunctionResponse]
	err    error
}

func (f fakeEvaluator) EvaluateFunction(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[EvaluateFunctionRequest, EvaluateFunctionResponse], error) {
	return f.stream, f.err
}

type clientStream struct {
	sent      []*EvaluateFunctionRequest
	responses []*EvaluateFunctionResponse
	i         int
	sendErr   error
	closeErr  error
	closed    bool
}

func (s *clientStream) Send(req *EvaluateFunctionRequest) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, req)
	return nil
}

func (s *clientStream) Recv() (*EvaluateFunctionResponse, error) {
	if s.i >= len(s.responses) {
		return nil, io.EOF
	}
	msg := s.responses[s.i]
	s.i++
	return msg, nil
}

func (s *clientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *clientStream) Trailer() metadata.MD         { return nil }
func (s *clientStream) CloseSend() error {
	s.closed = true
	return s.closeErr
}
func (s *clientStream) Context() context.Context { return context.Background() }
func (s *clientStream) SendMsg(any) error        { return nil }
func (s *clientStream) RecvMsg(any) error        { return io.EOF }

type serverStream struct {
	ctx      context.Context
	incoming []*EvaluateFunctionRequest
	i        int
	recvErr  error
	sent     []*EvaluateFunctionResponse
	sendErr  error
}

func (s *serverStream) Recv() (*EvaluateFunctionRequest, error) {
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	if s.i >= len(s.incoming) {
		return nil, io.EOF
	}
	msg := s.incoming[s.i]
	s.i++
	return msg, nil
}

func (s *serverStream) Send(resp *EvaluateFunctionResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, resp)
	return nil
}

func (s *serverStream) SetHeader(metadata.MD) error  { return nil }
func (s *serverStream) SendHeader(metadata.MD) error { return nil }
func (s *serverStream) SetTrailer(metadata.MD)       {}
func (s *serverStream) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}
func (s *serverStream) SendMsg(any) error { return nil }
func (s *serverStream) RecvMsg(any) error { return io.EOF }
