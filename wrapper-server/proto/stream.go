// Copyright 2026 The kpt and Nephio Authors
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
//
// Upstream (Porch): porch/func/evaluator/stream.go
// kpt extension: request_name in SendRequest/RecvRequest and related comments.

package proto

import (
	"bytes"
	"context"
	"io"

	"google.golang.org/grpc"
)

// maxChunkSize is the maximum number of resource_list / log bytes carried by a
// single streamed gRPC message.
const maxChunkSize = 1 << 20 // 1 MiB

type requestSender interface {
	Send(*EvaluateFunctionRequest) error
}

type requestReceiver interface {
	Recv() (*EvaluateFunctionRequest, error)
}

type responseSender interface {
	Send(*EvaluateFunctionResponse) error
}

type responseReceiver interface {
	Recv() (*EvaluateFunctionResponse, error)
}

// SendRequest streams req to the server, splitting resource_list into chunks.
// The function metadata (image, tag, exec_path, request_name) is only carried by the first
// message. At least one message is always sent, even for an empty resource_list.
func SendRequest(stream requestSender, req *EvaluateFunctionRequest) error {
	rl := req.GetResourceList()
	first := true
	for first || len(rl) > 0 {
		chunk := rl
		if len(chunk) > maxChunkSize {
			chunk = chunk[:maxChunkSize]
		}
		msg := &EvaluateFunctionRequest{ResourceList: chunk}
		if first {
			msg.Image = req.GetImage()
			msg.Tag = req.GetTag()
			msg.ExecPath = req.GetExecPath()
			msg.RequestName = req.GetRequestName()
			first = false
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
		rl = rl[len(chunk):]
	}
	return nil
}

// RecvRequest reassembles a request from the stream. The metadata is taken from
// the first message and resource_list is the concatenation of every chunk in
// receive order.
func RecvRequest(stream requestReceiver) (*EvaluateFunctionRequest, error) {
	out := &EvaluateFunctionRequest{}
	var rl bytes.Buffer
	first := true
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if first {
			out.Image = msg.GetImage()
			out.Tag = msg.GetTag()
			out.ExecPath = msg.GetExecPath()
			out.RequestName = msg.GetRequestName()
			first = false
		}
		_, _ = rl.Write(msg.GetResourceList())
	}
	if rl.Len() > 0 {
		out.ResourceList = rl.Bytes()
	}
	return out, nil
}

// SendResponse streams resp to the client, splitting resource_list and then log
// into chunks. At least one message is always sent, even for an empty response.
func SendResponse(stream responseSender, resp *EvaluateFunctionResponse) error {
	if resp == nil {
		resp = &EvaluateFunctionResponse{}
	}
	rl := resp.GetResourceList()
	log := resp.GetLog()
	sentAny := false

	for len(rl) > 0 {
		chunk := rl
		if len(chunk) > maxChunkSize {
			chunk = chunk[:maxChunkSize]
		}
		if err := stream.Send(&EvaluateFunctionResponse{ResourceList: chunk}); err != nil {
			return err
		}
		rl = rl[len(chunk):]
		sentAny = true
	}

	for len(log) > 0 {
		chunk := log
		if len(chunk) > maxChunkSize {
			chunk = chunk[:maxChunkSize]
		}
		if err := stream.Send(&EvaluateFunctionResponse{Log: chunk}); err != nil {
			return err
		}
		log = log[len(chunk):]
		sentAny = true
	}

	if !sentAny {
		return stream.Send(&EvaluateFunctionResponse{})
	}
	return nil
}

// RecvResponse reassembles a response from the stream by concatenating the
// resource_list and log chunks of every message in receive order.
func RecvResponse(stream responseReceiver) (*EvaluateFunctionResponse, error) {
	var rl, log bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		_, _ = rl.Write(msg.GetResourceList())
		_, _ = log.Write(msg.GetLog())
	}
	resp := &EvaluateFunctionResponse{}
	if rl.Len() > 0 {
		resp.ResourceList = rl.Bytes()
	}
	if log.Len() > 0 {
		resp.Log = log.Bytes()
	}
	return resp, nil
}

// Evaluate performs a full streaming EvaluateFunction round-trip from the client
// side: it opens the stream, sends req (chunked), half-closes the send
// direction, and reassembles the streamed response.
func Evaluate(ctx context.Context, client FunctionEvaluatorClient, req *EvaluateFunctionRequest, opts ...grpc.CallOption) (*EvaluateFunctionResponse, error) {
	stream, err := client.EvaluateFunction(ctx, opts...)
	if err != nil {
		return nil, err
	}
	if err := SendRequest(stream, req); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	return RecvResponse(stream)
}

// UnaryEvaluateFunc is the non-streaming function-evaluation logic: it receives
// a fully-assembled request and returns a fully-assembled response.
type UnaryEvaluateFunc func(ctx context.Context, req *EvaluateFunctionRequest) (*EvaluateFunctionResponse, error)

// UnaryEvaluator is a unary implementation of function evaluation used with NewStreamingServer.
type UnaryEvaluator interface {
	EvaluateFunction(ctx context.Context, req *EvaluateFunctionRequest) (*EvaluateFunctionResponse, error)
}

// NewStreamingServer adapts unary evaluation logic to the streaming
// FunctionEvaluator gRPC service.
func NewStreamingServer(eval UnaryEvaluateFunc) FunctionEvaluatorServer {
	return &streamingServer{eval: eval}
}

type streamingServer struct {
	UnimplementedFunctionEvaluatorServer
	eval UnaryEvaluateFunc
}

func (s *streamingServer) EvaluateFunction(stream grpc.BidiStreamingServer[EvaluateFunctionRequest, EvaluateFunctionResponse]) error {
	req, err := RecvRequest(stream)
	if err != nil {
		return err
	}
	resp, err := s.eval(stream.Context(), req)
	if err != nil {
		return err
	}
	return SendResponse(stream, resp)
}
