// Copyright 2022 The kpt and Nephio Authors
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
// Upstream (Porch): porch/func/wrapper-server/main.go — type singleFunctionEvaluator and EvaluateFunction.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/kptdev/kpt/api/kubeobject"
	pb "github.com/kptdev/kpt/wrapper-server/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// SingleFunctionEvaluator runs the configured entrypoint binary as the KRM function.
type SingleFunctionEvaluator struct {
	Entrypoint []string
}

var _ pb.UnaryEvaluator = &SingleFunctionEvaluator{}

func (e *SingleFunctionEvaluator) EvaluateFunction(ctx context.Context, req *pb.EvaluateFunctionRequest) (*pb.EvaluateFunctionResponse, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, e.Entrypoint[0], e.Entrypoint[1:]...) // #nosec G204 -- entrypoint fixed at deploy time
	cmd.Stdin = bytes.NewReader(req.ResourceList)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	outbytes := stdout.Bytes()
	stderrStr := stderr.String()

	var resultsStr string
	rl, parseErr := kubeobject.ParseResourceList(outbytes)
	if parseErr == nil && rl != nil {
		jsonBytes, _ := json.Marshal(rl.Results)
		resultsStr = string(jsonBytes)
	}

	if err != nil {
		klog.Warningf("Error while evaluating %q: %s", req.Image, err)
		klog.V(4).Infof("Input Resource List: %s\nOutput Resource List: %s", req.ResourceList, outbytes)
		if resultsStr != "" {
			return nil, status.Errorf(codes.Internal,
				"Failed to execute function %q: %s ; <stderr>%s</stderr> ; <results>%s</results>",
				req.Image, err, stderrStr, resultsStr)
		}
		return nil, status.Errorf(codes.Internal,
			"Failed to execute function %q: %s ; <stderr>%s</stderr> ; <stdout>%s</stdout>",
			req.Image, err, stderrStr, string(outbytes))
	}

	klog.Infof("Evaluated %q: stdout length in bytes: %d", req.Image, len(outbytes))

	if parseErr == nil && rl != nil && rl.Results.ExitCode() != 0 {
		klog.Warningf("function %q returned with an error: <results>%s</results> ; <stderr>%s</stderr>", req.Image, resultsStr, stderrStr)
	}

	return &pb.EvaluateFunctionResponse{
		ResourceList: outbytes,
		Log:          []byte(stderrStr),
	}, nil
}
