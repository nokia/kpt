# wrapper-server (Function Evaluator Pod)

Binary and libraries: streaming `FunctionEvaluator` gRPC service,
`FunctionPodRequest` claim reconciliation, and KRM function execution.

## Layout

| Path | Role |
|------|------|
| `proto/` | gRPC API (proto + helpers ported from Porch `func/evaluator`). |
| `healthchecker/` | gRPC health service (ported from Porch `func/healthchecker`). |
| `*.go` (root) | Claim controller, gRPC wiring, `controller-runtime` startup, CLI (`package main`). |

This directory is the `github.com/kptdev/kpt/wrapper-server` module. It uses the
sibling `function-pod-manager` module through a local replace. `go test ./...`
from this directory runs the tests.

