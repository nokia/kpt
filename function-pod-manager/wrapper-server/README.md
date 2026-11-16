# wrapper-server (Function Evaluator Pod)

Binary and libraries: streaming `FunctionEvaluator` gRPC service,
`FunctionPodRequest` claim reconciliation, and KRM function execution.

## Layout

| Path | Role |
|------|------|
| `evaluator/` | gRPC API (proto + helpers ported from Porch `func/evaluator`). |
| `healthchecker/` | gRPC health service (ported from Porch `func/healthchecker`). |
| `*.go` (root) | Claim controller, gRPC wiring, `controller-runtime` startup, CLI (`package main`). |

After moving shared types, run `go mod tidy` in this directory.
