# AGENTS.md — hyperfleet-broker

Go library: unified pub/sub API over RabbitMQ and Google Pub/Sub with CloudEvents. Not a binary — consumed as a dependency by other HyperFleet services. Wraps [Watermill](https://github.com/ThreeDotsLabs/watermill) internally; public API never exposes Watermill types.

## Verification

| Target | What it runs |
|--------|-------------|
| `make lint` | golangci-lint v2.7.0 (pinned in tools/go.mod, no config file - default rules) |
| `make fmt` | `gofmt -s -w .` |
| `make test` | Unit tests: `./broker/... ./pkg/...` (timeout 10m) |
| `make test-integration` | Integration tests: `./test/integration/...` (sequential `-p 1`, timeout 10m) |
| `make test-all` | Both unit + integration |

### Pre-push order

1. `make fmt` → 2. `make lint` → 3. `make test` → 4. `make test-integration` (if touching broker/ or test/)

## Source of truth

| Topic | Location |
|-------|----------|
| Public API | `broker/broker.go`, `broker/publisher.go`, `broker/subscriber.go`, `broker/metrics.go`, `broker/errors.go` |
| Logger interface | `pkg/logger/logger.go` |
| Config structure + validation | `broker/config.go`, `broker/rabbitmq.go`, `broker/googlepubsub.go` |
| Config fields reference | `example/broker.example.yaml` |
| Integration test helpers | `test/integration/common/common.go` |
| Mock logger (unit tests) | `pkg/logger/mock.go` — use `NewMockLogger()` |
| Container setup helpers | `test/integration/rabbitmq/setup.go`, `test/integration/googlepubsub/setup.go` |
| Leak & perf integration tests | `test/integration/broker_leak_test.go`, `test/integration/broker_perf_test.go` |
| Examples (separate go.mod) | `example/go.mod`, `example/cmd/publisher/main.go`, `example/cmd/subscriber/main.go` |
| Docker Compose examples | `example/rabbitmq/`, `example/googlepubsub/` |
| Comprehensive user docs | `README.md` |

## Architecture context

Only patterns an agent cannot infer from reading the code:

- **Subscription ID** controls messaging pattern: same ID = shared/load-balanced queue, different IDs = fanout. RabbitMQ queue name = `{topic}-{subscriptionID}` (default) or `{queue}-{subscriptionID}` when `broker.rabbitmq.queue` is set. Google Pub/Sub subscription name = `{subscriptionID}`.
- **Config precedence**: programmatic map > env vars > broker.yaml file > defaults. `BROKER_CONFIG_FILE` env var overrides file path. Env vars use underscore for dots (e.g., `BROKER_RABBITMQ_URL`).
- **Health check asymmetry**: RabbitMQ = in-memory connection state check (no network call). Google Pub/Sub = `GetTopic` API probe with 3s timeout on a non-existent topic; `NotFound` = healthy.
- **Config debugging**: `log_config: true` in broker.yaml (or `LOG_CONFIG=true`) logs full config as JSON at creation time. Passwords masked.
- **DLQ topic naming**: DLQ topic is always `{subscriptionID}-dlq` (hardcoded at `googlepubsub.go`). Not configurable.

## Project boundaries

### DO

- Keep Watermill as internal implementation detail — never expose Watermill types in public API
- Write integration tests for new broker-level behavior using `test/integration/common/` helpers

### DON'T

- Don't add a third broker backend without updating `validateConfig`, both constructors, health checks, and integration test infrastructure

## Gotchas

- **Google Pub/Sub health check requires `pubsub.topics.get`** — NOT included in `roles/pubsub.publisher`. Grant `roles/pubsub.viewer` or custom role.
- **`subscriber.parallelism` > 1 only needed for RabbitMQ**. Google Pub/Sub handles parallelism internally via `num_goroutines` and `max_outstanding_messages`.
- **Integration tests run sequentially** (`-p 1`) because CI has 1 CPU. Parallel execution causes timeouts.
- **`example/` has its own `go.mod`** — `make test` from root does not test examples. Update `example/go.mod` when changing public API.
- **`error_test.go` "missing rabbitmq url" test is false-passing**: sets `expectError: false` but `validateRabbitMQConfig` rejects empty URLs. Test passes because else branch only asserts when `err == nil`. Do not rely on it as documenting intentional behavior.
- **Google Pub/Sub subscriber auto-creates DLQ topic** (`{subscriptionID}-dlq`) when `create_topic_if_missing` is true — see `googlepubsub.go:188`.
- **Integration tests**: call `common.SetupTestEnvironment()` first in `TestMain`, share one container per package. Topic/subscription name uniqueness is handled internally by `Run*` helper functions in `common/common.go`. Pattern at `test/integration/rabbitmq/rabbitmq_test.go:28`.
