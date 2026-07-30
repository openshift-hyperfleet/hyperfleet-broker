# River (Postgres) broker backend POC

**Status.** Working end to end. The full HyperFleet pipeline (API, Sentinel,
adapter) ran in kind on 2026-07-30 with Postgres as the only event
transport: a cluster POSTed to the API reached `Reconciled=True` with every
event hop riding `river_job` rows instead of RabbitMQ or Pub/Sub. All nine
shared integration scenarios pass, goroutine leak tests pass, and a load
run sustained 200 events/sec with 3-way fan-out (17,982/17,982 delivered,
p50 41ms, p99 88ms). Branch: `poc/river-backend` (this repo),
`poc/river-broker` (sentinel, adapter). The POC evaluates the "interim
doorbell swap" shape: per-subscription queues in Postgres behind the
existing broker seam. It does not prejudge the outbox-with-cursors endgame,
it proves the operational half (Postgres carries the bus load fine) and
retires the "we need a broker component" assumption.

## What changed

| Repo | Change |
|---|---|
| hyperfleet-broker | `broker/river.go` (new backend, no Watermill), 3-line dispatch/validate touches in `broker.go`/`config.go`, DSN masking branch, unit tests, `test/integration/river/` (nine scenarios on a postgres:16 testcontainer), river cases in the leak tests, `poc/river-pipeline/` (kind demo + loadgen) |
| hyperfleet-sentinel | 1 Go line (OTel messaging-system map), chart: broker.yaml template block, values schema + defaults. Broker pinned to the fork build |
| hyperfleet-adapter | Zero Go changes. Chart: `river` joins the allowed types (3 one-word edits), `charts/examples/river-poc/` using the `broker.yaml` raw passthrough |

## Semantics mapping

| Broker concept | River realization |
|---|---|
| Topic | Name fragment in the queue derivation, plus the key in the subscription registry |
| Subscription ID | `broker_subscriptions` row + a River queue per (topic, subscription) pair |
| Fan-out (different IDs) | Publish reads the registry and inserts one job per subscription in a single `InsertMany` batch |
| Competing consumers (same ID) | Replicas work the same River queue |
| Publish | Batch insert of a `cloudevent` job kind carrying the JSON-encoded CloudEvent |
| Ack / Nack | Worker returns nil (complete) or error (retry on a fixed backoff up to `max_attempts`, then discard) |
| Poison message | Undecodable payload is cancelled outright (`river.JobCancel`), it can never succeed |
| Publish dedup (JetStream Msg-Id) | River unique jobs on (queue, encoded args), `unique_events` flag, on by default |
| Push latency | LISTEN/NOTIFY wake-up built into River, polling fallback at `poll_interval` |
| DLQ | None; discarded jobs surface on the subscriber error channel and stay queryable in `river_job` |

## Config

```yaml
broker:
  type: river
  river:
    dsn: postgres://user:pass@host:5432/broker?sslmode=disable  # URI form required
    create_schema_if_missing: true   # river migrations + subscription registry
    max_attempts: 5                  # attempts before discard
    retry_backoff: 1s                # fixed retry delay
    poll_interval: 1s                # fetch fallback (LISTEN/NOTIFY is the fast path)
    fetch_cooldown: 100ms            # per-subscriber throughput governor
    pool_max_conns: 10
    unique_events: true
subscriber:
  parallelism: 1                     # MaxWorkers per queue
```

**The one tuning knob that matters:** `fetch_cooldown` bounds per-subscriber
throughput at roughly `parallelism` jobs per cooldown interval. The 100ms
default gives ~10x parallelism deliveries/sec per subscriber, ample for the
real pipeline (events are per-in-flight-resource, tens/sec); the load
harness runs at 10ms to hit hundreds/sec.

## How to test

```bash
make test                                    # unit, includes river config/sanitizer tests
make test-integration                        # nine scenarios + leak tests (podman)
./poc/river-pipeline/scripts/00-setup.sh     # full pipeline in kind
./poc/river-pipeline/scripts/10-lifecycle-test.sh
./poc/river-pipeline/scripts/20-load-test.sh # loadgen + table stats
./poc/river-pipeline/scripts/99-teardown.sh
```

## Measured

- Local (podman postgres:16, one machine): 500 events/sec published, 5-way
  fan-out = 2,475 deliveries/sec, 100 percent delivered, p50 43ms, p99 92ms,
  flat as rate increased. 37k jobs weighed 78MB pre-pruning.
- In kind through a port-forward: 200 events/sec, 3-way fan-out,
  17,982/17,982 delivered, p50 41ms, p99 88ms, max 1.03s.
- Lifecycle: POST to `Reconciled=True` in ~25s, indistinguishable from the
  other backends (the 5s Sentinel poll dominates, not the transport). The
  completed-job trail in `river_job` is the Sentinel republish heartbeat
  made visible, 15 jobs for one cluster reaching Reconciled.

## Consumer wiring (fork-replace)

```
replace github.com/openshift-hyperfleet/hyperfleet-broker => github.com/ciaranRoche/hyperfleet-broker <pseudo-version of poc/river-backend>
```

Sentinel chart: `--set broker.type=river --set-string broker.river.dsn=... --set broker.river.createSchemaIfMissing=true`.
Adapter chart: `-f examples/river-poc/values.yaml` (raw `broker.yaml` passthrough, no template work).

## What this buys / what it costs

Buys: one fewer component class entirely (no broker to deploy, operate, or
mirror for disconnected installs), delivery observable with plain SQL
(`SELECT queue, state, count(*) FROM river_job GROUP BY 1,2`), retry and
discard semantics that actually run (unlike the DLQ config the current
pipeline never exercises), and exactly-once-processing potential when a
consumer's state lives in the same Postgres.

Costs, honestly: the dependency delta is heavier than NATS's single dep
(river + riverdriver + pgx and transitives); fan-out is write-amplified
(N job rows per event, one per subscription, fine at 3-15 subscriptions,
wrong at hundreds); the subscription registry adds a registration-before-
publish race identical to every other backend's create-subscription-first
semantics; completed-job retention (default 24h) means table size is
governed by retention, not burst, and needs watching under condition-churn
volumes; and Postgres becomes a shared fate domain for the bus, which is
precisely the argument the outbox endgame resolves properly (the event row
commits with the mutation instead of being a second write). Known gaps
carried from the NATS POC: no auth/TLS story beyond DSN credentials, single
Postgres replica in the demo.
