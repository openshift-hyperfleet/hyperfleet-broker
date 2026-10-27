# Event log (Postgres) broker backend POC

**Status.** Working end to end behind the broker seam. The nine shared
integration scenarios and the goroutine leak tests pass, plus targeted
tests: visibility under concurrent publishers, crash recovery, injected
hole resolution, and PgBouncer transaction pooling. Load: 100 percent
delivery at 500 events/sec with 5-way fan-out. Branch:
`HYPERFLEET-1656-purpose-built-backend` (off `poc/river-backend`). Spike:
[HYPERFLEET-1656](https://redhat.atlassian.net/browse/HYPERFLEET-1656);
the comparison and decision context live in the spike doc in
`openshift-hyperfleet/architecture`.

## Semantics mapping

| Broker concept | Event log realization |
| --- | --- |
| Topic | `broker_events` rows keyed by topic with a per-topic sequence from `broker_topic_seq` |
| Subscription ID | `broker_cursors` row per (topic, subscription ID) |
| Fan-out (different IDs) | Independent cursors read the same rows; publish writes no per-subscription rows |
| Competing consumers (same ID) | Replicas reserve disjoint sequence ranges; claims are `broker_ranges` rows with per-range leases |
| Publish | One transaction: bootstrap counter, allocate sequence, insert one row |
| Ack / Nack | Ack advances the cursor only on handler success; a failure releases the range for any subscriber after `retry_backoff` |
| Retry / DLQ | Shared `attempts` counter on the event row; past `max_attempts` the event is skipped and surfaced on the subscriber error channel |
| Poison message | An undecodable payload takes the same failure path and dead-letters at the cap |
| Ordering | Per-topic sequence; the acked cursor advances only over contiguous ids, and a hole older than `grace_period` is skipped as rolled back |
| Crash recovery | A crashed worker's range lease expires and any worker reclaims it; delivery is at-least-once within a subscription |
| Push latency | Polling only (`poll_interval`, `fetch_cooldown`); no LISTEN/NOTIFY |
| Retention | Daily partitions of `broker_events`, created a day ahead, dropped by a reaper after `retention` |

Sequence allocation is transactional with the insert and serialized by the
counter row lock, so this publisher cannot produce in-flight or rolled-back
gaps; the contiguity-plus-grace rule keeps the reader correct for any
producer that can.

## Config

```yaml
broker:
  type: eventlog
  eventlog:
    dsn: postgres://user:pass@host:5432/broker?sslmode=disable  # URI form required
    create_schema_if_missing: true
    poll_interval: 1s          # idle poll cadence
    fetch_cooldown: 100ms      # pause after each range; the per-subscriber throughput governor
    batch_size: 10             # events per reserved range
    grace_period: 5s           # sequence-hole wait before skipping as rolled back
    lease_duration: 30s        # range-claim lifetime for crash recovery
    max_attempts: 5            # shared delivery attempts before dead-letter
    retry_backoff: 200ms       # released-range reclaim delay
    retention: 24h             # daily-partition retention
    pool_max_conns: 10
subscriber:
  parallelism: 4               # range workers per topic
```

**The throughput governor:** `fetch_cooldown` bounds per-subscriber intake
at roughly `batch_size` events per cooldown. The 100ms default gives about
100 events/sec per worker at batch 10; the load harness runs 10ms.

## How to test

Requires a running Docker or Podman daemon (the integration tests start Postgres and PgBouncer containers).

```bash
make test                # unit: config validation, defaults, partition naming
make test-integration    # nine scenarios, leak tests, and the targeted tests
                         # (visibility, crash recovery, hole resolution, PgBouncer)

go run ./poc/river-pipeline/loadgen \
  -backend eventlog \
  -dsn postgres://broker:broker@localhost:5432/broker?sslmode=disable \
  -rate 500 -duration 30s -subs 5 -parallelism 4
```

For the in-kind numbers, create a kind cluster and deploy the postgres manifest:

```bash
kind create cluster --name eventlog-poc --wait 60s
kubectl create namespace hyperfleet
kubectl -n hyperfleet apply -f poc/river-pipeline/manifests/postgres.yaml
kubectl -n hyperfleet port-forward svc/river-postgres 15432:5432 &
go run ./poc/river-pipeline/loadgen \
  -backend eventlog \
  -dsn postgres://broker:broker@localhost:15432/broker?sslmode=disable \
  -rate 200 -duration 30s -subs 3 -parallelism 8
kind delete cluster --name eventlog-poc
```

## Measured

One machine, one Postgres, one harness for both backends (Docker Desktop
`postgres:16`, parallelism 8, 30s runs; the event log at batch 10 /
cooldown 10ms; repeat run on a fresh instance):

| Backend | Scenario | Delivery | p50 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Event log | 500 events/sec, 5-way | 73,755 / 73,755 | 6ms | 211ms | 496ms |
| Event log | 200 events/sec, 3-way | 17,628 / 17,628 | 8ms | 176ms | 482ms |
| River | 500 events/sec, 5-way | 73,425 / 73,425 | 35ms | 94ms | 881ms |
| River | 200 events/sec, 3-way | 17,817 / 17,817 | 43ms | 92ms | 789ms |

Zero publish errors in every run. The event log's
median is about 5x lower than River's (no queue round trip); River's tail
is about 2x tighter (LISTEN/NOTIFY push versus batch polling), and every
figure is within a few percent across repeat runs. The event log's p99 is
insensitive to the fetch cooldown (211ms at 5ms and at 10ms). River at
parallelism 4 starves at 500 events/sec (p50 in seconds), so the
comparison uses the harness's parallelism-8 shape throughout.

In-kind (single Postgres pod in kind, port-forward, both backends,
parallelism 8, 30s at 200 events/sec with 3-way fan-out):

| Backend | Delivery | p50 | p99 | max |
| --- | --- | --- | --- | --- |
| Event log | 17,334 / 17,334 | 12ms | 213ms | 505ms |
| River | 17,283 / 17,283 | 44ms | 101ms | 1.07s |

River's kind figures reproduce `RIVER-POC.md`'s recorded in-kind run
(p50 41ms, p99 88ms, max 1.03s). Same latency pattern as local: event
log median lower, River tail tighter.

- Storage: in equivalent 30-second runs at 500 events/sec with 5-way
  fan-out, the event log stored 14,577 events in 5.5MB — about 0.4KB per
  event — with zero dead tuples. River stored 73,340 job rows in 97MB —
  about 1.4KB per row, one per subscription per event, about 7KB per
  event. Neither run triggered retention cleanup (24h default in both).
  Consumer lag and cursor state are plain SQL: `max(seq) - acked` per
  (topic, subscription) read 0 for every subscription after drain.
- Retention: partitions are created a day ahead (`broker_events_20261024`
  existed during the run); a partition older than `retention` is dropped
  by the reaper (verified with an injected stale partition).
- PgBouncer 1.26 in transaction mode: the shared scenarios pass with pgx's
  default statement caching; no `default_query_exec_mode` override needed.

Range reservation runs in one transaction holding the cursor row lock from
the read through the range insert, so concurrent workers cannot claim the
same range.

## What this buys / what it costs

Buys: no external dependency beyond pgx; one event row per event
regardless of subscription count; append-only storage with partition
rotation and near-zero vacuum load; per-subscription consumer lag in one
SQL query; the same table becomes an API-emitted outbox by changing the
producer; transaction-pooling safe with no LISTEN/NOTIFY.

Costs: a polling latency tail (p99 about 200ms at the tested knobs,
insensitive to the fetch cooldown, max under 500ms) where River pushes via LISTEN/NOTIFY; per-topic publish
serialization on the counter row (irrelevant at the contract's tens of
events per second); range coordination adds two small tables and one
transaction per batch; no built-in UI or retry tooling — metrics wire into
the existing `MetricsRecorder`, everything else is SQL.
