# Kafka speed layer

This directory holds the opt-in Kafka replay producer, Spark Structured Streaming
job, strict stream contract and local Compose stack for `realtime.*`. It is a speed
layer only: bronze, silver, gold and Databricks marts never depend on it.

## Run locally

```bash
make stream-build
make stream-up
make stream-produce
make stream-status
```

`make stream-build` builds the local `fleet-stream:local` image. `make stream-up`
starts the `streaming` Compose profile, creates topics from
`kafka/config/topics.yml` and enables the row in `control.stream`. Host clients use
`localhost:9094`; containers use `kafka:9092`. The job consumes only that prefixed
topic and writes only `realtime.*` plus append-only `audit.stream_health` rows.

`make stream-demo` replays the fixture samples into `fleet_dev` with no source
credentials. It needs the external Compose network named by `WAREHOUSE_NETWORK`
and a Postgres host reachable from the stream container (`STREAM_PG_HOST`).

## Contract and safety

- `contracts/streams/delivery_events.v1.yml` is the only accepted value shape.
- Invalid values go to the prefixed DLQ with a reason and original payload; job-side
  rejects also include topic, partition and offset.
- Valid events land once per `(event_id, event_type)` in `realtime.delivery_events_raw`.
- Events older than the committed watermark minus allowed lateness are counted in
  `late_dropped` and excluded from windows and alerts.
- Topics, DLQ payloads and logs never contain PII.
- `ENV=dev`/`ci` requires a `fleet_dev`/`fleet_it_*` database and a `dev.`/`ci.`
  topic prefix. Production streaming requires `STREAM_ALLOW_PROD=YES` and
  `CONFIRM=yes`.

## Monitoring and reconciliation

```bash
make stream-monitor
make stream-reconcile BATCH_ID=42
```

The watchdog probe records Kafka high-minus-low backlog and newest-event age in
`audit.stream_health`; the message is labelled `topic-high-low` because Spark
tracks consumed offsets in its checkpoint rather than a consumer group. Breaches
of `KAFKA_LAG_THRESHOLD` or `STREAM_FRESHNESS_THRESHOLD_MINUTES` produce a
`BREACH` message and a redacted Airflow alert.

Reconciliation is observational. It writes `audit.reconciliation_results` with
`target_layer=gold`, uses valid statuses `MATCH`, `WITHIN_TOLERANCE` and
`MISMATCH`, and prefixes drift messages with `DRIFT`. A disabled stream writes
one `STREAM_SKIPPED` warning instead; nothing here fails the daily DAG.

## Reset

```bash
make stream-reset CONFIRM=yes
```

This drops the checkpoint and truncates only:

- `realtime.delivery_events_raw`
- `realtime.facility_service_window`
- `realtime.detention_alert`

Kafka topic retention, bronze, silver, gold, snapshots, marts and audit history are
not touched. Replay from the source or from the topic's seven-day retention after a
reset.

## Tests

```bash
make stream-test
make stream-test-integration
```

Unit tests require no broker. Integration tests use the local Kafka profile and a
throwaway `fleet_it_*` database; they are skipped unless
`RUN_STREAM_INTEGRATION=1` and the stack is already running.
