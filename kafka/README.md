# Kafka speed layer

This directory holds the opt-in Kafka replay producer, Spark Structured Streaming
job, strict stream contract and local Compose stack for `realtime.*`. It is a speed
layer only: bronze, silver, gold and Databricks marts never depend on it.

## Run locally

```bash
make stream-up
make stream-produce
make stream-status
```

`make stream-up` starts the `streaming` Compose profile, creates topics from
`kafka/config/topics.yml` and enables the row in `control.stream`. The job consumes
only that prefixed topic and writes only `realtime.*` plus append-only
`audit.stream_health` rows.

`make stream-demo` replays the fixture samples into `fleet_dev` with no source
credentials. It needs the external Compose network named by `WAREHOUSE_NETWORK`
and a Postgres host reachable from the stream container (`STREAM_PG_HOST`).

## Contract and safety

- `contracts/streams/delivery_events.v1.yml` is the only accepted value shape.
- Invalid values go to the prefixed DLQ with reason, original payload and offsets.
- Valid events land once per `(event_id, event_type)` in `realtime.delivery_events_raw`.
- Events older than the committed watermark minus allowed lateness are counted in
  `late_dropped` and excluded from windows and alerts.
- Topics, DLQ payloads and logs never contain PII.
- `ENV=dev`/`ci` requires a `fleet_dev`/`fleet_it_*` database and a `dev.`/`ci.`
  topic prefix. Production streaming requires `STREAM_ALLOW_PROD=YES` and
  `CONFIRM=yes`.

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

Unit tests require no broker; integration tests use the local Kafka profile and a
throwaway `fleet_it_*` database.
