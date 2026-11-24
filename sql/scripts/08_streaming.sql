-- 08_streaming.sql
-- Streaming speed layer: realtime tables, stream registry and append-only health.

BEGIN;

CREATE SCHEMA IF NOT EXISTS realtime;

-- -----------------------------------------------------------------------------
-- 1. realtime.delivery_events_raw
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS realtime.delivery_events_raw (
    event_id          text        NOT NULL,
    event_type        text        NOT NULL,
    stop_type         text        NOT NULL,
    facility_id       text        NOT NULL,
    scheduled_datetime timestamptz NOT NULL,
    actual_datetime   timestamptz NOT NULL,
    event_time        timestamptz NOT NULL,
    detention_minutes integer     NOT NULL,
    on_time_flag      boolean     NOT NULL,
    location_city     text        NOT NULL,
    location_state    text        NOT NULL,
    loaded_at         timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_delivery_events_raw PRIMARY KEY (event_id, event_type),
    CONSTRAINT ck_delivery_events_raw_event_type
        CHECK (event_type IN ('ARRIVED', 'DEPARTED')),
    CONSTRAINT ck_delivery_events_raw_stop_type
        CHECK (stop_type IN ('PICKUP', 'DELIVERY')),
    CONSTRAINT ck_delivery_events_raw_detention
        CHECK (detention_minutes >= 0)
);

CREATE INDEX IF NOT EXISTS ix_delivery_events_raw_facility_time
    ON realtime.delivery_events_raw (facility_id, event_time DESC);

COMMENT ON TABLE realtime.delivery_events_raw IS
    'Disposable stream buffer keyed by delivery event and lifecycle type.';

-- -----------------------------------------------------------------------------
-- 2. realtime.facility_service_window
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS realtime.facility_service_window (
    facility_id       text        NOT NULL,
    window_start      timestamptz NOT NULL,
    window_end        timestamptz NOT NULL,
    events_count      integer     NOT NULL DEFAULT 0,
    completed_events  integer     NOT NULL DEFAULT 0,
    on_time_events    integer     NOT NULL DEFAULT 0,
    late_events       integer     NOT NULL DEFAULT 0,
    on_time_known_events integer  NOT NULL DEFAULT 0,
    detention_events  integer     NOT NULL DEFAULT 0,
    detention_minutes integer     NOT NULL DEFAULT 0,
    dwell_minutes     integer     NOT NULL DEFAULT 0,
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_facility_service_window PRIMARY KEY (facility_id, window_start),
    CONSTRAINT ck_facility_service_window_window
        CHECK (window_end > window_start),
    CONSTRAINT ck_facility_service_window_counts_non_negative
        CHECK (
            events_count >= 0 AND completed_events >= 0
            AND on_time_events >= 0 AND late_events >= 0
            AND on_time_known_events >= 0 AND detention_events >= 0
            AND detention_minutes >= 0 AND dwell_minutes >= 0
        )
);

COMMENT ON TABLE realtime.facility_service_window IS
    'Disposable event-time service windows used by the speed layer only.';

-- -----------------------------------------------------------------------------
-- 3. realtime.detention_alert
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS realtime.detention_alert (
    event_id          text        PRIMARY KEY,
    facility_id       text        NOT NULL,
    status            text        NOT NULL DEFAULT 'OPEN',
    opened_at         timestamptz NOT NULL,
    resolved_at       timestamptz,
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_detention_alert_status CHECK (status IN ('OPEN', 'RESOLVED')),
    CONSTRAINT ck_detention_alert_resolution
        CHECK ((status = 'OPEN' AND resolved_at IS NULL)
               OR (status = 'RESOLVED' AND resolved_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ix_detention_alert_open
    ON realtime.detention_alert (status, opened_at DESC) WHERE status = 'OPEN';

COMMENT ON TABLE realtime.detention_alert IS
    'Open or resolved detention crossings from the stream stateful operator.';

-- -----------------------------------------------------------------------------
-- 4. control.stream
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.stream (
    stream_name       text        PRIMARY KEY,
    topic             text        NOT NULL,
    dlq_topic         text        NOT NULL,
    contract_file     text        NOT NULL,
    target_tables     text[]      NOT NULL,
    checkpoint_path   text        NOT NULL,
    owner             text        NOT NULL,
    status            text        NOT NULL DEFAULT 'DISABLED',
    registered_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_stream_status CHECK (status IN ('ENABLED', 'DISABLED')),
    CONSTRAINT ck_stream_targets
        CHECK (target_tables @> ARRAY[
            'realtime.delivery_events_raw',
            'realtime.facility_service_window',
            'realtime.detention_alert'
        ]::text[])
);

COMMENT ON TABLE control.stream IS
    'Registry of supervised streams and their disposable speed-layer targets.';

-- -----------------------------------------------------------------------------
-- 5. audit.stream_health  (append-only)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.stream_health (
    health_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_name           text        NOT NULL,
    topic                 text        NOT NULL,
    source                text        NOT NULL,
    observed_at           timestamptz NOT NULL DEFAULT now(),
    partition_lag         bigint,
    newest_event_age_seconds numeric(12, 2),
    rows_raw              integer     NOT NULL DEFAULT 0,
    rows_window           integer     NOT NULL DEFAULT 0,
    rows_alert            integer     NOT NULL DEFAULT 0,
    rejected              integer     NOT NULL DEFAULT 0,
    late_dropped          integer     NOT NULL DEFAULT 0,
    message               text,
    CONSTRAINT ck_stream_health_source CHECK (source IN ('JOB', 'WATCHDOG')),
    CONSTRAINT ck_stream_health_counts_non_negative
        CHECK (
            partition_lag IS NULL OR partition_lag >= 0
        ),
    CONSTRAINT ck_stream_health_rates_non_negative
        CHECK (
            newest_event_age_seconds IS NULL OR newest_event_age_seconds >= 0
        )
);

CREATE INDEX IF NOT EXISTS ix_stream_health_stream_time
    ON audit.stream_health (stream_name, observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_stream_health_source_time
    ON audit.stream_health (source, observed_at DESC);

CREATE TRIGGER trg_stream_health_append_only
    BEFORE UPDATE OR DELETE ON audit.stream_health
    FOR EACH ROW EXECUTE FUNCTION audit.prevent_modification();
CREATE TRIGGER trg_stream_health_no_truncate
    BEFORE TRUNCATE ON audit.stream_health
    FOR EACH STATEMENT EXECUTE FUNCTION audit.prevent_modification();

COMMENT ON TABLE audit.stream_health IS
    'Append-only micro-batch and watchdog health evidence for supervised streams.';

COMMIT;
