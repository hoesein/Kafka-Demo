CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS worker_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    worker_type TEXT NOT NULL,
    cron_expression TEXT NOT NULL DEFAULT '*/5 * * * *',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    retry_count INTEGER NOT NULL DEFAULT 3 CHECK (retry_count >= 0),
    timeout_seconds INTEGER NOT NULL DEFAULT 60 CHECK (timeout_seconds > 0),
    batch_size INTEGER NOT NULL DEFAULT 100 CHECK (batch_size > 0),
    configuration_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_run_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled', 'deleted'))
);

CREATE TABLE IF NOT EXISTS worker_definition_settings (
    id BIGSERIAL PRIMARY KEY,
    worker_definition_id UUID NOT NULL REFERENCES worker_definitions(id) ON DELETE CASCADE,
    setting_key TEXT NOT NULL,
    setting_value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (worker_definition_id, setting_key)
);

CREATE TABLE IF NOT EXISTS worker_execution_history (
    id BIGSERIAL PRIMARY KEY,
    worker_definition_id UUID NOT NULL REFERENCES worker_definitions(id) ON DELETE CASCADE,
    correlation_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'failed', 'timeout', 'cancelled')),
    processed_record_count INTEGER NOT NULL DEFAULT 0 CHECK (processed_record_count >= 0),
    error_message TEXT NULL,
    execution_duration_ms BIGINT NULL
);

CREATE TABLE IF NOT EXISTS notification_queue (
    id BIGSERIAL PRIMARY KEY,
    message_key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW', 'PROCESSING', 'SENT', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_worker_definitions_type
    ON worker_definitions (worker_type);

CREATE INDEX IF NOT EXISTS idx_worker_definitions_enabled
    ON worker_definitions (enabled);

CREATE INDEX IF NOT EXISTS idx_worker_definition_settings_worker
    ON worker_definition_settings (worker_definition_id);

CREATE INDEX IF NOT EXISTS idx_worker_execution_history_worker
    ON worker_execution_history (worker_definition_id);

CREATE INDEX IF NOT EXISTS idx_notification_queue_status
    ON notification_queue (status);

CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_worker_definitions_touch_updated_at ON worker_definitions;
CREATE TRIGGER trg_worker_definitions_touch_updated_at
BEFORE UPDATE ON worker_definitions
FOR EACH ROW
EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_worker_definition_settings_touch_updated_at ON worker_definition_settings;
CREATE TRIGGER trg_worker_definition_settings_touch_updated_at
BEFORE UPDATE ON worker_definition_settings
FOR EACH ROW
EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_notification_queue_touch_updated_at ON notification_queue;
CREATE TRIGGER trg_notification_queue_touch_updated_at
BEFORE UPDATE ON notification_queue
FOR EACH ROW
EXECUTE FUNCTION touch_updated_at();
