-- Feedback labels table: stores every transaction scored by Spark.
-- Run once: docker exec -i project-copy-postgres-1 psql -U airflow -d frauddb < scripts/init_feedback.sql

CREATE TABLE IF NOT EXISTS feedback_labels (
    id               SERIAL PRIMARY KEY,
    transaction_id   TEXT        NOT NULL UNIQUE,
    amount           FLOAT       NOT NULL,
    is_fraud         SMALLINT    NOT NULL DEFAULT 0,   -- model verdict (0/1)
    fraud_score      FLOAT,                            -- raw probability
    features_json    JSONB,                            -- feature snapshot at scoring time
    confirmed_fraud  SMALLINT,                         -- NULL until a human confirms
    scored_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at     TIMESTAMPTZ
);

-- Index for the 7-day rolling window read in Airflow
CREATE INDEX IF NOT EXISTS idx_feedback_scored_at ON feedback_labels (scored_at DESC);

-- Index for quick fraud-only queries
CREATE INDEX IF NOT EXISTS idx_feedback_is_fraud  ON feedback_labels (is_fraud);
