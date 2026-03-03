-- Initial database schema migration
-- Creates base tables for TRON Sentinel

CREATE TABLE IF NOT EXISTS raw_data (
    id          BIGSERIAL PRIMARY KEY,
    source      VARCHAR(50)  NOT NULL,
    content     TEXT         NOT NULL,
    url         TEXT,
    author      VARCHAR(255),
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sentiment_results (
    id           BIGSERIAL PRIMARY KEY,
    raw_data_id  BIGINT REFERENCES raw_data(id),
    score        FLOAT        NOT NULL,
    label        VARCHAR(20)  NOT NULL,
    analyzed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL PRIMARY KEY,
    level        VARCHAR(20)  NOT NULL,
    message      TEXT         NOT NULL,
    triggered_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ
);
