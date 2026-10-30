CREATE DATABASE IF NOT EXISTS rates_db;

CREATE TABLE IF NOT EXISTS rates_db.fact_rates (
    date Date,
    target_currency String,
    avg_rate Float64,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, target_currency);

CREATE TABLE IF NOT EXISTS rates_db.streaming_rates (
    window_start DateTime,
    window_end DateTime,
    currency String,
    avg_rate Float64
) ENGINE = MergeTree()
ORDER BY (window_start, currency)
TTL window_start + INTERVAL 5 MINUTE;