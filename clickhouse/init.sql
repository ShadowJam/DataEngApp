CREATE DATABASE IF NOT EXISTS rates_db;

CREATE TABLE IF NOT EXISTS rates_db.fact_rates (
    date Date,
    target_currency String,
    avg_rate Float64,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, target_currency);