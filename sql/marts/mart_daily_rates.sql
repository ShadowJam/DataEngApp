CREATE OR REPLACE TABLE rates_db.mart_daily_rates (
    date Date,
    currency_code String,
    currency_name String,  -- denorm
    avg_rate Float64,
    rate_change_pct Float64
) ENGINE = MergeTree()
ORDER BY (date, currency_code);

INSERT INTO rates_db.mart_daily_rates
WITH aggregated_facts AS (
    SELECT 
        date, 
        target_currency AS currency_code, 
        round(avg(avg_rate), 6) AS daily_avg_rate 
    FROM rates_db.fact_rates 
    GROUP BY date, target_currency
),
with_previous_day AS (
    SELECT 
        date, 
        currency_code, 
        daily_avg_rate AS avg_rate, 
        lagInFrame(daily_avg_rate) OVER (PARTITION BY currency_code ORDER BY date) AS prev_rate
    FROM aggregated_facts
)
SELECT 
    w.date, 
    w.currency_code, 
    coalesce(d.currency_name, 'Unknown Currency') AS currency_name,
    w.avg_rate,
    if(w.prev_rate = 0 OR isNull(w.prev_rate), 0.0, round((w.avg_rate - w.prev_rate) / w.prev_rate * 100, 2)) AS rate_change_pct
FROM with_previous_day w
-- Wrap FINAL in a subquery to avoid alias syntax errors in JOINs
LEFT JOIN (SELECT currency_code, currency_name FROM rates_db.dim_currencies FINAL) AS d 
    ON w.currency_code = d.currency_code
WHERE (w.date, w.currency_code) NOT IN (SELECT date, currency_code FROM rates_db.mart_daily_rates);