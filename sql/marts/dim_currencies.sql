-- Using ReplacingMergeTree to ensure idempotency (no duplicates on re-runs)
CREATE TABLE IF NOT EXISTS rates_db.dim_currencies (
    currency_code String,
    currency_name String
) ENGINE = ReplacingMergeTree()
ORDER BY currency_code;

INSERT INTO rates_db.dim_currencies (currency_code, currency_name) VALUES
    ('USD', 'US Dollar'), ('EUR', 'Euro'), ('GBP', 'British Pound'), 
    ('JPY', 'Japanese Yen'), ('AUD', 'Australian Dollar'), ('CAD', 'Canadian Dollar'), 
    ('CHF', 'Swiss Franc'), ('CNY', 'Chinese Yuan'), ('BGN', 'Bulgarian Lev'), 
    ('BRL', 'Brazilian Real'), ('CZK', 'Czech Koruna'), ('DKK', 'Danish Krone'), 
    ('HKD', 'Hong Kong Dollar'), ('HUF', 'Hungarian Forint'), ('IDR', 'Indonesian Rupiah'), 
    ('ILS', 'Israeli New Shekel'), ('INR', 'Indian Rupee'), ('ISK', 'Icelandic Krona'), 
    ('KRW', 'South Korean Won'), ('MXN', 'Mexican Peso'), ('MYR', 'Malaysian Ringgit'), 
    ('NOK', 'Norwegian Krone'), ('NZD', 'New Zealand Dollar'), ('PHP', 'Philippine Peso'), 
    ('PLN', 'Polish Zloty'), ('RON', 'Romanian Leu'), ('SEK', 'Swedish Krona'), 
    ('SGD', 'Singapore Dollar'), ('THB', 'Thai Baht'), ('TRY', 'Turkish Lira'), 
    ('ZAR', 'South African Rand');
