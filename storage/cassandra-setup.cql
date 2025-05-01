#DROP TABLE IF EXISTS fintech.aggregated_trades;

CREATE TABLE IF NOT EXISTS fintech.aggregated_trades (
    symbol text,
    window_start timestamp,
    window_end timestamp,
    avg_price double,
    volume_sum double,
    trade_count bigint,
    PRIMARY KEY ((symbol, window_start), window_end)
);

CREATE TABLE IF NOT EXISTS fintech.raw_trades (
    symbol text,
    timestamp timestamp,
    price double,
    volume double,
    PRIMARY KEY ((symbol), timestamp)
);