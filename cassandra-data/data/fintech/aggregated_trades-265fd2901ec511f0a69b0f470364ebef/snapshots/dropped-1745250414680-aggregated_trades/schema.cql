CREATE TABLE IF NOT EXISTS fintech.aggregated_trades (
    symbol text,
    window_start_bucket timestamp,
    window_start timestamp,
    avg_price double,
    trade_count bigint,
    volume_sum double,
    window_end timestamp,
    PRIMARY KEY ((symbol, window_start_bucket), window_start)
) WITH ID = 265fd290-1ec5-11f0-a69b-0f470364ebef
    AND CLUSTERING ORDER BY (window_start DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = 'default'
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
CREATE INDEX IF NOT EXISTS window_start_idx ON fintech.aggregated_trades (window_start);
