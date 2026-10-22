-- Latest description of each machine, from the agent's /v1/machine, matching
-- shared.machines.Machine. Joined to metrics and evaluations on machine_id.
-- observed_at is when the agent last described itself, so it doubles as
-- "last seen". No TTL, a machine that goes away keeps its row.
CREATE TABLE IF NOT EXISTS machines (
    machine_id    String,
    hostname      String,
    os            LowCardinality(String),
    os_version    LowCardinality(String),
    architecture  LowCardinality(String),
    cores         UInt16,
    total_memory  UInt64,
    total_disk    UInt64,
    last_boot     DateTime64(3, 'UTC'),
    observed_at   DateTime64(3, 'UTC'),
    ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY machine_id;
