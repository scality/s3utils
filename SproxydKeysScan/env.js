const env = {
    BUCKETD_HOSTPORT: process.env.BUCKETD_HOSTPORT,
    SPROXYD_HOSTPORT: process.env.SPROXYD_HOSTPORT,
    RAFT_SESSION_ID: process.env.RAFT_SESSION_ID,
    RAFT_LOG_BATCH_SIZE: process.env.RAFT_LOG_BATCH_SIZE || 1000,
    LOOKBACK_WINDOW: process.env.LOOKBACK_WINDOW || 10000,
    LOG_LEVEL: process.env.LOG_LEVEL || 'info',
    DUMP_LEVEL: process.env.DUMP_LEVEL || 'error',
    LOG_INTERVAL: process.env.LOG_INTERVAL || 300,
};

module.exports = { env };
