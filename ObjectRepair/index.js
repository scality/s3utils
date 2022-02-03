/* eslint-disable no-console */
const { RaftJournalReader } = require('./DuplicateKeysIngestion');
const { getSproxydAlias } = require('../repairDuplicateVersionsSuite');
const { ProxyLoggerCreator, AggregateLogger } = require('./Logging');
const werelogs = require('werelogs');
const { env } = require('./env');

const loggerConfig = {
    logLevel: env.OBJECT_REPAIR_LOG_LEVEL,
    dumpLevel: env.OBJECT_REPAIR_DUMP_LEVEL,
};

werelogs.configure(loggerConfig);
const aggregator = new AggregateLogger(env.OBJECT_REPAIR_LOG_INTERVAL);
aggregator.run();

const log = new ProxyLoggerCreator(new werelogs.Logger('s3utils:ObjectRepair:run'));

const USAGE = `
This script continously polls the Raft Journal of a given Raft session id.
When a duplicate sproxyd key is found for objects with different versioned object keys, 
a repair is done by creating new sproxyd keys.

Usage:
    node ObjectRepair

Mandatory environment variables:
    OBJECT_REPAIR_BUCKETD_HOSTPORT: <bucketd_host>:<bucketd_port>
    OBJECT_REPAIR_SPROXYD_HOSTPORT: <sproxyd_host>:<sproxyd_port>
    OBJECT_REPAIR_RAFT_SESSION_ID: Session id from which to read Journal.
    OBJECT_REPAIR_RAFT_LOG_BATCH_SIZE: Number of records to fetch at each poll of the Raft Journal.
    OBJECT_REPAIR_DUPLICATE_KEYS_WINDOW_SIZE: Max unique sproxydkeys that the Map will store.

Optional environment variables:
    OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ: offset to begin scanning from. Leave this out if you wish to begin 
        at latest cseq - OBJECT_REPAIR_LOOKBACK_WINDOW
    OBJECT_REPAIR_LOOKBACK_WINDOW: When the process is started/restarted it will begin at 
        cseq - OBJECT_REPAIR_LOOKBACK_WINDOW unless OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ is set explicitly. 
    OBJECT_REPAIR_LOG_LEVEL: defaults to info
    OBJECT_REPAIR_DUMP_LEVEL: defaults to error
    OBJECT_REPAIR_LOG_INTERVAL: logs summary of events in the given interval - defaults to 5 minutes
    OBJECT_REPAIR_TLS_KEY_PATH: path to TLS key file
    OBJECT_REPAIR_TLS_CERT_PATH: path to TLS cert file
    OBJECT_REPAIR_TLS_CA_PATH: path to TLS alternate CA file
`;
for (const [key, value] of Object.entries(env)) {
    if (!value) {
        log.info(`${key} must be defined`);
        console.error(USAGE);
        process.exit(1);
    }
}

env.OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ = process.env.OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ;

/**
 * Creates new reader and runs until stop().
 * @returns {undefined}
 */
function runJournalReader() {
    if (env.OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ === undefined) {
        log.info('OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ is not defined.' +
        'Ingestion will start at latest cseq - OBJECT_REPAIR_LOOKBACK_WINDOW');
    }
    const reader = new RaftJournalReader(
        Number.parseInt(env.OBJECT_REPAIR_RAFT_LOG_BEGIN_SEQ, 10),
        Number.parseInt(env.OBJECT_REPAIR_RAFT_LOG_BATCH_SIZE, 10),
        Number.parseInt(env.OBJECT_REPAIR_RAFT_SESSION_ID, 10)
    );
    reader.run();
}
/**
 * stops polling upon exit or container shutdown.
 * @returns {undefined}
 */
function stop() {
    log.info('stopping scan');
    process.exit(0);
}

/**
 * Fetches sproxyd alias for the environment before starting polling.
 * @returns {undefined}
 */
function main() {
    getSproxydAlias(() => {
        runJournalReader();
    });
}

main();

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);

module.exports = { env };

