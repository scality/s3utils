const { RaftJournalReader } = require('./DuplicateKeysIngestion');
const { getSproxydAlias } = require('../repairDuplicateVersionsSuite');
const { Logger } = require('werelogs');

const log = new Logger('s3utils:SproxydKeysScan:run');

const env = {
    SPROXYD_KEY_LIMIT: process.env.SPROXYD_KEY_LIMIT,
    RAFT_SESSION_ID: process.env.RAFT_SESSION_ID,
    LOOKBACK_WINDOW: process.env.LOOKBACK_WINDOW,
};

const USAGE = `
runSproxydKeyScan.js

This script continously polls the Raft Journal of a given Raft session id.
When a duplicate sproxyd key is found for objects with different versioned object keys, 
a repair is done by creating new sproxyd keys.

Usage:
    node runSproxydKeyScan.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: <bucketd_host>:<bucketd_port>
    SPROXYD_HOSTPORT: <sproxyd_host>:<sproxyd_port>
    SPROXYD_KEY_LIMIT: Number of objects to fetch at each poll of the Raft Journal.
    RAFT_SESSION_ID: Session id from which to read Journal.
    LOOKBACK_WINDOW: When the process is started/restarted it will begin at cseq - LOOKBACK_WINDOW 
        unless SPROXYD_KEY_BEGIN is set explicitly. 
    DUPLICATE_KEYS_WINDOW_SIZE: Max unique sproxydkeys that the Map will store.

Optional environment variables:
    SPROXYD_KEY_BEGIN: offset to begin scanning from. Leave this out if you wish to begin 
    at latest cseq - LOOKBACK_WINDOW
`;
for (const [key, value] of Object.entries(env)) {
    if (!value) {
        log.info(`${key} must be defined`);
        log.info(USAGE);
        process.exit(1);
    }
}

env.SPROXYD_KEY_BEGIN = process.env.SPROXYD_KEY_BEGIN;

/**
 * Creates new reader and runs until stop().
 * @returns {undefined}
 */
function runJournalReader() {
    if (env.SPROXYD_KEY_BEGIN === undefined) {
        log.info('SPROXYD_KEY_BEGIN is not defined. Ingestion will start at latest cseq - LOOKBACK_WINDOW');
    }
    const reader = new RaftJournalReader(
        env.SPROXYD_KEY_BEGIN,
        env.SPROXYD_KEY_LIMIT,
        env.RAFT_SESSION_ID
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
