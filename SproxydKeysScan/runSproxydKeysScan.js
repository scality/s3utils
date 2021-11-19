const { RaftJournalReader } = require('./DuplicateKeysIngestion');
const { getSproxydAlias } = require('../repairDuplicateVersionsSuite');
const { Logger } = require('werelogs');

const log = new Logger('s3utils:SproxydKeysScan:run');

const env = {
    SPROXYD_KEY_LIMIT: process.env.SPROXYD_KEY_LIMIT,
    RAFT_SESSION_ID: process.env.RAFT_SESSION_ID,
    LOOKBACK_WINDOW: process.env.LOOKBACK_WINDOW,
};

for (const [key, value] of Object.entries(env)) {
    if (!value) {
        log.info(`${key} must be defined`);
        process.exit(1);
    }
}

env.SPROXYD_KEY_BEGIN = process.env.SPROXYD_KEY_BEGIN;

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

function stop() {
    log.info('stopping scan');
    process.exit(0);
}

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
