const { whilst, waterfall } = require('async');
const { httpRequest } = require('../repairDuplicateVersions');
const { SproxydKeysProcessor } = require('./DuplicateKeysWindow');
// const { subscribers } = require('./SproxydKeysSubscribers');
const { Logger } = require('werelogs');
const log = new Logger('s3utils:DuplicateKeysIngestion');

const {
    BUCKETD_HOSTPORT,
    // RAFT_SESSIONS,
    DUPLICATE_KEYS_WINDOW_SIZE,
} = process.env;

// in order to send keys to DuplicateKeysWindow, we need to read the raft journals
// furthermore, there should be only one of this service across all servers. Ballot should be useful here
class RaftJournalReader {
    constructor(begin, limit, sessionId, subscribers) {
        this.begin = begin;
        this.limit = limit;
        this.url = this._getJournalUrl(sessionId);
        this.processor = new SproxydKeysProcessor(
            DUPLICATE_KEYS_WINDOW_SIZE,
            subscribers
        );
    }

    _getJournalUrl(sessionId) {
        return `http://${BUCKETD_HOSTPORT}/_/raft_sessions/${sessionId}`;
    }

    getBatch(cb) {
        const requestUrl = `${this.url}/log?begin=${this.begin}&limit=${this.limit}&targetLeader=False`;
        return httpRequest('GET', requestUrl, (err, res) => {
            if (err) {
                return cb(err);
            }
            if (res.statusCode !== 200) {
                return cb(new Error(`GET ${requestUrl} returned status ${res.statusCode}`));
            }
            const body = JSON.parse(res.body);
            return cb(null, body);
        });
    }

    processBatch(body, cb) {
        // {masterKey, sproxydKeys}
        const extractedKeys = [];

        body.log.forEach(log => {
            log.entries.forEach(entry => {
                const masterKey = entry.key.split('\0')[0];
                const sproxydKeys = JSON.parse(entry.value).location.map(loc => loc.key);
                extractedKeys.push({ masterKey, sproxydKeys });
            });
        });
        log.info('processBatch succeeded');
        return cb(null, extractedKeys);
    }

    updateStatus(extractedKeys, cb) {
        extractedKeys.forEach(entry => {
            this.processor.insert(entry.masterKey, entry.sproxydKeys);
        });
        this.begin += this.limit;
        log.info('updateStatus succeeded');
        return cb();
    }

    runOnce() {
        return waterfall([
            next => this.getBatch(
                (err, res) => {
                    if (err) {
                        log.error('in getBatch', { error: err.message });
                    } else {
                        next(null, res);
                    }
                }),
            (body, next) => this.processBatch(body,
                (err, extractedKeys) => {
                    if (err) {
                        log.error('in processBatch', { error: err.message });
                    } else {
                        next(null, extractedKeys);
                    }
                }),
            extractedKeys => this.updateStatus(extractedKeys),
        ], (err, next) => {
            if (err) {
                log.error('error in RaftJournalEntries:run', { error: err.message });
            }
            return next();
        });
    }

    run() {
        whilst(
            () => true,
            this.runOnce()
        );
    }
}


// TODO: Optionally get initial lookback window of ~30 seconds and populate the map before continuing.
// Otherwise, start from the last cseq. One reader should be intialized per raft session in its own process.

function stop() {
    log.info('stopping raft journal ingestion');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);

module.exports = { RaftJournalReader };
