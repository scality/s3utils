const { waterfall } = require('async');
const { httpRequest } = require('../repairDuplicateVersions');
const { SproxydKeysProcessor } = require('./DuplicateKeysWindow');
const { subscribers } = require('./SproxydKeysSubscribers');
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
    constructor(begin, limit, sessionId) {
        this.begin = begin;
        this.cseq = null;
        this.limit = limit;
        this.sessionId = sessionId;
        this.url = this._getJournalUrl(sessionId);
        this.processor = new SproxydKeysProcessor(
            DUPLICATE_KEYS_WINDOW_SIZE,
            subscribers
        );
        this._httpRequest = httpRequest;
    }

    _getJournalUrl(sessionId) {
        return `http://${BUCKETD_HOSTPORT}/_/raft_sessions/${sessionId}`;
    }

    _setCseq(body) {
        this.cseq = body.info.cseq;
    }

    getBatch(cb) {
        const requestUrl = `${this.url}/log?begin=${this.begin}&limit=${this.limit}&targetLeader=False`;
        return this._httpRequest('GET', requestUrl, (err, res) => {
            if (err) {
                return cb(err);
            }
            if (!res || !res.body) {
                return cb(new Error(`GET ${requestUrl} returned empty body`));
            }

            if (res.statusCode !== 200) {
                return cb(new Error(`GET ${requestUrl} returned status ${res.statusCode}`));
            }

            const body = JSON.parse(res.body);
            return cb(null, body);
        });
    }

    processBatch(body, cb) {
        this._setCseq(body);

        // {masterKey, sproxydKeys}
        const extractedKeys = [];

        body.log.forEach(record => {
            if (record.method === 8) {
                record.entries.forEach(entry => {
                    const masterKey = entry.key.split('\0')[0];
                    if (!entry.value) {
                        return;
                    }

                    let json = null;
                    try {
                        json = JSON.parse(entry.value);
                    } catch (err) {
                        log.error('json corrupted');
                        return;
                    }

                    if (!json.location || !Array.isArray(json.location)) {
                        return;
                    }
                    const sproxydKeys = json.location.map(loc => loc.key);
                    extractedKeys.push({ masterKey, sproxydKeys });
                });
            }
        });
        log.info('processBatch succeeded');
        return cb(null, extractedKeys);
    }

    updateStatus(extractedKeys, cb) {
        extractedKeys.forEach(entry => {
            this.processor.insert(entry.masterKey, entry.sproxydKeys);
        });

        // if we go over cseq, start at cseq + 1 while waiting for new raft journal entries
        this.begin = Math.min(this.limit + this.begin, this.cseq + 1);
        log.info('updateStatus succeeded');
        return cb(null, true);
    }

    runOnce(cb) {
        return waterfall([
            next => this.getBatch(
                (err, res) => {
                    if (err) {
                        log.error('in getBatch', { error: err.message });
                        next(err);
                    } else {
                        next(null, res);
                    }
                }),
            (body, next) => this.processBatch(body,
                (err, extractedKeys) => {
                    if (err) {
                        log.error('in processBatch', { error: err.message });
                        next(err);
                    } else {
                        next(null, extractedKeys);
                    }
                }),
            (extractedKeys, next) => this.updateStatus(extractedKeys,
                (err, res) => {
                    if (err) {
                        log.error('in updateStatus', { error: err.message });
                        next(err);
                    } else {
                        next(null, res);
                    }
                }),
        ], err => {
            if (err) {
                cb(err, 5000);
            } else {
                cb(null, 0);
            }
        });
    }

    run() {
        const context = this;
        context.runOnce((err, timeout) => {
            if (err) {
                log.error('Error in runOnce. Retrying in 5 seconds', { error: err });
            }
            setTimeout(() => context.run(), timeout);
        });
    }
}


// TODO: Optionally get initial lookback window of ~30 seconds and populate the map before continuing.
// Otherwise, start from the last cseq. One reader should be intialized per raft session in its own process.

// function stop() {
//     log.info('stopping raft journal ingestion');
//     process.exit(0);
// }

// process.on('SIGINT', stop);
// process.on('SIGHUP', stop);
// process.on('SIGQUIT', stop);
// process.on('SIGTERM', stop);

module.exports = { RaftJournalReader };
