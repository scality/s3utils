const { waterfall } = require('async');
const { httpRequest } = require('../repairDuplicateVersionsSuite');
const { SproxydKeysProcessor } = require('./DuplicateKeysWindow');
const { subscribers } = require('./SproxydKeysSubscribers');
const { Logger } = require('werelogs');
const log = new Logger('s3utils:DuplicateKeysIngestion');

const {
    BUCKETD_HOSTPORT,
    DUPLICATE_KEYS_WINDOW_SIZE,
    LOOKBACK_WINDOW,
} = process.env;

/**
 * @class
 * @classdesc In order to send keys to DuplicateKeysWindow, we need to read the raft journals.
 * There is only one of this service per raft session across all servers. Ballot is used
 * to coordinate the readers
 */
class RaftJournalReader {
    constructor(begin, limit, sessionId) {
        this.lookBack = LOOKBACK_WINDOW;
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

    setBegin(cb) {
        if (this.begin) {
            return cb(null);
        }
        // fetch one record to get cseq
        const requestUrl = `${this.url}/log?begin=${1}&limit=${1}&targetLeader=False`;
        return this._httpRequest('GET', requestUrl, null, (err, res) => {
            if (err) {
                log.error('unable to fetch cseq', { err });
                return cb(err);
            }
            const body = JSON.parse(res.body);
            this._setCseq(body);

            // make sure begin is at least 1
            this.begin = Math.max(1, this.cseq - this.lookBack);
            log.info(`initial begin: ${this.begin}`);
            return cb(null);
        });
    }

    getBatch(cb) {
        const requestUrl = `${this.url}/log?begin=${this.begin}&limit=${this.limit}&targetLeader=False`;
        return this._httpRequest('GET', requestUrl, null, (err, res) => {
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

        // { objectKey, sproxydKeys }
        const extractedKeys = [];

        body.log.forEach(record => {
            if (record.method === 8) {
                record.entries.forEach(entry => {
                    if (!entry.value) {
                        return;
                    }
                    const objectKey = entry.key;
                    if (!objectKey.includes('\u0000')) {
                        // skip non-versioned objectKeys
                        return;
                    }

                    const bucket = record.db;

                    let json = null;
                    try {
                        json = JSON.parse(entry.value);
                    } catch (err) {
                        log.error('json corrupted');
                        return;
                    }

                    // skip empty objects
                    if (!json.location || !Array.isArray(json.location)) {
                        return;
                    }
                    const sproxydKeys = json.location.map(loc => loc.key);
                    extractedKeys.push({ objectKey, sproxydKeys, bucket });
                });
            }
        });
        log.info('processBatch succeeded');
        return cb(null, extractedKeys);
    }

    updateStatus(extractedKeys, cb) {
        for (const entry of extractedKeys) {
            try {
                this.processor.insert(entry.objectKey, entry.sproxydKeys, entry.bucket);
            } catch (err) {
                log.error('insert key failed in updateStatus', { err, entry });
                return cb(err, null);
            }
        }

        // if we go over cseq, start at cseq + 1 while waiting for new raft journal entries
        this.begin = Math.min(this.limit + this.begin, this.cseq + 1);
        log.info('updateStatus succeeded');
        return cb(null, true);
    }

    runOnce(cb) {
        return waterfall([
            next => this.setBegin(
                err => {
                    if (err) {
                        log.error('in setBegin', { error: err.message });
                        next(err);
                    } else {
                        next(null);
                    }
                }),
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
                log.error('Retrying in 5 seconds', { error: err });
            }
            setTimeout(() => context.run(), timeout);
        });
    }
}

module.exports = { RaftJournalReader };
