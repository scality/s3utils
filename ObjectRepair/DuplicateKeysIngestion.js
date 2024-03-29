const { waterfall } = require('async');
const { Logger } = require('werelogs');
const { httpRequest } = require('../repairDuplicateVersionsSuite');
const { SproxydKeysProcessor } = require('./DuplicateKeysWindow');
const { ProxyLoggerCreator, AggregateLogger } = require('./Logging');
const { subscribers } = require('./SproxydKeysSubscribers');
const { env } = require('./env');

const log = new ProxyLoggerCreator(new Logger('ObjectRepair:DuplicateKeysIngestion'));
/**
 * @class
 * @classdesc In order to send keys to DuplicateKeysWindow, we need to read the raft journals.
 * There is only one of this service per raft session across all servers. Ballot is used
 * to coordinate the readers
 */
class RaftJournalReader {
    /**
     *
     * @param {(number|undefined)} begin - The first cseq number to read from. Automatically set if undefined.
     * @param {number} limit - How many Raft Journal entries to fetch with each poll.
     * @param {number} sessionId - Raft Session Id that the reader will poll from.
     */
    constructor(begin, limit, sessionId) {
        this.lookBack = Number.parseInt(env.OBJECT_REPAIR_LOOKBACK_WINDOW, 10);
        this.aggregateLogger = new AggregateLogger(env.OBJECT_REPAIR_LOG_INTERVAL);
        this.begin = begin;
        this.cseq = null;
        this.limit = limit;
        this.sessionId = sessionId;
        this.url = this._getJournalUrl(sessionId);
        this.processor = new SproxydKeysProcessor(
            env.OBJECT_REPAIR_DUPLICATE_KEYS_WINDOW_SIZE,
            subscribers,
        );
        this._httpRequest = httpRequest;
        this._useHttps = (process.env.OBJECT_REPAIR_TLS_KEY_PATH !== undefined
                          && process.env.OBJECT_REPAIR_TLS_KEY_PATH !== ''
                          && process.env.OBJECT_REPAIR_TLS_CERT_PATH !== undefined
                          && process.env.OBJECT_REPAIR_TLS_CERT_PATH !== '');
    }

    /**
     * @param {string} sessionId - Raft session ID
     * @returns {string} - Url string for given Raft sesssion ID
     */
    _getJournalUrl(sessionId) {
        const protocol = this._useHttps ? 'https' : 'http';
        return `${protocol}://${env.OBJECT_REPAIR_BUCKETD_HOSTPORT}/_/raft_sessions/${sessionId}`;
    }

    /**
     * @param {Object} body - Paginated response body from JournalUrl/log
     * @returns {void}
     */
    _setCseq(body) {
        this.cseq = body.info.cseq;
    }

    /**
     * If this.begin is undefined, latest cseq is fetched and this.begin is set to this.cseq - this.lookBack
     * @param {function(err:Error)} cb - callback
     * @returns {undefined}
     * @callback called with (err:Error)
     */
    setBegin(cb) {
        if (this.begin) {
            return process.nextTick(cb);
        }
        // fetch one record to get cseq
        const requestUrl = `${this.url}/log?begin=1&limit=1&targetLeader=False`;
        return this._httpRequest('GET', requestUrl, null, (err, res) => {
            if (err) {
                log.error('unable to fetch cseq', { err, requestUrl });
                return cb(err);
            }

            const body = JSON.parse(res.body);

            this._setCseq(body);

            // make sure begin is at least 1 since Raft Journal logs are 1-indexed
            this.begin = Math.max(1, this.cseq - this.lookBack);
            log.info(`initial begin: ${this.begin}`);
            return cb();
        });
    }

    /**
    * @typedef {Object} LogObjectEntry
    * @property {string} key - ObjectKey which may be version or unversioned.
    * @property {string} value - All metadata for the object with the accompanying Sproxyd Key.
     */

    /**
     * @typedef {Object} RaftJournalLogObject
     * @property {string} db - source bucket name
     * @property {number} method - the type of operation that created the log object.
     * @property {Array.<LogObjectEntry>} entries - Array of LogObjectEntry
     */

    /**
     * @typedef {Object} RaftJournalBody
     * @property {number} info.start - the offset that this request started reading from.
     * @property {number} info.cseq - total number of logged objects in this raft session.
       @property {Array.<RaftJournalLogObject>} log - Array of RaftJournalLogObject
     */

    /**
     * Fetches RaftJournalBody
     * @param {function(err: Error, res: RaftJournalBody)} cb - callback
     * @returns {undefined}
     * @callback called with (err:Error, body:RaftJournalBody)
     */
    getBatch(cb) {
        const requestUrl = `${this.url}/log?begin=${this.begin}&limit=${this.limit}&targetLeader=False`;
        return this._httpRequest('GET', requestUrl, null, (err, res) => {
            if (err) {
                return cb(err);
            }

            if (!res) {
                return cb(new Error(`GET ${requestUrl} returned empty response at ${this.begin}`));
            }

            if (res.statusCode === 416) {
                return cb(new RangeError(`GET ${requestUrl} found no new records at ${this.begin}`));
            }

            if (res.statusCode !== 200) {
                return cb(new Error(`GET ${requestUrl} returned status ${res.statusCode} at ${this.begin}`));
            }

            if (!res.body) {
                return cb(new Error(`GET ${requestUrl} returned empty body at ${this.begin}`));
            }

            const body = JSON.parse(res.body);
            // FIXME this special case should be taken care of once S3C-3928 is fixed
            if (body.log.length === 0) {
                return cb(new RangeError(`GET ${requestUrl} found no new records at ${this.begin}`));
            }
            return cb(null, body);
        });
    }
    /**
     * @typedef {Object} ExtractedKey
     * @property {string} ObjectKey - unversioned object key
     * @property {Array.<string>} sproxydKeys - Array of sproxydKeys for the given object key
     */

    /**
     * Extracts Sproxyd Keys for a given object key
     * @param {RaftJournalBody} body - RaftJournalBody instance
     * @param {function(err: Error, res: Array.<ExtractedKey>)} cb - callback
     * @returns {undefined}
     * @callback called with (err:Error, extractedKeys:Array.<ExtractedKey>)
     */
    processBatch(body, cb) {
        this._setCseq(body);

        // { objectKey, sproxydKeys }
        const extractedKeys = [];

        body.log.forEach(record => {
            // method 8 is batch
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
                        log.error('json corrupted', {
                            begin: this.begin,
                            limit: this.limit,
                            eventMessage: 'corruptedJSON',
                        });
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
        log.debug('processBatch succeeded', {
            begin: this.begin,
            limit: this.limit,
            eventMessage: 'processBatchSuccess',
        });
        return cb(null, extractedKeys);
    }

    /**
     * Hands off keys to the processor and updates this.begin to mark next offset to read from.
     * @param {Array.<ExtractedKey>} extractedKeys - Array of ExtractedKey instances.
     * @param {function(err:Error, res:Boolean)} cb - callback. Response res is true if success.
     * @returns {undefined}
     * @callback called with (err:Error, res: Boolean).
     */
    updateStatus(extractedKeys, cb) {
        for (const entry of extractedKeys) {
            try {
                this.processor.insert(entry.objectKey, entry.sproxydKeys, entry.bucket);
            } catch (err) {
                log.error('insert key failed in updateStatus', { err, entry, eventMessage: 'insertKeyFailure' });
                return cb(err);
            }
        }
        const oldBegin = this.begin;
        // if we go over cseq, start at cseq + 1 while waiting for new raft journal entries
        this.begin = Math.min(this.limit + this.begin, this.cseq + 1);
        this.aggregateLogger.update('cseqProcessed', this.begin - oldBegin);
        log.debug('updateStatus succeeded', { eventMessage: 'batchProcessed' });
        return cb(null, true);
    }

    /**
     * Reads, processes, and updates status of one batch of objects from the the Raft Journal.
     * If an error occurs at any point in the process, or if there are no new objects to be processed
     * a timeout of 5 seconds is set before polling again.
     * @param {function(err:Error, res:number)} cb - callback. Response res is a timeout in milliseconds.
     * @returns {undefined}
     * @callback called with (err:Error, res:number)
     */

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
                },
            ),
            next => this.getBatch(
                (err, res) => {
                    if (err) {
                        if (!(err instanceof RangeError)) {
                            log.error('in getBatch', { error: err.message });
                        }
                        next(err);
                    } else {
                        next(null, res);
                    }
                },
            ),
            (body, next) => this.processBatch(
                body,
                (err, extractedKeys) => {
                    if (err) {
                        log.error('in processBatch', { error: err.message });
                        next(err);
                    } else {
                        next(null, extractedKeys);
                    }
                },
            ),
            (extractedKeys, next) => this.updateStatus(
                extractedKeys,
                (err, res) => {
                    if (err) {
                        log.error('in updateStatus', { error: err.message });
                        next(err);
                    } else {
                        next(null, res);
                    }
                },
            ),
        ], err => {
            if (err) {
                cb(err, 5000);
            } else {
                cb(null, 0);
            }
        });
    }

    /**
     * Continously runs runOnce after either a 0 or 5000 millisecond timeout.
     * 0 millisecond timeout is used when there are more Raft Journal Objects to scan (this.begin < this.cseq)
     * @returns {undefined}
     */
    run() {
        const context = this;
        context.runOnce((err, timeout) => {
            if (err) {
                if (err instanceof RangeError) {
                    log.debug(err.message, { eventMessage: 'noNewRecords' });
                } else {
                    log.error('Retrying in 5 seconds', { error: err.message, offset: this.begin });
                }
            }
            setTimeout(() => context.run(), timeout);
        });
    }
}

module.exports = { RaftJournalReader };
