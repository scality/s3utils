const async = require('async');
const http = require('http');
const stream = require('stream');

const DEFAULT_REFRESH_PERIOD_MS = 5000;
const DEFAULT_MAX_RECORDS_PER_REQUEST = 1000;
const DEFAULT_RETRY_DELAY_MS = 1000;
const DEFAULT_MAX_RETRY_DELAY_MS = 10000;

const httpAgent = new http.Agent({
    keepAlive: true,
});

// this mapping comes from Metadata, see DbMethod in lib/protocol.json
const DbMethodToString = {
    0: 'CREATE',
    1: 'DELETE',
    2: 'GET',
    3: 'PUT',
    4: 'LIST',
    5: 'DEL',
    6: 'GET_ATTRIBUTES',
    7: 'PUT_ATTRIBUTES',
    8: 'BATCH',
    9: 'NOOP',
};

/**
 * @class RaftOplogStream
 *
 * @classdesc Readable stream that tails a raft session's oplog from
 * bucketd, and emits individual entries as stream data events, as
 * well as notifications when reaching the tail of the oplog.
 *
 * Each data event emitted is an object containing a unique "entry" attribute:
 * {null|object} entry
 *
 * When entry is an non-null object, it contains the following attributes:
 *     {string} method - database method converted to an upper-case
 *         string, e.g. "BATCH", see Metadata lib/protocol.json for
 *         the full list
 *     {string} bucket  - bucket name
 *     {string} [key]   - object key
 *     {string} [value] - object value
 *     {string} [type]  - 'del' refers to a delete operation in a batch
 *
 * When entry is null, it is a special event to notify that the tail
 * of the log has been reached for the time being. Later on, if there
 * are new events in the oplog, they will be emitted, and periodically
 * new null entries will be emitted as well.
 */
class RaftOplogStream extends stream.Readable {
    /**
     * @constructor
     * @param {object} params - constructor params
     * @param {string} params.bucketdHost - hostname or IP address of bucketd
     * @param {number} params.bucketdPort - bucketd API port
     * @param {number} params.raftSessionId - raft session ID to tail
     * @param {number} [params.startSeq=1] - start fetching oplog from this cseq
     * @param {number} [params.refreshPeriodMs=5000] - time in milliseconds
     * between two polls when reaching the oplog's tail
     * @param {number} [params.maxRecordsPerRequest=1000] - maximum number
     * of records to fetch in a single request to bucketd
     * @param {number} [params.retryDelayMs=1000] - initial delay in
     * milliseconds before retrying a failed request to bucketd
     * @param {number} [params.maxRetryDelayMs=10000] - maximum delay
     * in milliseconds between retry attempts of failed requests to
     * bucketd
     */
    constructor(params) {
        super({ objectMode: true });
        const {
            bucketdHost,
            bucketdPort,
            raftSessionId,
            startSeq,
            refreshPeriodMs,
            maxRecordsPerRequest,
            retryDelayMs: userRetryDelayMs,
            maxRetryDelayMs: userMaxRetryDelayMs,
        } = params;
        this.bucketdHost = bucketdHost;
        this.bucketdPort = bucketdPort;
        this.raftSessionId = raftSessionId;
        this.nextSeq = startSeq || 1;
        this.refreshPeriodMs = refreshPeriodMs || DEFAULT_REFRESH_PERIOD_MS;
        this.maxRecordsPerRequest = maxRecordsPerRequest || DEFAULT_MAX_RECORDS_PER_REQUEST;
        const retryDelayMs = userRetryDelayMs || DEFAULT_RETRY_DELAY_MS;
        const maxRetryDelayMs = userMaxRetryDelayMs || DEFAULT_MAX_RETRY_DELAY_MS;
        this.retryParams = {
            times: 20,
            interval: retryCount => Math.min(
                // the first retry comes as "retryCount=2", hence substract 2
                retryDelayMs * (2 ** (retryCount - 2)),
                maxRetryDelayMs,
            ),
        };
        this.fetchMoreTimer = null;
        this.canPush = false;
    }

    _read() {
        this.canPush = true;
        if (!this.fetchMoreTimer) {
            this._fetchMoreOplogEntries();
        }
    }

    _destroy() {
        clearTimeout(this.fetchMoreTimer);
    }

    _requestWrapper(reqParams, cb) {
        const req = http.request({
            hostname: this.bucketdHost,
            port: this.bucketdPort,
            agent: httpAgent,
            ...reqParams,
        }, res => {
            const chunks = [];
            res.on('data', chunk => chunks.push(chunk));
            res.once('end', () => {
                if (res.statusCode === 416) {
                    // log tail reached
                    return cb();
                }
                if (res.statusCode !== 200) {
                    return cb(new Error(`GET ${reqParams.path} returned status ${res.statusCode}`));
                }
                const body = chunks.join('');
                return cb(null, { statusCode: res.statusCode, body });
            });
            res.once('error', err => cb(err));
        });
        req.once('error', err => cb(err));
        req.end();
    }

    _onOplogTail() {
        this.push({ entry: null });
        this.fetchMoreTimer = setTimeout(
            () => this._fetchMoreOplogEntries(),
            this.refreshPeriodMs,
        );
    }

    _fetchMoreOplogEntries() {
        if (this.fetchMoreTimer) {
            clearTimeout(this.fetchMoreTimer);
            this.fetchMoreTimer = null;
        }
        if (!this.canPush) {
            return undefined;
        }
        return async.retry(
            this.retryParams,
            done => this._requestWrapper({
                method: 'GET',
                path: `/_/raft_sessions/${this.raftSessionId}/log`
                    + `?begin=${this.nextSeq}&limit=${this.maxRecordsPerRequest}`,
            }, done),
            (err, response) => {
                if (err) {
                    // error after retries: emit a stream error
                    return this.emit('error', err);
                }
                // we should wait for the next call to _read() before pushing more entries
                this.canPush = false;
                if (!response) {
                    return this._onOplogTail();
                }
                const parsedBody = JSON.parse(response.body);
                this.nextSeq += parsedBody.log.length;
                for (const record of parsedBody.log) {
                    const { db: bucket, method } = record;
                    for (const entry of record.entries) {
                        this.push({
                            entry: {
                                method: DbMethodToString[method],
                                bucket,
                                ...entry,
                            },
                        });
                    }
                }
                const { start, cseq } = parsedBody.info;
                if (start + parsedBody.log.length === cseq + 1) {
                    this._onOplogTail();
                }
                return undefined;
            },
        );
    }
}

module.exports = RaftOplogStream;
