const async = require('async');
const http = require('http');
const stream = require('stream');

const RaftOplogStream = require('./RaftOplogStream');

const DEFAULT_MAX_BUFFERED_ENTRIES = 10000;
const DEFAULT_RETRY_DELAY_MS = 1000;
const DEFAULT_MAX_RETRY_DELAY_MS = 10000;

const httpAgent = new http.Agent({
    keepAlive: true,
});

class DiffStreamOplogFilter extends stream.Transform {
    constructor(params) {
        super({ objectMode: true });
        const {
            bucketdHost,
            bucketdPort,
            maxBufferedEntries: userMaxBufferedEntries,
            retryDelayMs: userRetryDelayMs,
            maxRetryDelayMs: userMaxRetryDelayMs,
        } = params;
        this.bucketdHost = bucketdHost;
        this.bucketdPort = bucketdPort;
        this.maxBufferedEntries = userMaxBufferedEntries || DEFAULT_MAX_BUFFERED_ENTRIES;
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
        this.bucketNameToRaftSessionId = {};
        this._initRaftOplogStreams(params);

        this.nBufferedEntries = 0;
        this.pendingTransformCallback = null;
        this.pendingFlushCallback = null;
    }

    _initRaftOplogStreams(params) {
        const {
            bucketdHost, bucketdPort,
            excludeFromCseqs,
        } = params;

        this.raftSessionStates = {};
        for (const rsId of Object.keys(excludeFromCseqs)) {
            const raftSessionState = {};
            this.raftSessionStates[rsId] = raftSessionState;
            const oplogStream = new RaftOplogStream({
                bucketdHost,
                bucketdPort,
                raftSessionId: rsId,
                startSeq: excludeFromCseqs[rsId] + 1,
            });
            this._setupOplogStream(raftSessionState, oplogStream);
            raftSessionState.oplogStream = oplogStream;
            raftSessionState.oplogKeys = new Set();
            raftSessionState.inputBuffer = [];
        }
    }

    _setupOplogStream(raftSessionState, oplogStream) {
        // no 'end' event handling is required since the oplog streams never end
        oplogStream
            .on('data', data => this._onOplogEvent(raftSessionState, data))
            .on('error', err => {
                this.emit('error', err);
            });
    }

    _onOplogEvent(raftSessionState, data) {
        const { entry } = data;
        if (entry === null) {
            // a null entry means we reached the end of
            // the raft oplog, this is the point where
            // it becomes safe to process the raft
            // session's current input buffer
            this._processInputBuffer(raftSessionState);
        } else if (entry.method === 'BATCH') {
            // only consider events from BATCH records
            const fullKey = `${entry.bucket}/${entry.key}`;
            raftSessionState.oplogKeys.add(fullKey);
        }
    }

    _transform(diffEntry, encoding, callback) {
        const bucketName = this._extractBucketName(diffEntry);
        async.waterfall([
            next => {
                const rsId = this.bucketNameToRaftSessionId[bucketName];
                if (rsId !== undefined) {
                    return next(null, rsId);
                }
                return this._fetchBucketRaftSessionId(bucketName, next);
            },
        ], (err, rsId) => {
            if (err) {
                this.emit('error', err);
                return callback();
            }
            // bucket not found: ignore the entry
            if (rsId === null) {
                return callback();
            }
            const raftSessionState = this.raftSessionStates[rsId];
            // if for some reason the returned raft session ID is not
            // managed by us (e.g. a bucket has been recently
            // recreated with a different raft session ID), ignore the
            // entry
            if (!raftSessionState) {
                return callback();
            }
            raftSessionState.inputBuffer.push(diffEntry);
            this.nBufferedEntries += 1;
            if (this.nBufferedEntries !== this.maxBufferedEntries) {
                // we can accept more input
                return callback();
            }
            // wait until the current read buffer is flushed before
            // accepting more input
            this.pendingTransformCallback = callback;
            return undefined;
        });
    }

    _flush(callback) {
        if (this.nBufferedEntries === 0) {
            // end the output stream if there are no entries to forward
            this.push(null);
            return callback();
        }
        this.pendingFlushCallback = callback;
        return undefined;
    }

    _destroy() {
        for (const rsState of Object.values(this.raftSessionStates)) {
            rsState.oplogStream.destroy();
        }
    }

    /**
     * Extract the bucket name from a diff entry coming from the stream's input
     *
     * @param {object} diffEntry - entry coming from DiffStream
     * @return {string} - bucket name
     */
    _extractBucketName(diffEntry) {
        const nonNullEntry = diffEntry[0] || diffEntry[1];
        const { key: fullKey } = nonNullEntry;
        const slashIndex = fullKey.indexOf('/');
        const bucketName = fullKey.slice(0, slashIndex);
        return bucketName;
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
                if (res.statusCode === 404) {
                    return cb();
                }
                const body = chunks.join('');
                if (res.statusCode !== 200) {
                    return cb(new Error(`GET ${reqParams.path} returned status ${res.statusCode}`));
                }
                return cb(null, { statusCode: res.statusCode, body });
            });
            res.once('error', err => cb(err));
        });
        req.once('error', err => cb(err));
        req.end();
    }

    _fetchBucketRaftSessionId(bucketName, cb) {
        return async.retry(
            this.retryParams,
            done => this._requestWrapper({
                method: 'GET',
                path: `/_/buckets/${bucketName}/id`,
            }, done),
            (err, response) => {
                if (err) {
                    // error after retries
                    return cb(err);
                }
                // no response means the bucket does not exist
                // (assuming it has been recently deleted since it
                // appears in a diff entry)
                const rsId = response ? Number.parseInt(response.body, 10) : null;
                this.bucketNameToRaftSessionId[bucketName] = rsId;
                return cb(null, rsId);
            },
        );
    }

    _processInputBuffer(raftSessionState) {
        const { inputBuffer } = raftSessionState;
        // eslint-disable-next-line no-param-reassign
        raftSessionState.inputBuffer = [];
        this.nBufferedEntries -= inputBuffer.length;
        for (const diffEntry of inputBuffer) {
            const nonNullEntry = diffEntry[0] || diffEntry[1];
            const { key: fullKey } = nonNullEntry;
            // this is where the filtering occurs: only forward the
            // entry if its key hasn't been seen in the latest oplog
            // entries of the bucket's raft session
            if (!raftSessionState.oplogKeys.has(fullKey)) {
                this.push(diffEntry);
            }
        }
        const { pendingTransformCallback, pendingFlushCallback } = this;
        if (pendingTransformCallback) {
            // call the transform callback to accept more input
            this.pendingTransformCallback = null;
            pendingTransformCallback();
        }
        if (pendingFlushCallback && this.nBufferedEntries === 0) {
            // end the output stream if there are no more entries to forward
            this.push(null);
            this.pendingFlushCallback = null;
            pendingFlushCallback();
        }
    }
}

module.exports = DiffStreamOplogFilter;
