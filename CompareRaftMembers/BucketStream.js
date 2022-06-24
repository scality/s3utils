const async = require('async');
const http = require('http');
const stream = require('stream');

const { versioning } = require('arsenal');

const httpAgent = new http.Agent({
    keepAlive: true,
});

class BucketStream extends stream.Readable {
    constructor(params) {
        super({ objectMode: true });
        const {
            bucketdHost,
            bucketdPort,
            bucketName,
            marker,
            lastKey,
            retryDelayMs: userRetryDelayMs,
            maxRetryDelayMs: userMaxRetryDelayMs,
        } = params;
        this.bucketdHost = bucketdHost;
        this.bucketdPort = bucketdPort;
        this.bucketName = bucketName;
        this.marker = marker;
        this.lastKey = lastKey;
        const retryDelayMs = userRetryDelayMs || 1000;
        const maxRetryDelayMs = userMaxRetryDelayMs || 10000;
        this.retryParams = {
            times: 20,
            interval: retryCount => Math.min(
                // the first retry comes as "retryCount=2", hence substract 2
                this.retryDelayMs * (2 ** (retryCount - 2)),
                this.maxRetryDelayMs,
            ),
        };
        // for filtering seen master keys from listing
        if (marker && marker.indexOf('\0') === -1) {
            // if the marker resembles a master key, record it as the
            // master key just seen, like if we listed that key first,
            // to ensure consistency with the skipping algorithm
            this.lastMasterKey = marker;
        } else {
            this.lastMasterKey = null;
        }
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
                    // bucket/object does not exist: return no contents
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

    _preprocessItem(item) {
        if (item.key.startsWith(versioning.VersioningConstants.DbPrefixes.Replay)) {
            return null;
        }
        const vidSepPos = item.key.lastIndexOf('\0');
        const md = JSON.parse(item.value);
        if (md.isPHD) {
            // object is a Place Holder Delete (PHD)
            return null;
        }
        if (vidSepPos === -1) {
            this.lastMasterKey = item.key;
        } else {
            const masterKey = item.key.slice(0, vidSepPos);
            if (masterKey === this.lastMasterKey) {
                // we have already processed this versioned key as the
                // master key, so skip it, but reset the state to not
                // skip further versions of the same key
                this.lastMasterKey = null;
                return null;
            }
        }
        return md;
    }

    _listMore() {
        let url = `/default/bucket/${this.bucketName}`;
        if (this.marker) {
            url += `?marker=${encodeURIComponent(this.marker)}`;
        }
        return async.retry(
            this.retryParams,
            done => this._requestWrapper({ method: 'GET', path: url }, done),
            (err, response) => {
                if (err) {
                    // error after retries: destroy the stream
                    return this.destroy(err);
                }
                if (!response) {
                    return this.push(null);
                }
                const parsedBody = JSON.parse(response.body);
                const { Contents, IsTruncated } = parsedBody;
                let ended = false;
                return async.mapLimit(Contents, 10, (item, itemCb) => {
                    if (this.lastKey && item.key > this.lastKey) {
                        ended = true;
                        return itemCb(null, null);
                    }
                    const metadata = this._preprocessItem(item);
                    if (!metadata) {
                        return itemCb(null, null);
                    }
                    if (metadata.location !== undefined) {
                        return itemCb(null, item);
                    }
                    // need to fetch the object metadata to retrieve
                    // the full location array
                    return this._fetchObjectMetadata(item.key, (err, completeMetadata) => {
                        if (err) {
                            return itemCb(err);
                        }
                        if (!completeMetadata) {
                            // can happen if the object was not found,
                            // just skip it in that case
                            return itemCb(null, null);
                        }
                        return itemCb(null, { key: item.key, value: completeMetadata });
                    });
                }, (err, listing) => {
                    if (err) {
                        // there was an error fetching metadata after
                        // retries: destroy the stream
                        return this.destroy(err);
                    }
                    for (const item of listing) {
                        if (item) {
                            const fullKey = `${this.bucketName}/${item.key}`;
                            this.push({
                                key: fullKey,
                                value: item.value,
                            });
                        }
                    }
                    if (IsTruncated && !ended) {
                        this.marker = Contents[Contents.length - 1].key;
                    } else {
                        this.push(null);
                    }
                    return undefined;
                });
            },
        );
    }

    _fetchObjectMetadata(objectKey, callback) {
        const url = `/default/bucket/${this.bucketName}/${encodeURIComponent(objectKey)}`;
        return async.retry(
            this.retryParams,
            done => this._requestWrapper({ method: 'GET', path: url }, done),
            (err, response) => {
                if (err) {
                    return callback(err);
                }
                if (!response) {
                    // the object was not found
                    return callback();
                }
                return callback(null, response.body);
            },
        );
    }

    _read() {
        return this._listMore();
    }
}

module.exports = BucketStream;
