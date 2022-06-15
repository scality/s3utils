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
            retryDelayMs,
            maxRetryDelayMs,
        } = params;
        this.bucketdHost = bucketdHost;
        this.bucketdPort = bucketdPort;
        this.bucketName = bucketName;
        this.marker = marker;
        this.lastKey = lastKey;
        this.retryDelayMs = retryDelayMs || 1000;
        this.maxRetryDelayMs = maxRetryDelayMs || 10000;

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
                    // bucket does not exist: return no contents
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

    _shouldOutputItem(item) {
        if (item.key.startsWith(versioning.VersioningConstants.DbPrefixes.Replay)) {
            return false;
        }
        const vidSepPos = item.key.lastIndexOf('\0');
        const md = JSON.parse(item.value);
        if (md.isPHD) {
            // object is a Place Holder Delete (PHD)
            return false;
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
                return false;
            }
        }
        return true;
    }

    _listMore() {
        let url = `/default/bucket/${this.bucketName}`;
        if (this.marker) {
            url += `?marker=${encodeURIComponent(this.marker)}`;
        }
        return async.retry(
            {
                times: 20,
                interval: retryCount => Math.min(
                    // the first retry comes as "retryCount=2", hence substract 2
                    this.retryDelayMs * (2 ** (retryCount - 2)),
                    this.maxRetryDelayMs,
                ),
            },
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
                for (const item of Contents) {
                    const fullKey = `${this.bucketName}/${item.key}`;
                    if (!this.lastKey || item.key <= this.lastKey) {
                        if (this._shouldOutputItem(item)) {
                            this.push({
                                key: fullKey,
                                value: item.value,
                            });
                        }
                    } else {
                        ended = true;
                        break;
                    }
                }
                if (IsTruncated && !ended) {
                    this.marker = Contents[Contents.length - 1].key;
                } else {
                    this.push(null);
                }
                return undefined;
            },
        );
    }

    _read() {
        return this._listMore();
    }
}

module.exports = BucketStream;
