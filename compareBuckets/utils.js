const http = require('http');
const async = require('async');
const { URL } = require('url');

const { jsutil } = require('arsenal');

const httpAgent = new http.Agent({
    keepAlive: true,
});

// TODO: remove duplication
function httpRequest(method, url, cb) {
    const cbOnce = jsutil.once(cb);
    const urlObj = new URL(url);
    const req = http.request({
        hostname: urlObj.hostname,
        port: urlObj.port,
        path: `${urlObj.pathname}${urlObj.search}`,
        method,
        agent: httpAgent,
    }, res => {
        if (method === 'HEAD') {
            return cbOnce(null, res);
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.once('end', () => {
            const body = chunks.join('');
            // eslint-disable-next-line no-param-reassign
            res.body = body;
            return cbOnce(null, res);
        });
        res.once('error', err => cbOnce(new Error(
            'error reading response from HTTP request '
                + `to ${url}: ${err.message}`
        )));
        return undefined;
    });
    req.once('error', err => cbOnce(new Error(
        `error sending HTTP request to ${url}: ${err.message}`
    )));
    req.end();
}

function getObjectURL(bucket, objectKey) {
    if (!bucket) {
        return 's3://';
    }
    if (!objectKey) {
        return `s3://${bucket}`;
    }
    return `s3://${bucket}/${encodeURI(objectKey)}`;
}

function getBucketdURL(hostPort, params) {
    let baseURL = `http://${hostPort}/default/bucket/${params.Bucket}`;
    if (params.Key !== undefined) {
        baseURL += `/${encodeURIComponent(params.Key)}`;
    }
    const queryParams = [];
    if (params.MaxKeys) {
        queryParams.push(`maxKeys=${params.MaxKeys}`);
    }
    if (params.KeyMarker !== undefined) {
        queryParams.push(`marker=${encodeURIComponent(params.KeyMarker)}`);
    }
    return `${baseURL}${queryParams.length > 0 ? '?' : ''}${queryParams.join('&')}`;
}

function listBucketMasterKeys(params, cb) {
    const { bucket, marker, hostPort, maxKeys, workers } = params;
    const url = getBucketdURL(hostPort, {
        Bucket: bucket,
        MaxKeys: maxKeys,
        KeyMarker: marker,
    });
    // TODO: get to a maxKeys number of master keys
    httpRequest('GET', url, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
        }
        const resp = JSON.parse(res.body);
        const { Contents, IsTruncated } = resp;

        return async.filterLimit(Contents, workers, (item, itemDone) => {
            const isMaster = item.key.lastIndexOf('\0') === -1;
            return itemDone(null, isMaster);
        }, (err, filtered) => {
            let marker = '';
            if (IsTruncated) {
                marker = Contents[Contents.length - 1].key;
            }
            cb(null, IsTruncated, marker, filtered);
        });
    });
}

module.exports = {
    httpRequest,
    listBucketMasterKeys,
    getBucketdURL,
    getObjectURL,
};
