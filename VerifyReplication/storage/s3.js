/* eslint-disable no-console */

const AWS = require('aws-sdk');
const fs = require('fs');
const http = require('http');
const https = require('https');

const { defaults } = require('../constants');

function getClient(params) {
    const {
        endpoint,
        accessKey,
        secretKey,
        region,
        httpsCaPath,
        httpsNoVerify,
        httpTimeout,
        showClientLogsIfAvailable,
        log,
    } = params;
    const s3EndpointIsHttps = (endpoint && endpoint.startsWith('https:')) || false;
    let agent;
    let clientLogger;

    if (s3EndpointIsHttps) {
        agent = new https.Agent({
            keepAlive: true,
            ca: httpsCaPath ? fs.readFileSync(httpsCaPath) : undefined,
            rejectUnauthorized: httpsNoVerify !== '1',
        });
    } else {
        agent = new http.Agent({ keepAlive: true });
    }

    // enable/disable sdk logs
    if (showClientLogsIfAvailable) {
        // TODO: may be use werelogs
        clientLogger = console;
    }

    const options = {
        accessKeyId: accessKey,
        secretAccessKey: secretKey,
        endpoint,
        region,
        sslEnabled: s3EndpointIsHttps,
        s3ForcePathStyle: true,
        apiVersions: { s3: '2006-03-01' },
        signatureVersion: 'v4',
        signatureCache: false,
        httpOptions: {
            timeout: httpTimeout,
            agent,
        },
        logger: clientLogger,
    };

    /**
     *  Options specific to s3 requests
     *  `maxRetries` & `customBackoff` are set only to s3 requests
     *  default aws sdk retry count is 3 with an exponential delay of 2^n * 30 ms
     */
    const s3Options = {
        maxRetries: defaults.AWS_SDK_REQUEST_RETRIES,
        customBackoff: (retryCount, error) => {
            log.error('awssdk request error', { error, retryCount });
            // retry with exponential backoff delay capped at 60s max
            // between retries, and a little added jitter
            return Math.min(defaults.AWS_SDK_REQUEST_INITIAL_DELAY_MS
                * 2 ** retryCount, defaults.AWS_SDK_REQUEST_MAX_BACKOFF_LIMIT_MS)
                * (0.9 + Math.random() * 0.2);
        },
    };

    return new AWS.S3({ ...options, ...s3Options });
}

function getObjMd(params, cb) {
    const {
        client,
        bucket,
        key,
        versionId,
    } = params;

    if (!client || !bucket || !key) {
        const errMsg = `missing required params, ${params}`;
        return cb(new Error(errMsg));
    }

    return client.headObject({
        Bucket: bucket,
        Key: key,
        VersionId: versionId,
    }, (err, data) => {
        if (err) {
            return cb(err);
        }
        const {
            ContentLength,
            LastModified,
            VersionId,
            Metadata,
        } = data;
        const resp = {
            size: ContentLength,
            lastModified: LastModified,
            versionId: VersionId,
            md: Metadata,
        };
        return cb(null, resp);
    });
}

function listObjects(params, cb) {
    const {
        client,
        bucket,
        prefix,
        delimiter,
        // listAllVersions,
        listingLimit,
        nextContinuationToken,
        // nextKeyMarker,
        // nextVersionIdMarker,
    } = params;

    if (!client || !bucket) {
        const errMsg = `missing required params, ${params}`;
        return cb(new Error(errMsg));
    }

    // TODO: support listing all versions
    return client.listObjectsV2({
        Bucket: bucket,
        MaxKeys: listingLimit,
        Prefix: prefix,
        Delimiter: delimiter,
        ContinuationToken: nextContinuationToken,
    }, cb);
}

module.exports = {
    getClient,
    getObjMd,
    listObjects,
};
