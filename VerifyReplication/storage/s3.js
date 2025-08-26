const { S3Client, HeadObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const { ConfiguredRetryStrategy } = require('@smithy/util-retry');
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

    const httpAgent = new http.Agent({ keepAlive: true });
    const httpsAgent = new https.Agent({
        keepAlive: true,
        ca: httpsCaPath ? fs.readFileSync(httpsCaPath) : undefined,
        rejectUnauthorized: httpsNoVerify !== '1',
    });

    // Options specific to s3 requests - maxRetries & customBackoff
    // Default aws sdk retry count is 3 with an exponential delay of 2^n * 30 ms
    const retryStrategy = new ConfiguredRetryStrategy(
        defaults.AWS_SDK_REQUEST_RETRIES,
        retryCount => {
            // retry with exponential backoff delay capped at 60s max
            // between retries, and a little added jitter
            const backoff = Math.min(defaults.AWS_SDK_REQUEST_INITIAL_DELAY_MS
                * 2 ** retryCount, defaults.AWS_SDK_REQUEST_MAX_BACKOFF_LIMIT_MS)
                * (0.9 + Math.random() * 0.2);
            // show retry errors only if client logs are enabled as this may
            // increase log size!
            if (showClientLogsIfAvailable) {
                log.error('awssdk request error', {
                    retryCount,
                    backoff,
                });
            }
            return backoff;
        }
    );

    const clientConfig = {
        region,
        credentials: {
            accessKeyId: accessKey,
            secretAccessKey: secretKey,
        },
        endpoint,
        forcePathStyle: true,
        retryStrategy,
        requestHandler: new NodeHttpHandler({
            httpAgent,
            httpsAgent,
            requestTimeout: httpTimeout || 300000,
        }),
    };

    if (showClientLogsIfAvailable) {
        // TODO: consider using werelogs
        clientConfig.logger = console;
    }

    return new S3Client(clientConfig);
}

async function getObjMd(params, cb) {
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

    try {
        const commandParams = {
            Bucket: bucket,
            Key: key,
            VersionId: versionId
        };
        
        const data = await client.send(new HeadObjectCommand(commandParams));
        const resp = {
            size: data.ContentLength,
            lastModified: data.LastModified,
            versionId: data.VersionId,
            md: data.Metadata,
        };
        return cb(null, resp);
    } catch (err) {
        return cb(err);
    }
}

async function listObjects(params, cb) {
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

    try {
        // TODO: support listing all versions
        const data = await client.send(new ListObjectsV2Command({
            Bucket: bucket,
            MaxKeys: listingLimit,
            Prefix: prefix,
            Delimiter: delimiter,
            ContinuationToken: nextContinuationToken,
        }));
        return cb(null, data);
    } catch (err) {
        return cb(err);
    }
}

module.exports = {
    getClient,
    getObjMd,
    listObjects,
};
