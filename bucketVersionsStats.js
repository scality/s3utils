const fs = require('fs');
const { http, https } = require('httpagent');

const { S3Client, ListObjectVersionsCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const { ConfiguredRetryStrategy } = require('@smithy/util-retry');

const { Logger } = require('werelogs');

const parseOlderThan = require('./utils/parseOlderThan');

const log = new Logger('s3utils::bucketVersionsStats');
const { ENDPOINT } = process.env;
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { BUCKET } = process.env;
const { TARGET_PREFIX } = process.env;
const LOG_PROGRESS_INTERVAL_MS = Number.parseInt(process.env.LOG_PROGRESS_INTERVAL || 10, 10) * 1000;
const LISTING_LIMIT = Number.parseInt(process.env.LISTING_LIMIT || 1000, 10);
const { KEY_MARKER } = process.env;
const { VERSION_ID_MARKER } = process.env;
const { HTTPS_CA_PATH } = process.env;
const { HTTPS_NO_VERIFY } = process.env;
const { OLDER_THAN } = process.env;
const VERBOSE = !!process.env.VERBOSE;
const AWS_SDK_REQUEST_RETRIES = 100;
const AWS_SDK_REQUEST_INITIAL_DELAY_MS = 30;

const USAGE = `
bucketVersionsStats.js

This script gathers and displays statistics about current and
non-current versions of objects in a given bucket.

Usage:
    node bucketVersionsStats.js

Mandatory environment variables:
    ENDPOINT: S3 endpoint URL
    ACCESS_KEY: S3 account/user access key
    SECRET_KEY: S3 account/user secret key
    BUCKET: S3 bucket name

Optional environment variables:
    TARGET_PREFIX: gather stats only inside this key prefix
    LISTING_LIMIT: number of keys to list per listing request (default 1000)
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default 10)
    KEY_MARKER: start counting from a specific key
    VERSION_ID_MARKER: start counting from a specific version ID
    HTTPS_CA_PATH: path to a CA certificate bundle used to authentify
    the S3 endpoint
    HTTPS_NO_VERIFY: set to 1 to disable S3 endpoint certificate check
    VERBOSE: set to a non-empty value to enable logging of individual version info
    OLDER_THAN: only count versions older than this date
        set this as an ISO date, a number of days, or a number of seconds e.g.,
        - setting to "2022-11-30T00:00:00Z" counts objects created/modified before Nov 30th 2022
        - setting to "30 days" counts objects created/modified more than 30 days ago
        - setting to "30 seconds" counts objects created/modified more than 30 seconds ago
`;

// We accept console statements for usage purpose
/* eslint-disable no-console */
['ENDPOINT', 'ACCESS_KEY', 'SECRET_KEY', 'BUCKET'].forEach(envVar => {
    if (!process.env[envVar]) {
        console.error(`Missing mandatory environment variable ${envVar}`);
        console.error(USAGE);
        process.exit(1);
    }
});
const s3EndpointIsHttps = ENDPOINT.startsWith('https:');

let _OLDER_THAN_TIMESTAMP;
if (OLDER_THAN) {
    _OLDER_THAN_TIMESTAMP = parseOlderThan(OLDER_THAN);
    if (Number.isNaN(_OLDER_THAN_TIMESTAMP.getTime())) {
        console.error('OLDER_THAN is not valid');
        console.error(USAGE);
        process.exit(1);
    }
}

/* eslint-enable no-console */
log.info('Start listing bucket for gathering versions statistics', {
    bucket: BUCKET,
    prefix: TARGET_PREFIX,
    endpoint: ENDPOINT,
});

let agent;
if (s3EndpointIsHttps) {
    agent = new https.Agent({
        keepAlive: true,
        ca: HTTPS_CA_PATH ? fs.readFileSync(HTTPS_CA_PATH) : undefined,
        rejectUnauthorized: HTTPS_NO_VERIFY !== '1',
    });
} else {
    agent = new http.Agent({ keepAlive: true });
}

const s3 = new S3Client({
    credentials: {
        accessKeyId: ACCESS_KEY,
        secretAccessKey: SECRET_KEY,
    },
    endpoint: ENDPOINT,
    region: 'us-east-1',
    forcePathStyle: true,
    tls: s3EndpointIsHttps,
    requestHandler: new NodeHttpHandler({
        httpAgent: agent,
        httpsAgent: agent,
        requestTimeout: 60000,
    }),
    retryStrategy: new ConfiguredRetryStrategy(
        AWS_SDK_REQUEST_RETRIES,
        // Custom backoff with exponential delay capped at 1mn max
        // between retries, and a little added jitter
        attempt => Math.min(
            AWS_SDK_REQUEST_INITIAL_DELAY_MS * 2 ** attempt,
            60000
        ) * (0.9 + Math.random() * 0.2)
    ),
});

const stats = {
    current: {
        count: 0n,
        size: 0n,
    },
    noncurrent: {
        count: 0n,
        size: 0n,
    },
};

let KeyMarker;
let VersionIdMarker;

function _logProgress(message) {
    const loggedStats = {
        total: {
            count: (stats.current.count + stats.noncurrent.count).toString(),
            size: (stats.current.size + stats.noncurrent.size).toString(),
        },
        current: {
            count: stats.current.count.toString(),
            size: stats.current.size.toString(),
        },
        noncurrent: {
            count: stats.noncurrent.count.toString(),
            size: stats.noncurrent.size.toString(),
        },
    };
    log.info(message, {
        bucket: BUCKET,
        prefix: TARGET_PREFIX,
        stats: loggedStats,
        keyMarker: KeyMarker,
        versionIdMarker: VersionIdMarker,
    });
}

const logProgressInterval = setInterval(
    () => _logProgress('progress update'),
    LOG_PROGRESS_INTERVAL_MS,
);

async function listBucket(bucket) {
    let NextKeyMarker = KEY_MARKER;
    let NextVersionIdMarker = VERSION_ID_MARKER;
    
    while (true) {
        KeyMarker = NextKeyMarker;
        VersionIdMarker = NextVersionIdMarker;
        
        const command = new ListObjectVersionsCommand({
            Bucket: bucket,
            MaxKeys: LISTING_LIMIT,
            Prefix: TARGET_PREFIX,
            KeyMarker,
            VersionIdMarker,
        });
        
        try {
            const data = await s3.send(command);
            const versions = data.Versions || [];
            for (const version of versions) {
                if (_OLDER_THAN_TIMESTAMP) {
                    const parsed = new Date(version.LastModified);
                    if (Number.isNaN(parsed.getTime()) || parsed > _OLDER_THAN_TIMESTAMP) {
                        continue;
                    }
                }
                const statObj = version.IsLatest ? stats.current : stats.noncurrent;
                statObj.count += 1n;
                statObj.size += BigInt(version.Size || 0);
                if (VERBOSE) {
                    log.info('version info', {
                        bucket: BUCKET,
                        key: version.Key,
                        versionId: version.VersionId,
                        isLatest: version.IsLatest,
                        lastModified: version.LastModified,
                        size: version.Size,
                    });
                }
            }
            
            NextKeyMarker = data.NextKeyMarker;
            NextVersionIdMarker = data.NextVersionIdMarker;
            
            if (!NextKeyMarker && !NextVersionIdMarker) {
                break;
            }
        } catch (error) {
            log.error('error listing object versions', {
                bucket,
                keyMarker: KeyMarker,
                versionIdMarker: VersionIdMarker,
                error,
                errorName: error.name,
                errorMessage: error.message,
            });
            throw error;
        }
    }
}

function shutdown(exitCode) {
    agent.destroy();
    clearInterval(logProgressInterval);
    process.exit(exitCode);
}

async function main() {
    try {
        await listBucket(BUCKET);
        _logProgress('final summary');
        shutdown(0);
    } catch (error) {
        log.error('error during execution', {
            bucket: BUCKET,
            KeyMarker,
            VersionIdMarker,
            error,
        });
        _logProgress('summary after error');
        shutdown(1);
    }
}

main();

function stop() {
    log.warn('stopping execution');
    shutdown(1);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
