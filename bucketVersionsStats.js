const fs = require('fs');
const { http, https } = require('httpagent');

const AWS = require('aws-sdk');
const { doWhilst } = require('async');

const { Logger } = require('werelogs');

const parseOlderThan = require('./utils/parseOlderThan');
const { safeListObjectVersions } = require('./utils/safeList');

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

const options = {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    endpoint: ENDPOINT,
    region: 'us-east-1',
    sslEnabled: s3EndpointIsHttps,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
    httpOptions: {
        timeout: 0,
        agent,
    },
};
/**
 *  Options specific to s3 requests
 *  `maxRetries` & `customBackoff` are set only to s3 requests
 *  default aws sdk retry count is 3 with an exponential delay of 2^n * 30 ms
 */
const s3Options = {
    maxRetries: AWS_SDK_REQUEST_RETRIES,
    customBackoff: (retryCount, error) => {
        log.error('aws sdk request error', { error, retryCount });
        // retry with exponential backoff delay capped at 1mn max
        // between retries, and a little added jitter
        return Math.min(AWS_SDK_REQUEST_INITIAL_DELAY_MS
                        * 2 ** retryCount, 60000)
            * (0.9 + Math.random() * 0.2);
    },
};
const s3 = new AWS.S3(Object.assign(options, s3Options));

const stats = {
    current: {
        count: 0,
        size: 0,
    },
    noncurrent: {
        count: 0,
        size: 0,
    },
};

let KeyMarker;
let VersionIdMarker;

function _logProgress(message) {
    const loggedStats = {
        total: {
            count: stats.current.count + stats.noncurrent.count,
            size: stats.current.size + stats.noncurrent.size,
        },
        ...stats,
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

function _listObjectVersions(bucket, KeyMarker, VersionIdMarker, cb) {
    return safeListObjectVersions(s3, {
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        Prefix: TARGET_PREFIX,
        KeyMarker,
        VersionIdMarker,
    }, cb);
}


function listBucket(bucket, cb) {
    let NextKeyMarker = KEY_MARKER;
    let NextVersionIdMarker = VERSION_ID_MARKER;
    return doWhilst(
        done => {
            KeyMarker = NextKeyMarker;
            VersionIdMarker = NextVersionIdMarker;
            _listObjectVersions(bucket, KeyMarker, VersionIdMarker, (err, data) => {
                if (err) {
                    log.error('error listing object versions', {
                        error: err,
                    });
                    return done(err);
                }
                for (const version of data.Versions) {
                    if (_OLDER_THAN_TIMESTAMP) {
                        const parsed = new Date(version.LastModified);
                        if (Number.isNaN(parsed.getTime()) || parsed > _OLDER_THAN_TIMESTAMP) {
                            continue;
                        }
                    }
                    const statObj = version.IsLatest ? stats.current : stats.noncurrent;
                    statObj.count += 1;
                    statObj.size += version.Size || 0;
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
                return done();
            });
        },
        () => {
            if (NextKeyMarker || NextVersionIdMarker) {
                return true;
            }
            KeyMarker = undefined;
            VersionIdMarker = undefined;
            return false;
        },
        cb,
    );
}

function shutdown(exitCode) {
    agent.destroy();
    clearInterval(logProgressInterval);
    process.exit(exitCode);
}

listBucket(BUCKET, err => {
    if (err) {
        log.error('error during execution', {
            bucket: BUCKET,
            KeyMarker,
            VersionIdMarker,
        });
        _logProgress('summary after error');
        shutdown(1);
    } else {
        _logProgress('final summary');
        shutdown(0);
    }
});

function stop() {
    log.warn('stopping execution');
    shutdown(1);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
