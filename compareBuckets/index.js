/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const { Logger } = require('werelogs');

const { compareBuckets } = require('./compareBuckets');

const DEFAULT_WORKERS = 100;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;
const DEFAULT_LISTING_LIMIT = 1000;

const {
    DST_BUCKETD_HOSTPORT,
    SRC_BUCKETD_HOSTPORT,
    SRC_BUCKET,
    DST_BUCKET,
} = process.env;

const WORKERS = (
    process.env.WORKERS
        && Number.parseInt(process.env.WORKERS, 10)) || DEFAULT_WORKERS;

const LOG_PROGRESS_INTERVAL = (
    process.env.LOG_PROGRESS_INTERVAL
        && Number.parseInt(process.env.LOG_PROGRESS_INTERVAL, 10))
      || DEFAULT_LOG_PROGRESS_INTERVAL;

const SRC_KEY_MARKER = process.env.SRC_KEY_MARKER || '';
const DST_KEY_MARKER = process.env.DST_KEY_MARKER || '';

const LISTING_LIMIT = (
    process.env.LISTING_LIMIT
        && Number.parseInt(process.env.LISTING_LIMIT, 10))
      || DEFAULT_LISTING_LIMIT;

const USAGE = `
compareBuckets.js

This script compares the current objects in a bucket of two bucketd databases.

Usage:
    node compareBuckets.js

Mandatory environment variables:
    SRC_BUCKETD_HOSTPORT: ip:port of the source bucketd endpoint
    DST_BUCKETD_HOSTPORT: ip:port of the destination bucketd endpoint
    SRC_BUCKET: bucket to be scanned
    DST_BUCKET: bucket to be scanned

Optional environment variables:
    SRC_KEY_MARKER: key to continue listing from on the source bucket
    DST_KEY_MARKER: key to continue listing from on the destination bucket
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default ${DEFAULT_LOG_PROGRESS_INTERVAL})
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
`;

if (!SRC_BUCKETD_HOSTPORT) {
    console.error('ERROR: SRC_BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}

if (!DST_BUCKETD_HOSTPORT) {
    console.error('ERROR: DST_BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
}

if (!SRC_BUCKET) {
    console.error('ERROR: BUCKET not defined');
    console.error(USAGE);
}

if (!DST_BUCKET) {
    console.error('ERROR: BUCKET not defined');
    console.error(USAGE);
}

const log = new Logger('s3utils:compareListings');

const status = {
    missingInSrcCount: 0,
    missingInDstCount: 0,
    dstBucketInProgress: null,
    srcBucketInProgress: null,
    srcKeyMarker: '',
    dstKeyMarker: '',
};

function logProgress(message, status) {
    log.info(message, {
        targetSourceBucket: status.srcBucketInProgress,
        sourceKeyMarker: status.srcKeyMarker,
        missingInSourceCount: status.missingInSrcCount,
        targetDestinationBucket: status.dstBucketInProgress,
        destinationKeyMarker: status.dstKeyMarker,
        missingInDestinationCount: status.missingInDstCount,
    });
}

setInterval(() => logProgress('progress update', status), LOG_PROGRESS_INTERVAL * 1000);

function main() {
    const params = {
        bucketdSrcParams: {
            bucket: SRC_BUCKET,
            marker: SRC_KEY_MARKER,
            hostPort: SRC_BUCKETD_HOSTPORT,
            maxKeys: LISTING_LIMIT,
            workers: WORKERS,
        },
        bucketdDstParams: {
            bucket: DST_BUCKET,
            marker: DST_KEY_MARKER,
            hostPort: DST_BUCKETD_HOSTPORT,
            maxKeys: LISTING_LIMIT,
            workers: WORKERS,
        },
        statusObj: status,
    };
    compareBuckets(params, log, err => {
        if (err) {
            log.error('an error occurred during scan', {
                error: { message: err.message },
            });
            logProgress('last status', status);
            process.exit(1);
        } else {
            logProgress('completed scan', status);
            process.exit(0);
        }
    }, status);
}

main();

function stop() {
    log.info('stopping execution');
    logProgress('last status', status);
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
