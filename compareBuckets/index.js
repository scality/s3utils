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

const VERBOSE = process.env.VERBOSE === '1';
const COMPARE_VERSION_ID = process.env.COMPARE_VERSION_ID === '1';
const COMPARE_OBJECT_SIZE = process.env.COMPARE_OBJECT_SIZE === '1';

const KEY_MARKER = process.env.KEY_MARKER || '';

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
    KEY_MARKER: key to continue listing from
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default ${DEFAULT_LOG_PROGRESS_INTERVAL})
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
    VERBOSE: set to 1 for more verbose output (show last-modified dates and replication statuses of objects)
    COMPARE_VERSION_ID: set to 1 to have additional check by comapring version ids (cannot be set with COMPARE_OBJECT_SIZE)
    COMPARE_OBJECT_SIZE: set to 1 to have additional check by comparing object sizes (cannot be set with COMPARE_VERSION_ID)
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

if (COMPARE_VERSION_ID && COMPARE_OBJECT_SIZE) {
    console.error('ERROR: COMPARE_VERSION_ID and COMPARE_OBJECT_SIZE cannot be set together');
    console.error(USAGE);
}

const log = new Logger('s3utils:compareListings');

const status = {
    srcProcessedCount: 0,
    dstProcessedCount: 0,
    missingInSrcCount: 0,
    missingInDstCount: 0,
    dstBucketInProgress: null,
    srcBucketInProgress: null,
    srcKeyMarker: '',
    dstKeyMarker: '',
};

function logProgress(message, status) {
    log.info(message, {
        source: {
            bucket: status.srcBucketInProgress,
            objectsMissingInDestination: status.missingInDstCount,
            objectsScanned: status.srcProcessedCount,
        },
        destination: {
            bucket: status.dstBucketInProgress,
            objectsMissingInSource: status.missingInSrcCount,
            objectsScanned: status.dstProcessedCount,
        },
        keyMarker: status.srcKeyMarker < status.dstKeyMarker
            ? status.srcKeyMarker : status.dstKeyMarker,
    });
}

setInterval(() => logProgress('progress update', status), LOG_PROGRESS_INTERVAL * 1000);

function main() {
    const params = {
        bucketdSrcParams: {
            bucket: SRC_BUCKET,
            marker: KEY_MARKER,
            hostPort: SRC_BUCKETD_HOSTPORT,
            maxKeys: LISTING_LIMIT,
            workers: WORKERS,
        },
        bucketdDstParams: {
            bucket: DST_BUCKET,
            marker: KEY_MARKER,
            hostPort: DST_BUCKETD_HOSTPORT,
            maxKeys: LISTING_LIMIT,
            workers: WORKERS,
        },
        verbose: VERBOSE,
        compareVersionId: COMPARE_VERSION_ID,
        compareObjectSize: COMPARE_OBJECT_SIZE,
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
