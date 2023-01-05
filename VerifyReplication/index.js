/* eslint-disable no-console */

const { Logger } = require('werelogs');

const { defaults, mandatoryVars } = require('./constants');
const { verifyReplication } = require('./verifyReplication');
const parseOlderThan = require('../utils/parseOlderThan');

const log = new Logger('s3utils:verifyReplication');
const status = {
    srcListedCount: 0,
    dstProcessedCount: 0,
    skippedByDate: 0,
    missingInDstCount: 0,
    sizeMismatchCount: 0,
    replicatedCount: 0,
    dstFailedMdRetrievalsCount: 0,
    dstBucket: null,
    srcBucket: null,
    prefixFilters: null,
    skipOlderThan: null,
};
const {
    SRC_ENDPOINT,
    SRC_BUCKET,
    SRC_ACCESS_KEY,
    SRC_SECRET_KEY,
    DST_ENDPOINT,
    DST_ACCESS_KEY,
    DST_SECRET_KEY,
    DST_BUCKET,
    SHOW_CLIENT_LOGS_IF_AVAILABLE,
    LOG_PROGRESS_INTERVAL,
    SRC_BUCKET_PREFIXES,
    SRC_DELIMITER,
    LISTING_LIMIT,
    LISTING_WORKERS,
    BUCKET_MATCH,
    COMPARE_OBJECT_SIZE,
    COMPARE_OBJECT_ALL_VERSIONS,
    DST_STORAGE_TYPE,
    DST_REGION,
    DST_MD_REQUEST_WORKERS,
    HTTPS_CA_PATH,
    HTTPS_NO_VERIFY,
    SKIP_OLDER_THAN,
} = process.env;

const USAGE = `
VerifyReplication/index.js

This script verifies replication by comparing source and destination objects.

Usage:
    node VerifyReplication/index.js

Mandatory environment variables:
    SRC_ENDPOINT: url/ip:port of the source s3 endpoint
    SRC_BUCKET: source bucket
    SRC_ACCESS_KEY: source account/user access key
    SRC_SECRET_KEY: source account/user secret key
    DST_ACCESS_KEY: destination account/user access key
    DST_SECRET_KEY: destination account/user access key
    DST_BUCKET: destination bucket

Optional environment variables:
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines, 
    default ${defaults.LOG_PROGRESS_INTERVAL} seconds
    SRC_BUCKET_PREFIXES: comma separated prefixes to restrict the listing to
    LISTING_LIMIT: number of keys to list per listing request, default ${defaults.LISTING_LIMIT}
    LISTING_WORKERS: number of listing workers, default ${defaults.LISTING_WORKERS}
    BUCKET_MATCH: set to 1 if bucket match is enabled with replication (if bucket match is not enabled, objects
    from source are replicated with the prefix 'source_bucket_name/')
    COMPARE_OBJECT_SIZE: set to 1 to have additional check by comparing object sizes
    COMPARE_OBJECT_ALL_VERSIONS: (not supported yet) set to 1 to verify all versions of object,
    default behavior verifies only current versions
    DST_ENDPOINT: url/ip:port of the destination endpoint
    DST_STORAGE_TYPE: supports only ${defaults.STORAGE_TYPE}
    DST_REGION: applies only to storage type 'aws_s3' and if the default aws region is not ${defaults.AWS_REGION}
    DST_MD_REQUEST_WORKERS: number of concurrent destination workers making metadata requests, 
    default ${defaults.DESTINATION_MD_REQUEST_WORKERS}
    HTTPS_CA_PATH: path to a CA certificate bundle used to authentify the source endpoint
    HTTPS_NO_VERIFY: set to 1 to disable source endpoint certificate check
    SKIP_OLDER_THAN: skip replication verification of objects whose last modified date is older than this,
        set this as an ISO date or a number of days e.g.,
        - setting to "2022-11-30T00:00:00Z" skips the verification of objects created/modified before Nov 30th 2022
        - setting to "30 days" skips the verification of objects created/modified more than 30 days ago
`;

/* eslint-disable no-console */
mandatoryVars.forEach(envVar => {
    if (!process.env[envVar]) {
        console.error(`Missing mandatory environment variable ${envVar}`);
        console.error(USAGE);
        process.exit(1);
    }
});

if (DST_STORAGE_TYPE && !defaults.SUPPORTED_STORAGE_TYPES.includes(DST_STORAGE_TYPE)) {
    console.error(`Unsupported destination storage type ${DST_STORAGE_TYPE}`);
    console.error(USAGE);
    process.exit(1);
}

const skipOlderThan = SKIP_OLDER_THAN ? parseOlderThan(SKIP_OLDER_THAN) : null;

if (skipOlderThan && Number.isNaN(skipOlderThan.getTime())) {
    console.error('SKIP_OLDER_THAN is not valid');
    console.error(USAGE);
    process.exit(1);
}

const logProgressInterval = (LOG_PROGRESS_INTERVAL
    && Number.parseInt(LOG_PROGRESS_INTERVAL, 10)) || defaults.LOG_PROGRESS_INTERVAL;
const showClientLogsIfAvailable = SHOW_CLIENT_LOGS_IF_AVAILABLE === '1';

// NOTE: aws rate limit
// ref: https://aws.amazon.com/premiumsupport/knowledge-center/s3-503-within-request-rate-prefix/
// 5500 GET/HEAD requests per second per prefix per bucket!!!
const listingLimit = (LISTING_LIMIT && Number.parseInt(LISTING_LIMIT, 10)) || defaults.LISTING_LIMIT;
const listingWorkers = (LISTING_WORKERS && Number.parseInt(LISTING_WORKERS, 10)) || defaults.LISTING_WORKERS;

const bucketMatch = BUCKET_MATCH === '1';
const destinationStorageType = DST_STORAGE_TYPE || defaults.STORAGE_TYPE;
const destinationRegion = DST_REGION || defaults.AWS_REGION;
const destinationRequestWorkers = (DST_MD_REQUEST_WORKERS
    && Number.parseInt(DST_MD_REQUEST_WORKERS, 10)) || defaults.DESTINATION_MD_REQUEST_WORKERS;
const compareObjectSize = COMPARE_OBJECT_SIZE === '1';
const compareObjectAllVersions = COMPARE_OBJECT_ALL_VERSIONS === '1';
const delimiter = SRC_DELIMITER || defaults.DELIMITER;
const prefixes = SRC_BUCKET_PREFIXES ? SRC_BUCKET_PREFIXES.split(',')
    .filter(x => x)
    .map(x => (x.endsWith(delimiter) ? x : `${x}${delimiter}`)) : [];

function logProgress(message, status) {
    log.info(message, status);
}

setInterval(() => logProgress('progress update', status), logProgressInterval * 1000);

function main() {
    const params = {
        source: {
            storageType: defaults.STORAGE_TYPE,
            endpoint: SRC_ENDPOINT,
            accessKey: SRC_ACCESS_KEY,
            secretKey: SRC_SECRET_KEY,
            bucket: SRC_BUCKET,
            region: defaults.AWS_REGION,
            prefixes,
            httpsCaPath: HTTPS_CA_PATH,
            httpsNoVerify: HTTPS_NO_VERIFY,
            httpTimeout: defaults.AWS_SDK_REQUEST_TIMEOUT,
            listingLimit,
            listingWorkers,
            showClientLogsIfAvailable,
            delimiter,
        },
        destination: {
            storageType: destinationStorageType,
            endpoint: DST_ENDPOINT,
            accessKey: DST_ACCESS_KEY,
            secretKey: DST_SECRET_KEY,
            bucket: DST_BUCKET,
            bucketMatch,
            region: destinationRegion,
            requestWorkers: destinationRequestWorkers,
            httpTimeout: defaults.AWS_SDK_REQUEST_TIMEOUT,
            showClientLogsIfAvailable,
            // TODO: check if https should be supported on destinations
        },
        verification: {
            compareObjectSize,
            compareObjectAllVersions,
            skipOlderThan,
        },
        status,
        log,
    };
    log.info('starting verification', {
        srcBucket: SRC_BUCKET,
        srcEndpoint: SRC_ENDPOINT,
        srcPrefixes: prefixes,
        dstBucket: DST_BUCKET,
        dstEndpoint: DST_ENDPOINT,
        dstStorageType: destinationStorageType,
        compareObjectSize,
        compareObjectAllVersions,
        bucketMatch,
        listingLimit,
        listingWorkers,
        destinationRequestWorkers,
        showClientLogsIfAvailable,
        delimiter,
        skipOlderThan: SKIP_OLDER_THAN,
    });
    verifyReplication(params, err => {
        if (err) {
            log.error('an error occurred during replication verification', err);
            logProgress('last status', status);
            process.exit(1);
        } else {
            logProgress('completed replication verification', status);
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
