/* eslint-disable max-len */
/* eslint-disable no-console */

const http = require('http');
const async = require('async');
const AWS = require('aws-sdk');
const { Logger } = require('werelogs');

const log = new Logger('s3utils::removeDeleteMarkers');
// configurable params

const DEFAULT_WORKERS = 10;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;
const DEFAULT_LISTING_LIMIT = 1000;

const USAGE = `
removeDeleteMarkers.js

This script removes delete markers from one or more versioning-suspended bucket(s)

Usage:
    node removeDeleteMarkers.js bucket1[,bucket2...]

Mandatory environment variables:
    ENDPOINT: S3 endpoint
    ACCESS_KEY: S3 account access key
    SECRET_KEY: S3 account secret key

Optional environment variables:
    TARGET_PREFIX: only process a specific prefix in the bucket(s)
    WORKERS: concurrency value for listing / batch delete requests (default ${DEFAULT_WORKERS})
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default ${DEFAULT_LOG_PROGRESS_INTERVAL})
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
    KEY_MARKER: resume processing from a specific key
    VERSION_ID_MARKER: resume processing from a specific version ID
`;

const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { ENDPOINT } = process.env;

const { TARGET_PREFIX } = process.env;
const WORKERS = (
    process.env.WORKERS
        && Number.parseInt(process.env.WORKERS, 10)) || DEFAULT_WORKERS;

const { KEY_MARKER } = process.env;
const { VERSION_ID_MARKER } = process.env;

if (!BUCKETS || BUCKETS.length === 0) {
    console.error('No buckets given as input, please provide '
                  + 'a comma-separated list of buckets on the command line');
    console.error(USAGE);
    process.exit(1);
}
if (!ENDPOINT) {
    console.error('ENDPOINT not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!ACCESS_KEY) {
    console.error('ACCESS_KEY not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!SECRET_KEY) {
    console.error('SECRET_KEY not defined');
    console.error(USAGE);
    process.exit(1);
}
const LISTING_LIMIT = (
    process.env.LISTING_LIMIT
        && Number.parseInt(process.env.LISTING_LIMIT, 10))
      || DEFAULT_LISTING_LIMIT;

const LOG_PROGRESS_INTERVAL = (
    process.env.LOG_PROGRESS_INTERVAL
        && Number.parseInt(process.env.LOG_PROGRESS_INTERVAL, 10))
      || DEFAULT_LOG_PROGRESS_INTERVAL;

AWS.config.update({
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    endpoint: ENDPOINT,
    region: 'us-east-1',
    sslEnabled: false,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
});

const s3 = new AWS.S3({
    httpOptions: {
        agent: new http.Agent({ keepAlive: true }),
    },
});

const status = {
    objectsListed: 0,
    deleteMarkersDeleted: 0,
    deleteMarkersErrors: 0,
    bucketInProgress: null,
    keyMarker: KEY_MARKER,
    versionIdMarker: VERSION_ID_MARKER,
};

function logProgress(message) {
    log.info(message, {
        objectsListed: status.objectsListed,
        deleteMarkersDeleted: status.deleteMarkersDeleted,
        deleteMarkersErrors: status.deleteMarkersErrors,
        bucketInProgress: status.bucketInProgress,
        keyMarker: status.keyMarker,
        versionIdMarker: status.versionIdMarker,
    });
}

const progressInterval = setInterval(
    () => logProgress('progress update'),
    LOG_PROGRESS_INTERVAL * 1000,
);

const taskQueue = async.queue((task, done) => {
    const {
        bucket, beginBucket, keyMarker, versionIdMarker,
    } = task;
    let bucketDone = false;

    status.keyMarker = keyMarker;
    status.versionIdMarker = versionIdMarker;

    return async.waterfall([
        next => {
            if (!beginBucket) {
                return next();
            }
            logProgress('start scanning bucket');
            return s3.getBucketVersioning({
                Bucket: bucket,
            }, (err, data) => {
                if (err) {
                    log.error('error getting bucket versioning', {
                        bucket,
                        error: err.message,
                    });
                    bucketDone = true;
                    return next(err);
                }
                if (data.Status !== 'Suspended') {
                    log.error('bucket versioning status is not "Suspended", skipping bucket', {
                        bucket,
                        versioningStatus: data.Status,
                    });
                    bucketDone = true;
                    return next(new Error('bucket not processed'));
                }
                return next();
            });
        },
        next => s3.listObjectVersions({
            Bucket: bucket,
            MaxKeys: LISTING_LIMIT,
            Prefix: TARGET_PREFIX,
            KeyMarker: keyMarker,
            VersionIdMarker: versionIdMarker,
        }, (err, data) => {
            if (err) {
                log.error('error listing object versions', {
                    bucket,
                    keyMarker,
                    versionIdMarker,
                    error: err.message,
                });
                bucketDone = true;
                return next(err);
            }
            status.objectsListed += data.Versions.length + data.DeleteMarkers.length;
            if (data.NextKeyMarker || data.NextVersionIdMarker) {
                taskQueue.push({
                    bucket,
                    keyMarker: data.NextKeyMarker,
                    versionIdMarker: data.NextVersionIdMarker,
                });
            } else {
                bucketDone = true;
            }
            return next(null, data.DeleteMarkers);
        }),
        (deleteMarkers, next) => {
            if (deleteMarkers.length === 0) {
                return next();
            }
            return s3.deleteObjects({
                Bucket: bucket,
                Delete: {
                    Objects: deleteMarkers.map(item => ({
                        Key: item.Key,
                        VersionId: item.VersionId,
                    })),
                },
            }, (err, data) => {
                if (err) {
                    log.error('batch delete request error', {
                        bucket,
                        keyMarker,
                        versionIdMarker,
                        error: err.message,
                    });
                    status.deleteMarkersErrors += deleteMarkers.length;
                    deleteMarkers.forEach(entry => {
                        log.error('error deleting delete marker', {
                            bucket,
                            objectKey: entry.Key,
                            versionId: entry.VersionId,
                            error: 'batch delete request failed',
                        });
                    });
                    return next();
                }
                if (data.Deleted) {
                    status.deleteMarkersDeleted += data.Deleted.length;
                    data.Deleted.forEach(entry => {
                        log.info('delete marker deleted', {
                            bucket,
                            objectKey: entry.Key,
                            versionId: entry.VersionId,
                        });
                    });
                }
                if (data.Errors) {
                    status.deleteMarkersErrors += data.Errors.length;
                    data.Errors.forEach(entry => {
                        log.error('error deleting delete marker', {
                            bucket,
                            objectKey: entry.Key,
                            versionId: entry.VersionId,
                            error: entry.Code,
                            errorDesc: entry.Message,
                        });
                    });
                }
                return next();
            });
        },
    ], err => {
        if (bucketDone) {
            status.keyMarker = null;
            status.versionIdMarker = null;
            if (!err) {
                logProgress('completed listing of bucket');
            }
            if (BUCKETS.length > 0) {
                status.bucketInProgress = BUCKETS.shift();
                taskQueue.push({
                    bucket: status.bucketInProgress,
                    beginBucket: true,
                });
            }
        }
        return done();
    });
}, WORKERS);

log.info('starting cleanup of delete markers in buckets', {
    buckets: BUCKETS,
});
status.bucketInProgress = BUCKETS.shift();
taskQueue.push({
    bucket: status.bucketInProgress,
    beginBucket: true,
    keyMarker: KEY_MARKER,
    versionIdMarker: VERSION_ID_MARKER,
});
taskQueue.drain = () => {
    status.bucketInProgress = null;
    logProgress('completed cleanup of all buckets');
    clearInterval(progressInterval);
};

function stop() {
    log.info('stopping execution');
    logProgress('last status');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
