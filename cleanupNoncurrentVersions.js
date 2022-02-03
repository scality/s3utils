const fs = require('fs');
const http = require('http');
const https = require('https');

const AWS = require('aws-sdk');
const { doWhilst, eachSeries } = require('async');

const { Logger } = require('werelogs');

const log = new Logger('s3utils::cleanupNoncurrentVersions');
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { S3_ENDPOINT } = process.env;
const { TARGET_PREFIX } = process.env;
const MAX_DELETES = (process.env.MAX_DELETES
    && Number.parseInt(process.env.MAX_DELETES, 10));
const MAX_LISTED = (process.env.MAX_LISTED
    && Number.parseInt(process.env.MAX_LISTED, 10));
const { MARKER } = process.env;
const OLDER_THAN = (process.env.OLDER_THAN
    ? new Date(process.env.OLDER_THAN) : null);
const { HTTPS_CA_PATH } = process.env;
const { HTTPS_NO_VERIFY } = process.env;

const LISTING_LIMIT = 1000;
const LOG_PROGRESS_INTERVAL_MS = 10000;
const AWS_SDK_REQUEST_RETRIES = 100;
const AWS_SDK_REQUEST_INITIAL_DELAY_MS = 30;

const USAGE = `
cleanupNoncurrentVersions.js

This script removes noncurrent versions and current/noncurrent delete
markers, either all such objects or older than a specified
last-modified date.

Usage:
    node cleanupNoncurrentVersions.js bucket1[,bucket2...]

Mandatory environment variables:
    S3_ENDPOINT: S3 endpoint URL
    ACCESS_KEY: S3 account/user access key
    SECRET_KEY: S3 account/user secret key

Optional environment variables:
    TARGET_PREFIX: cleanup only inside this key prefix in each bucket
    MAX_LISTED: maximum number of keys listed before exiting (default
    unlimited)
    MAX_DELETES: maximum number of keys to delete before exiting
    (default unlimited)
    MARKER: marker from which to resume the cleanup, logged at the end
    of a previous invocation of the script, uses the format:
    MARKER := encodeURI(bucketName)
              "|" encodeURI(key)
              "|" encodeURI(versionId)
    OLDER_THAN: cleanup only objects which last modified date is older
    than this date, e.g. setting to "2021-01-09T00:00:00Z" limits the
    cleanup to objects created or modified before Jan 9th 2021.
    HTTPS_CA_PATH: path to a CA certificate bundle used to authentify
    the S3 endpoint
    HTTPS_NO_VERIFY: set to 1 to disable S3 endpoint certificate check
`;

// We accept console statements for usage purpose
/* eslint-disable no-console */
if (!BUCKETS || BUCKETS.length === 0) {
    console.error('No buckets given as input, please provide '
                  + 'a comma-separated list of buckets');
    console.error(USAGE);
    process.exit(1);
}
if (!S3_ENDPOINT) {
    console.error('S3_ENDPOINT not defined');
    console.error(USAGE);
    process.exit(1);
}
const s3EndpointIsHttps = S3_ENDPOINT.startsWith('https:');
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
if (OLDER_THAN && Number.isNaN(OLDER_THAN.getTime())) {
    console.error('OLDER_THAN is an invalid date');
    console.error(USAGE);
    process.exit(1);
}

function _encodeMarker(bucket, key, versionId) {
    return `${encodeURI(bucket)}`
        + `|${encodeURI(key || '')}|${encodeURI(versionId || '')}`;
}

function _parseMarker(marker) {
    if (!marker) {
        return {};
    }
    const [bucket, key, versionId] = marker.split('|');
    return {
        bucket: decodeURI(bucket),
        key: decodeURI(key),
        versionId: decodeURI(versionId),
    };
}

let {
    bucket: MARKER_BUCKET,
    key: MARKER_KEY,
    versionId: MARKER_VERSION_ID,
} = _parseMarker(MARKER);

/* eslint-enable no-console */
log.info('Start deleting noncurrent versions and delete markers', {
    buckets: BUCKETS,
    prefix: TARGET_PREFIX,
    endpoint: S3_ENDPOINT,
    maxListed: MAX_LISTED,
    maxDeletes: MAX_DELETES,
    startBucket: MARKER_BUCKET,
    startKey: MARKER_KEY,
    startVersionId: MARKER_VERSION_ID,
    olderThan: (OLDER_THAN ? OLDER_THAN.toString() : 'N/A'),
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
    endpoint: S3_ENDPOINT,
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

let nListed = 0;
let nDeletesTriggered = 0;
let nDeleted = 0;
let nSkippedCurrent = 0;
let nSkippedTooRecent = 0;
let nErrors = 0;
let bucketInProgress = null;
let KeyMarker = null;
let VersionIdMarker = null;

function _logProgress(message) {
    log.info(message, {
        listed: nListed,
        deletesTriggered: nDeletesTriggered,
        deleted: nDeleted,
        skippedCurrent: nSkippedCurrent,
        skippedTooRecent: nSkippedTooRecent,
        errors: nErrors,
        bucket: bucketInProgress || null,
        keyMarker: KeyMarker || null,
        versionIdMarker: VersionIdMarker || null,
    });
}

const logProgressInterval = setInterval(
    () => _logProgress('progress update'),
    LOG_PROGRESS_INTERVAL_MS,
);

function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    return s3.listObjectVersions({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        Prefix: TARGET_PREFIX,
        KeyMarker,
        VersionIdMarker,
    }, cb);
}

function _lastModifiedIsEligible(lastModifiedString) {
    return !OLDER_THAN || (new Date(lastModifiedString) < OLDER_THAN);
}

let deleteQueue = [];
let batchDeleteInProgress = false;
let batchDeleteOnDrain = null;
let batchDeleteOnFullDrain = null;

function _doBatchDelete(bucket) {
    batchDeleteInProgress = true;
    // multi object delete can delete max 1000 objects
    const batchDeleteObjects = deleteQueue.splice(0, 1000);
    const params = {
        Bucket: bucket,
        Delete: { Objects: batchDeleteObjects },
    };
    s3.deleteObjects(params, err => {
        if (err) {
            log.error('batch delete error', { error: err });
            nErrors += 1;
            batchDeleteObjects.forEach(
                v => log.error('object may not be deleted', {
                    bucket,
                    key: v.Key,
                    versionId: v.VersionId,
                }),
            );
        } else {
            nDeleted += batchDeleteObjects.length;
            batchDeleteObjects.forEach(v => log.info('object deleted', {
                bucket,
                key: v.Key,
                versionId: v.VersionId,
            }));
        }
        if (batchDeleteOnDrain && deleteQueue.length <= 1000) {
            process.nextTick(batchDeleteOnDrain);
            batchDeleteOnDrain = null;
        }
        if (batchDeleteOnFullDrain && deleteQueue.length === 0) {
            process.nextTick(batchDeleteOnFullDrain);
            batchDeleteOnFullDrain = null;
        }
        if (deleteQueue.length > 0) {
            // there are more objects to delete, keep going
            _doBatchDelete(bucket);
        } else {
            batchDeleteInProgress = false;
        }
    });
}

function _triggerDeletes(bucket, versionsToDelete, cb) {
    nDeletesTriggered += versionsToDelete.length;
    deleteQueue = deleteQueue.concat(versionsToDelete);
    if (nDeletesTriggered > MAX_DELETES) {
        deleteQueue.splice(
            deleteQueue.length - (nDeletesTriggered - MAX_DELETES),
        );
        nDeletesTriggered = MAX_DELETES;
    }
    if (deleteQueue.length > 0) {
        if (!batchDeleteInProgress) {
            _doBatchDelete(bucket);
        }
        if (deleteQueue.length > 1000) {
            batchDeleteOnDrain = cb;
            // wait for the batch delete in progress to complete before
            // resuming listings for backpressure
            return undefined;
        }
    }
    // continue listing if the delete queue is short enough
    // (i.e. shorter than the maximum allowed to batch delete at
    // once)
    return process.nextTick(cb);
}

// Decrement a versionId by one character, for use as a
// NextVersionIdMarker to include the versionId in the next listing.
function decVersionId(versionId) {
    if (!versionId) {
        return versionId;
    }
    return versionId.slice(0, versionId.length - 1)
        + String.fromCharCode(versionId.charCodeAt(versionId.length - 1) - 1);
}

function _triggerDeletesOnEligibleObjects(
    bucket,
    versions,
    deleteMarkers,
    endOfListing,
    cb,
) {
    const versionsToDelete = [];
    versions.forEach(version => {
        if (version.IsLatest !== false) {
            nSkippedCurrent += 1;
        } else if (!_lastModifiedIsEligible(version.LastModified)) {
            nSkippedTooRecent += 1;
        } else {
            versionsToDelete.push({
                Key: version.Key,
                VersionId: version.VersionId,
            });
        }
    });
    let ret = null;
    deleteMarkers.forEach(deleteMarker => {
        if (!_lastModifiedIsEligible(deleteMarker.LastModified)) {
            nSkippedTooRecent += 1;
        } else {
            // To avoid making existing noncurrent versions current,
            // we must be cautious on which delete markers we may
            // safely delete.
            //
            // We can delete a DM safely iff:
            //
            //   it is a noncurrent DM
            // OR
            //   at least one of the last listed version or DM key is
            //   strictly greater than the current DM key to delete
            // OR
            //   we reached the end of the listing
            //
            // Otherwise, we must not remove the DM before further
            // listing to make sure we also remove the associated
            // noncurrent versions. In such case, we continue listing
            // from that DM key and versionId at next iteration,
            // instead of the returned next key/versionId markers.
            if (deleteMarker.IsLatest === false
                || (deleteMarkers[deleteMarkers.length - 1].Key
                    > deleteMarker.Key)
                || (versions.length > 0
                    && (versions[versions.length - 1].Key
                        > deleteMarker.Key))
                || endOfListing) {
                versionsToDelete.push({
                    Key: deleteMarker.Key,
                    VersionId: deleteMarker.VersionId,
                });
            } else {
                ret = {
                    NextKeyMarker: deleteMarker.Key,
                    NextVersionIdMarker: decVersionId(deleteMarker.VersionId),
                };
            }
        }
    });
    _triggerDeletes(bucket, versionsToDelete, cb);
    return ret;
}

function _waitForDeletesCompletion(cb) {
    if (batchDeleteInProgress) {
        batchDeleteOnFullDrain = cb;
        return undefined;
    }
    return process.nextTick(cb);
}

function triggerDeletesOnBucket(bucketName, cb) {
    const bucket = bucketName.trim();
    let NextKeyMarker = null;
    let NextVersionIdMarker = null;
    if (MARKER_BUCKET) {
        // ignore initial buckets until we find the one where we left off
        if (MARKER_BUCKET !== bucket) {
            return process.nextTick(cb);
        }
        // resume from where we left off in the bucket
        NextKeyMarker = MARKER_KEY;
        NextVersionIdMarker = MARKER_VERSION_ID;
        MARKER_BUCKET = undefined;
        MARKER_KEY = undefined;
        MARKER_VERSION_ID = undefined;
        log.info(`resuming at: bucket=${bucket} KeyMarker=${NextKeyMarker} `
                 + `VersionIdMarker=${NextVersionIdMarker}`);
    }
    bucketInProgress = bucket;
    log.info(`starting task for bucket: ${bucket}`);
    return doWhilst(
        done => {
            KeyMarker = NextKeyMarker;
            VersionIdMarker = NextVersionIdMarker;
            _listObjectVersions(bucket, VersionIdMarker, KeyMarker, (err, data) => {
                if (err) {
                    log.error('error listing object versions', {
                        error: err,
                    });
                    return done(err);
                }
                nListed += data.Versions.length + data.DeleteMarkers.length;
                const ret = _triggerDeletesOnEligibleObjects(
                    bucket,
                    data.Versions,
                    data.DeleteMarkers,
                    !data.IsTruncated,
                    err => {
                        if (err) {
                            return done(err);
                        }
                        if (ret) {
                            NextKeyMarker = ret.NextKeyMarker;
                            NextVersionIdMarker = ret.NextVersionIdMarker;
                        } else {
                            NextKeyMarker = data.NextKeyMarker;
                            NextVersionIdMarker = data.NextVersionIdMarker;
                        }
                        return done();
                    },
                );
                return undefined;
            });
        },
        () => {
            if (nDeletesTriggered >= MAX_DELETES || nListed >= MAX_LISTED) {
                return false;
            }
            if (NextKeyMarker || NextVersionIdMarker) {
                return true;
            }
            return false;
        },
        err => {
            bucketInProgress = null;
            if (err) {
                log.error('error during execution', {
                    bucket,
                    KeyMarker,
                    VersionIdMarker,
                });
                _logProgress('final summary after error');
                return cb(err);
            }
            return _waitForDeletesCompletion(() => {
                if (nDeletesTriggered >= MAX_DELETES || nListed >= MAX_LISTED) {
                    _logProgress('final summary');
                    const marker = _encodeMarker(bucket, KeyMarker, VersionIdMarker);
                    const message = 'reached '
                        + `${nDeleted >= MAX_DELETES ? 'delete' : 'scanned'} `
                        + 'count limit, resuming from this point can be '
                        + 'achieved by re-running the script with the same '
                        + 'bucket list and the following environment variable '
                        + `set: MARKER='${marker}'`;
                    log.info(message);
                    process.exit(0);
                }
                log.info(`completed task for bucket: ${bucket}`);
                return cb();
            });
        },
    );
}

// trigger the calls to list objects and delete noncurrent versions
// and delete markers
eachSeries(BUCKETS, triggerDeletesOnBucket, err => {
    clearInterval(logProgressInterval);
    if (err) {
        return log.error('error during task execution', { error: err });
    }
    _logProgress('final summary');
    return log.info('completed task for all buckets');
});

function stop() {
    log.warn('stopping execution');
    _logProgress('final summary');
    process.exit(1);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
