const http = require('http');

const AWS = require('aws-sdk');
const { doWhilst, eachSeries, eachLimit, waterfall } = require('async');

const { Logger } = require('werelogs');
const { ObjectMD } = require('arsenal').models;

const BackbeatClient = require('./BackbeatClient');

const log = new Logger('s3utils::crrExistingObjects');
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
const SITE_NAME = process.env.SITE_NAME;
let STORAGE_TYPE = process.env.STORAGE_TYPE;
let TARGET_REPLICATION_STATUS = process.env.TARGET_REPLICATION_STATUS;
const TARGET_PREFIX = process.env.TARGET_PREFIX;
const WORKERS = (process.env.WORKERS &&
    Number.parseInt(process.env.WORKERS, 10)) || 10;
const MAX_UPDATES = (process.env.MAX_UPDATES &&
    Number.parseInt(process.env.MAX_UPDATES, 10));
const MAX_SCANNED = (process.env.MAX_SCANNED &&
    Number.parseInt(process.env.MAX_SCANNED, 10));
let KEY_MARKER = process.env.KEY_MARKER;
let VERSION_ID_MARKER = process.env.VERSION_ID_MARKER;
const { GENERATE_INTERNAL_VERSION_ID } = process.env;
const LISTING_LIMIT = (process.env.LISTING_LIMIT
    && Number.parseInt(process.env.LISTING_LIMIT, 10)) || 1000;
const LOG_PROGRESS_INTERVAL_MS = 10000;
const AWS_SDK_REQUEST_RETRIES = 100;
const AWS_SDK_REQUEST_DELAY_MS = 30;

if (!BUCKETS || BUCKETS.length === 0) {
    log.fatal('No buckets given as input! Please provide ' +
        'a comma-separated list of buckets');
    process.exit(1);
}
if (!ENDPOINT) {
    log.fatal('ENDPOINT not defined!');
    process.exit(1);
}
if (!ACCESS_KEY) {
    log.fatal('ACCESS_KEY not defined');
    process.exit(1);
}
if (!SECRET_KEY) {
    log.fatal('SECRET_KEY not defined');
    process.exit(1);
}
if (!STORAGE_TYPE) {
    STORAGE_TYPE = '';
}
if (!TARGET_REPLICATION_STATUS) {
    TARGET_REPLICATION_STATUS = 'NEW';
}

const replicationStatusToProcess = TARGET_REPLICATION_STATUS.split(',');
replicationStatusToProcess.forEach(state => {
    if (!['NEW', 'PENDING', 'COMPLETED', 'FAILED', 'REPLICA'].includes(state)) {
        log.fatal('invalid TARGET_REPLICATION_STATUS environment: must be a '
            + 'comma-separated list of replication statuses to requeue, '
            + 'as NEW, PENDING, COMPLETED, FAILED or REPLICA.');
        process.exit(1);
    }
});
log.info('Objects with replication status '
    + `${replicationStatusToProcess.join(' or ')} `
    + 'will be reset to PENDING to trigger CRR');

// Configuration for AWS S3
const awsConfig = {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    endpoint: ENDPOINT,
    region: 'us-east-1',
    sslEnabled: false,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
    httpOptions: {
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
};

/**
 * Custom backoff strategy for AWS SDK requests.
 * @param {number} retryCount - The current retry attempt.
 * @param {Error} error - The error that caused the retry.
 * @returns {number} The delay in milliseconds before the next retry.
 */
function customBackoffStrategy(retryCount, error) {
    log.error('aws sdk request error', { error, retryCount });
    // The delay is not truly exponential; it resets to the minimum after every 10 calls,
    // with a maximum delay of 15 seconds.
    return AWS_SDK_REQUEST_DELAY_MS * Math.pow(2, retryCount % 10);
}

// Specific options for S3 requests
const s3SpecificOptions = {
    maxRetries: AWS_SDK_REQUEST_RETRIES,
    customBackoff: customBackoffStrategy,
};

// Create an S3 client instance
const s3 = new AWS.S3({ ...awsConfig, ...s3SpecificOptions });

// Create a BackbeatClient instance
const bb = new BackbeatClient(awsConfig);

let nProcessed = 0;
let nSkipped = 0;
let nUpdated = 0;
let nErrors = 0;
let bucketInProgress = null;
let VersionIdMarker = null;
let KeyMarker = null;

function _logProgress() {
    log.info('progress update', {
        updated: nUpdated,
        skipped: nSkipped,
        errors: nErrors,
        bucket: bucketInProgress || null,
        keyMarker: KeyMarker || null,
        versionIdMarker: VersionIdMarker || null,
    });
}

const logProgressInterval = setInterval(_logProgress, LOG_PROGRESS_INTERVAL_MS);

function _objectShouldBeUpdated(objMD) {
    return replicationStatusToProcess.some(filter => {
        if (filter === 'NEW') {
            return (!objMD.getReplicationInfo() ||
                    objMD.getReplicationInfo().status === '');
        }
        return (objMD.getReplicationInfo() &&
                objMD.getReplicationInfo().status === filter);
    });
}

function _markObjectPending(bucket, key, versionId, storageClass,
    repConfig, cb) {
    let objMD;
    let skip = false;
    return waterfall([
        // get object blob
        next => bb.getMetadata({
            Bucket: bucket,
            Key: key,
            VersionId: versionId,
        }, next),
        (mdRes, next) => {
            // The Arsenal Object Metadata schema version 8.1 is being used for both Ring S3C and Artesca,
            // and this is acceptable because the 8.1 schema only adds extra properties to the 7.10 schema.
            // This is beneficial because:
            // - Forward compatibility: Having the 8.1 properties in place now ensures that
            //   S3C is compatible with the 8.1 schema, which could be useful if we plan to upgrade
            //   from 7.10 to 8.1 in the future.
            // - Minimal impact on current functionality: The extra properties from the 8.1
            //   schema do not interfere with the current functionalities of the 7.10 environment,
            //   so there is no harm in keeping them. S3C should ignore them without causing any issues.
            // - Simplified codebase: Not having to remove these properties simplifies the codebase of s3utils.
            //   This avoids the added complexity and potential errors associated with conditionally removing
            //   or altering metadata properties based on the version.
            // - Single schema approach: Maintaining a single, unified schema approach in s3utils can make the
            //   codebase easier to maintain and upgrade, as opposed to having multiple branches or versions of 
            //   the code for different schema versions.
            objMD = new ObjectMD(JSON.parse(mdRes.Body));
            console.log('GET METADATA!!!', objMD);
            if (!_objectShouldBeUpdated(objMD)) {
                skip = true;
                return next();
            }
            if (objMD.getVersionId()) {
                // The object already has an *internal* versionId,
                // which exists when the object has been put on
                // versioned or versioning-suspended bucket. Even if
                // the listed version is "null", the object may have
                // an actual internal versionId, only if the bucket
                // was versioning-suspended when the object was put.
                return next();
            }
            if (!GENERATE_INTERNAL_VERSION_ID) {
                // When the GENERATE_INTERNAL_VERSION_ID env variable is set,
                // matching objects with no *internal* versionId will get
                // "updated" to get an internal versionId. The external versionId
                // will still be "null".
                return next();
            }
            // The object does not have an *internal* versionId, as it
            // was put on a nonversioned bucket: do a first metadata
            // update to let cloudserver generate one, just passing on
            // the existing metadata blob. Note that the resulting key
            // will still be nonversioned, but the following update
            // will be able to create a versioned key for this object,
            // so that replication can happen. The externally visible
            // version will stay "null".
            console.log('PUT METADATA1!!!', {
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            });
            return bb.putMetadata({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                ContentLength: Buffer.byteLength(mdRes.Body),
                Body: mdRes.Body,
            }, (err, putRes) => {
                if (err) {
                    return next(err);
                }
                console.log('PUT METADATA >>>>>>>>');
                // No need to fetch the whole metadata again, simply
                // update the one we have with the generated versionId.
                objMD.setVersionId(putRes.versionId);
                console.log('putRes.versionId!!!', putRes.versionId);
                return next();
            });
        },
        // update replication info and put back object blob
        next => {
            if (skip) {
                return next();
            }
            // Initialize replication info, if missing
            // This is particularly important if the object was created before 
            // enabling replication on the bucket.
            if (!objMD.getReplicationInfo()
                || !objMD.getReplicationSiteStatus(storageClass)) {
                const { Rules, Role } = repConfig;
                const destination = Rules[0].Destination.Bucket;
                // set replication properties
                const ops = objMD.getContentLength() === 0 ? ['METADATA']
                    : ['METADATA', 'DATA'];
                const backends = [{
                    site: storageClass,
                    status: 'PENDING',
                    dataStoreVersionId: '',
                }];
                const replicationInfo = {
                    status: 'PENDING',
                    backends,
                    content: ops,
                    destination,
                    storageClass,
                    role: Role,
                    storageType: STORAGE_TYPE,
                };
                objMD.setReplicationInfo(replicationInfo);
            }

            objMD.setReplicationSiteStatus(storageClass, 'PENDING');
            objMD.setReplicationStatus('PENDING');
            objMD.updateMicroVersionId?.();
            console.log('objMD!!!', objMD);
            const md = objMD.getSerialized();
            console.log('SERIALIZED objMD!!!', md);
            console.log('PUT METADATA2', {
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            })
            return bb.putMetadata({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                ContentLength: Buffer.byteLength(md),
                Body: md,
            }, next);
        },
    ], err => {
        ++nProcessed;
        if (err) {
            ++nErrors;
            log.error('error updating object', {
                bucket, key, versionId, error: err.message,
            });
            return cb();
        }
        if (skip) {
            ++nSkipped;
        } else {
            ++nUpdated;
        }
        return cb();
    });
}

// list object versions
function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    return s3.listObjectVersions({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        Prefix: TARGET_PREFIX,
        VersionIdMarker,
        KeyMarker,
    }, cb);
}

function _markPending(bucket, versions, cb) {
    const options = { Bucket: bucket };
    waterfall([
        next => s3.getBucketReplication(options, (err, res) => {
            if (err) {
                log.error('error getting bucket replication', { error: err });
                return next(err);
            }
            return next(null, res.ReplicationConfiguration);
        }),
        (repConfig, next) => {
            const { Rules } = repConfig;
            const storageClass = Rules[0].Destination.StorageClass || SITE_NAME;
            if (!storageClass) {
                const errMsg = 'missing SITE_NAME environment variable, must be set to'
                    + ' the value of "site" property in the CRR configuration';
                log.error(errMsg);
                return next(new Error(errMsg));
            }
            return eachLimit(versions, WORKERS, (i, apply) => {
                const { Key, VersionId } = i;
                _markObjectPending(bucket, Key, VersionId, storageClass, repConfig, apply);
            }, next);
        },
    ], cb);
}

function triggerCRROnBucket(bucketName, cb) {
    const bucket = bucketName.trim();
    bucketInProgress = bucket;
    log.info(`starting task for bucket: ${bucket}`);
    if (KEY_MARKER || VERSION_ID_MARKER) {
        // resume from where we left off in previous script launch
        KeyMarker = KEY_MARKER;
        VersionIdMarker = VERSION_ID_MARKER;
        KEY_MARKER = undefined;
        VERSION_ID_MARKER = undefined;
        log.info(`resuming at: KeyMarker=${KeyMarker} `
            + `VersionIdMarker=${VersionIdMarker}`);
    }
    doWhilst(
        done => _listObjectVersions(
            bucket,
            VersionIdMarker,
            KeyMarker,
            (err, data) => {
                if (err) {
                    log.error('error listing object versions', { error: err });
                    return done(err);
                }
                return _markPending(
                    bucket, data.Versions.concat(data.DeleteMarkers), err => {
                        if (err) {
                            return done(err);
                        }
                        VersionIdMarker = data.NextVersionIdMarker;
                        KeyMarker = data.NextKeyMarker;
                        return done();
                    });
            }),
        () => {
            if (nUpdated >= MAX_UPDATES || nProcessed >= MAX_SCANNED) {
                _logProgress();
                let remainingBuckets;
                if (VersionIdMarker || KeyMarker) {
                    // next bucket to process is still the current one
                    remainingBuckets = BUCKETS.slice(
                        BUCKETS.findIndex(bucket => bucket === bucketName),
                    );
                } else {
                    // next bucket to process is the next in bucket list
                    remainingBuckets = BUCKETS.slice(
                        BUCKETS.findIndex(bucket => bucket === bucketName) + 1,
                    );
                }
                let message = 'reached '
                    + `${nUpdated >= MAX_UPDATES ? 'update' : 'scanned'} `
                    + 'count limit, resuming from this '
                    + 'point can be achieved by re-running the script with '
                    + `the bucket list "${remainingBuckets.join(',')}"`;
                if (VersionIdMarker || KeyMarker) {
                    message += ' and the following environment variables set: '
                        + `KEY_MARKER=${KeyMarker} `
                        + `VERSION_ID_MARKER=${VersionIdMarker}`;
                }
                log.info(message);
                process.exit(0);
            }
            if (VersionIdMarker || KeyMarker) {
                return true;
            }
            return false;
        },
        err => {
            bucketInProgress = null;
            if (err) {
                log.error('error marking objects for crr', { bucket });
                return cb(err);
            }
            _logProgress();
            log.info(`completed task for bucket: ${bucket}`);
            return cb();
        }
    );
}

// trigger the calls to list objects and mark them for crr
eachSeries(BUCKETS, triggerCRROnBucket, err => {
    clearInterval(logProgressInterval);
    if (err) {
        return log.error('error during task execution', { error: err });
    }
    return log.info('completed task for all buckets');
});

function stop() {
    log.warn('stopping execution');
    _logProgress();
    process.exit(1);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
