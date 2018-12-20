const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');

const { Logger } = require('./logging');
const log = new Logger('s3utils:crrUsageMetrics');
/* eslint-disable no-console */

// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
const WORKERS = (process.env.WORKERS &&
                 Number.parseInt(process.env.WORKERS, 10)) || 10;
const LISTING_LIMIT = 1000;
const LOG_PROGRESS_INTERVAL_MS = 10000;

if (!BUCKETS || BUCKETS.length === 0) {
    log.error('No buckets given as input! Please provide ' +
        'a comma-separated list of buckets');
    process.exit(1);
}
if (!ENDPOINT) {
    log.error('ENDPOINT not defined!');
    process.exit(1);
}
if (!ACCESS_KEY) {
    log.error('ACCESS_KEY not defined');
    process.exit(1);
}
if (!SECRET_KEY) {
    log.error('SECRET_KEY not defined');
    process.exit(1);
}

// mapping "bucketName:CanonicalID" => { completedCount,
// completedBytes }, aggregated for all objects with replication
// status COMPLETED, that will be dumped at the end in JSON format to
// stdout.
const metrics = {};

const s3 = new AWS.S3({
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    region: 'us-east-1',
    sslEnabled: false,
    endpoint: ENDPOINT,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
    httpOptions: {
        maxRetries: 0,
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
});

let nScanned = 0;
let nErrors = 0;
let bucketInProgress = null;

function _logProgress() {
    log.info(`progress update: ${nScanned} scanned, ${nErrors} errors, ` +
             `bucket in progress: ${bucketInProgress || '(none)'}`);
}

const logProgressInterval = setInterval(_logProgress, LOG_PROGRESS_INTERVAL_MS);

// list object versions
function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    s3.listObjectVersions({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        VersionIdMarker,
        KeyMarker,
    }, cb);
}

function listBucket(bucket, cb) {
    const bucketName = bucket.trim();
    let VersionIdMarker = null;
    let KeyMarker = null;
    bucketInProgress = bucket;
    log.info('aggregating CRR usage metrics from bucket', { bucket });
    async.doWhilst(
        done => _listObjectVersions(bucketName, VersionIdMarker, KeyMarker,
            (err, data) => {
                if (err) {
                    log.error('error listing object versions', {
                        error: err, bucketName });
                    return done(err);
                }
                return async.eachLimit(data.Versions, WORKERS, (k, next) => {
                    const { Key, VersionId, Owner } = k;
                    s3.headObject({ Bucket: bucketName, Key, VersionId },
                        (err, res) => {
                            ++nScanned;
                            if (err) {
                                ++nErrors;
                                return next(err);
                            }
                            if (res.ReplicationStatus === 'COMPLETED') {
                                const metricsKey = `${bucketName}:${Owner.ID}`;
                                if (!metrics[metricsKey]) {
                                    metrics[metricsKey] = {
                                        completedCount: 0,
                                        completedBytes: 0,
                                    };
                                }
                                const metricsValue = metrics[metricsKey];
                                metricsValue.completedCount += 1;
                                metricsValue.completedBytes +=
                                    res.ContentLength;
                            }
                            return next();
                        });
                }, err => {
                    if (err) {
                        return done(err);
                    }
                    VersionIdMarker = data.NextVersionIdMarker;
                    KeyMarker = data.NextKeyMarker;
                    return done();
                });
            }),
        () => {
            if (!VersionIdMarker || !KeyMarker) {
                bucketInProgress = null;
                log.debug('completed aggregating CRR usage metrics for bucket',
                    { bucket });
                _logProgress();
                return false;
            }
            return true;
        },
        cb
    );
}

async.eachSeries(BUCKETS, (bucket, done) => listBucket(bucket, done),
    err => {
        clearInterval(logProgressInterval);
        if (err) {
            log.error('error occurred while aggregating CRR usage metrics', {
                error: err,
            });
        }
        console.log(JSON.stringify(metrics));
    }
);

function stop() {
    log.warn('stopping execution');
    _logProgress();
    process.exit(1);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);

/* eslint-enable no-console */
