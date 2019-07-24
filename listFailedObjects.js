const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');
const { Logger } = require('werelogs');

const log = new Logger('s3utils:listFailedObjects');
/* eslint-disable no-console */

// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { ENDPOINT } = process.env;
const LISTING_LIMIT = 1000;

if (!BUCKETS || BUCKETS.length === 0) {
    log.error('No buckets given as input! Please provide '
        + 'a comma-separated list of buckets');
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

// list object versions
function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    s3.listObjectVersions({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        VersionIdMarker,
        KeyMarker,
    }, cb);
}

// return object with key and version_id
function _getKeys(list) {
    return list.map(v => ({
        Key: v.Key,
        VersionId: v.VersionId,
    }));
}

function listBucket(bucket, cb) {
    const bucketName = bucket.trim();
    let VersionIdMarker = null;
    let KeyMarker = null;
    log.info('listing failed objects from bucket', { bucket });
    async.doWhilst(
        done => _listObjectVersions(bucketName, VersionIdMarker, KeyMarker,
            (err, data) => {
                if (err) {
                    log.error('error occured while listing',
                    { error: err, bucketName });
                    return done(err);
                }
                const keys = _getKeys(data.Versions);
                return async.mapLimit(keys, 10, (k, next) => {
                    const { Key, VersionId } = k;
                    s3.headObject({ Bucket: bucketName, Key, VersionId },
                        (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            if (res.ReplicationStatus === 'FAILED') {
                                console.log(Object.assign({ Key }, res));
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
                log.debug('completed listing failed objects for bucket',
                    { bucket });
                return false;
            }
            return true;
        },
        cb,
    );
}

async.mapSeries(BUCKETS, (bucket, done) => listBucket(bucket, done), err => {
    if (err) {
        log.error('error occured while listing failed objects', {
            error: err,
        });
    }
},
);
/* eslint-enable no-console */
