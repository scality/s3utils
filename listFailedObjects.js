const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');

const { Logger } = require('werelogs');
const log = new Logger('s3utils:listFailedObjects');
/* eslint-disable no-console */

// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
const QUIET_MODE = !!process.env.QUIET_MODE;
const LISTING_LIMIT = 1000;

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

function logger(msg, val) {
    if (!QUIET_MODE) {
        console.log(msg, val);
    }
}

function recursiveListing(bucket, cb) {
    const bucketName = bucket.trim();
    let VersionIdMarker = null;
    let KeyMarker = null;
    log.info('listing failed objects from bucket', { bucket });
    async.doWhilst(
        done => _listObjectVersions(bucketName, VersionIdMarker, KeyMarker,
            (err, data) => {
                if (err) {
                    log.error('error occured while listing', {
                        error: err, bucket });
                    return done(err);
                }
                data.Versions.forEach(d => {
                    if (d.ReplicationStatus === 'FAILED') {
                        console.log(d);
                    }
                });
                VersionIdMarker = data.NextVersionIdMarker;
                KeyMarker = data.NextKeyMarker;
                logger('VersionIdMarker', VersionIdMarker);
                logger('KeyMarker', KeyMarker);
                return done();
            }),
        () => {
            if (!VersionIdMarker || !KeyMarker) {
                log.info('completed listing failed objects for bucket',
                    { bucket });
                return false;
            }
            return true;
        },
        cb
    );
}

async.mapSeries(BUCKETS, (bucket, done) => recursiveListing(bucket, done),
    err => {
        if (err) {
            return log.error('error occured while listing failed objects', {
                error: err,
            });
        }
        return log.info('completed listing failed objects');
    }
);
/* eslint-enable no-console */
