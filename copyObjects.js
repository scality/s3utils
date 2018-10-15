const http = require('http');
const async = require('async');
const AWS = require('aws-sdk');
const { Logger } = require('werelogs');

const log = new Logger('s3utils::emptyBucket');
// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
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
const LISTING_LIMIT = 1000;

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
        maxRetries: 0,
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
});

// list object versions
function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    return s3.listObjectVersions({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        VersionIdMarker,
        KeyMarker,
    }, cb);
}

// return object with key and version_id
function _getKeys(keys) {
    return keys.map(v => ({
        Key: v.Key,
        VersionId: v.VersionId,
    }));
}

// copy all versions of an object
function _deleteVersions(bucket, objectsToDelete, cb) {
    const params = {
        Bucket: bucket,
        Delete: { Objects: objectsToDelete },
    };
    s3.deleteObjects(params, err => {
        if (err) {
            log.error('batch delete err', err);
            return cb(err);
        }
        objectsToDelete.forEach(v => log.info(`deleted key: ${v.Key}`));
        return cb();
    });
}

function _copyObjects(bucket, cb) {
    let VersionIdMarker = null;
    let KeyMarker = null;
    async.doWhilst(
        done => _listObjectVersions(bucket, VersionIdMarker, KeyMarker,
            (err, data) => {
                if (err) {
                    return done(err);
                }
                VersionIdMarker = data.NextVersionIdMarker;
                KeyMarker = data.NextKeyMarker;
                const keysToDelete = _getKeys(data.Versions);
                const markersToDelete = _getKeys(data.DeleteMarkers);
                return _deleteVersions(bucket,
                    keysToDelete.concat(markersToDelete), done);
            }),
        () => {
            if (VersionIdMarker || KeyMarker) {
                return true;
            }
            return false;
        },
        cb
    );
}

function copyObjects(buckets) {
    async.mapLimit(buckets, 1, _copyObjects, err => {
        if (err) {
            return log.error('error occured deleting objects', err);
        }
        return log.info('completed cleaning up the given buckets');
    });
}

copyObjects(BUCKETS);
