const http = require('http');
const async = require('async');
const AWS = require('aws-sdk');
const { Logger } = require('werelogs');

const log = new Logger('s3utils::cleanupVersions');
// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
const REMOVE_DELETE_MARKERS = process.env.REMOVE_DELETE_MARKERS;
const LISTING_PREFIX = process.env.LISTING_PREFIX;
let WORKERS = 1;
if (process.env.WORKERS) {
    if (Number.isNaN(Number.parseInt(process.env.WORKERS, 10))) {
        log.fatal('WORKERS must be >= 1');
        process.exit(1);
    }
    WORKERS = Number.parseInt(process.env.WORKERS, 10);
}
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
        Prefix: LISTING_PREFIX || undefined,
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

// delete all versions of an object
function _deleteVersions(bucket, objectsToDelete, cb) {
    if (objectsToDelete.length === 0) {
        log.info('no archived objects to delete');
        return cb();
    }
    // multi object delete can delete max 1000 objects
    const params = {
        Bucket: bucket,
        Delete: { Objects: objectsToDelete },
    };
    s3.deleteObjects(params, err => {
        if (err) {
            log.error('batch delete err', {
                error: err,
                bucket,
                objectsToDelete,
            });
            return cb(err);
        }
        objectsToDelete.forEach(v =>
            log.info(`deleted key: ${v.Key} version: ${v.VersionId}`));
        return cb();
    });
}

const deleteQueue = async.queue((params, done) => {
    const { bucket, data } = params;
    const { Versions, DeleteMarkers } = data;
    // skip latest versions
    const keysToDelete = _getKeys(Versions.filter(i => i.IsLatest === false));
    // skip latest delete markers
    let markersToDelete = _getKeys(DeleteMarkers.filter(
        i => i.IsLatest === false));
    // remove all delete markers, use this option once all the
    // archived versions are deleted
    if (REMOVE_DELETE_MARKERS) {
        markersToDelete = _getKeys(data.DeleteMarkers);
    }
    return _deleteVersions(bucket, keysToDelete.concat(markersToDelete), done);
}, WORKERS);


function cleanupVersions(bucket, cb) {
    let VersionIdMarker = null;
    let KeyMarker = null;
    async.doWhilst(
        done => _listObjectVersions(bucket, VersionIdMarker, KeyMarker,
            (err, data) => {
                if (err) {
                    return done(err);
                }
                deleteQueue.push({ bucket, data });
                return done();
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

function _cleanupBucket(bucket, cb) {
    async.parallel([
        done => cleanupVersions(bucket, done),
    ], err => {
        if (err) {
            log.error('error occured deleting objects', err);
            return cb(err);
        }
        log.info(`completed cleaning up of bucket: ${bucket}`);
        return cb();
    });
}

function cleanupBuckets(buckets) {
    async.mapLimit(buckets, 1, _cleanupBucket, err => {
        if (err) {
            return log.error('error occured deleting objects', err);
        }
        return log.info('completed cleaning up the given buckets');
    });
}

cleanupBuckets(BUCKETS);
