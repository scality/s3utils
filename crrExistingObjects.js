const http = require('http');

const AWS = require('aws-sdk');
const { doWhilst, mapLimit, waterfall } = require('async');

const { Logger } = require('werelogs');

const BackbeatClient = require('./BackbeatClient');

const log = new Logger('s3utils::emptyBucket');
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
const LISTING_LIMIT = 1000;

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
const options = {
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
        maxRetries: 0,
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
};
const s3 = new AWS.S3(options);


const bb = new BackbeatClient(options);

function _markObjectPending(bucket, key, versionId, repConfig, cb) {
    return waterfall([
        // get object blob
        next => bb.getMetadata({
            Bucket: bucket,
            Key: key,
            VersionId: versionId,
        }, next),
        // update replication info and put back object blob
        (mdRes, next) => {
            const objMD = JSON.parse(mdRes.Body);
            if (objMD.replicationInfo.status !== '') {
                // skip object since it's already marked for crr
                return next();
            }
            const { Rules, Role } = repConfig;
            const destination = Rules[0].Destination.Bucket;
            const storageClass = Rules[0].Destination.StorageClass;
            // set replication properties
            const ops = objMD['content-length'] === 0 ? ['METADATA'] :
                ['METADATA', 'DATA'];
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
                storageType: '',
            };
            objMD.replicationInfo = replicationInfo;
            const mdBlob = JSON.stringify(objMD);
            return bb.putMetadata({
                Bucket: bucket,
                Key: key,
                ContentLength: Buffer.byteLength(mdBlob),
                Body: mdBlob,
            }, next);
        },
    ], cb);
}

// list object versions
function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    return s3.listObjectVersions({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
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
        (repConfig, next) => mapLimit(versions, 10, (i, apply) => {
            const { Key, VersionId } = i;
            _markObjectPending(bucket, Key, VersionId, repConfig, apply);
        }, next),
    ], cb);
}

function triggerCRR(bucketName, cb) {
    const bucket = bucketName.trim();
    let VersionIdMarker = null;
    let KeyMarker = null;
    doWhilst(
        done => _listObjectVersions(bucket, VersionIdMarker, KeyMarker,
            (err, data) => {
                if (err) {
                    log.error('error listing object versions', { error: err });
                    return done(err);
                }
                VersionIdMarker = data.NextVersionIdMarker;
                KeyMarker = data.NextKeyMarker;
                return _markPending(bucket, data.Versions, done);
            }),
        () => {
            if (VersionIdMarker || KeyMarker) {
                return true;
            }
            return false;
        },
        err => {
            if (err) {
                log.error('error marking objects for crr', { bucket });
                return cb(err);
            }
            log.info(`completed task for bucket: ${bucket}`);
            return cb();
        });
}

// trigger the calls to list objects and mark them for crr
mapLimit(BUCKETS, 1, triggerCRR, err => {
    if (err) {
        return log.error('error during task execution', { error: err });
    }
    return log.info('completed task for all buckets');
});
