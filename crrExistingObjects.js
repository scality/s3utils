const http = require('http');

const AWS = require('aws-sdk');
const { mapLimit, waterfall } = require('async');

const { Logger } = require('werelogs');

const BackbeatClient = require('./BackbeatClient');

const log = new Logger('s3utils::emptyBucket');
const BUCKET = process.argv[2];
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
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
};
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
            const { Rules, Role } = repConfig;
            const destination = Rules[0].Destination[0].Bucket;
            const storageClass = Rules[0].Destination[0].StorageClass[0];
            // set replication properties
            const ops = objMD['content-length'] === 0 ? ['METADATA'] :
                ['METADATA', 'DATA'];
            const backends = [{
                site: '',
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
            Object.assign(objMD.replicationInfo, replicationInfo);
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


// Here's the logic that triggers everything
waterfall([
    next => s3.listObjectVersions({ Bucket: BUCKET }, next),
    (data, next) => s3.getBucketReplication({ Bucket: BUCKET }, (err, res) => {
        if (err) {
            return next(err);
        }
        return next(null, data, res.ReplicationConfiguration);
    }),
    (data, repConfig, next) => mapLimit(data.Versions, 10, (i, apply) => {
        const { Key, VersionId } = i;
        _markObjectPending(BUCKET, Key, VersionId, repConfig, apply);
    }, next),
], err => {
    if (err) {
        return log.error('error occured', err);
    }
    return log.info('completed marking objects for crr');
});
