const async = require('async');
const { S3Client, ListObjectVersionsCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const http = require('http');

const { Logger } = require('werelogs');

const log = new Logger('s3utils:listObjectsByReplicationStatus');

// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { ENDPOINT } = process.env;
const LISTING_LIMIT = 1000;
let { REPLICATION_STATUS } = process.env;

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
if (!REPLICATION_STATUS) {
    REPLICATION_STATUS = 'FAILED';
}

const replicationStatusToProcess = REPLICATION_STATUS.split(',');
replicationStatusToProcess.forEach(state => {
    if (!['NEW', 'PENDING', 'COMPLETED', 'FAILED', 'REPLICA'].includes(state)) {
        log.error('invalid REPLICATION_STATUS environment: must be a '
            + 'comma-separated list of replication statuses to list, '
            + 'as NEW,PENDING,COMPLETED,FAILED,REPLICA.');
        process.exit(1);
    }
});
log.info('Objects with replication status '
    + `${replicationStatusToProcess.join(' or ')} will be listed`);

const s3 = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: ACCESS_KEY,
        secretAccessKey: SECRET_KEY,
    },
    endpoint: ENDPOINT,
    forcePathStyle: true,
    tls: false,
    requestHandler: new NodeHttpHandler({
        httpAgent: new http.Agent({ keepAlive: true }),
        requestTimeout: 60000,
    }),
});

// list object versions
function _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
    s3.send(new ListObjectVersionsCommand({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        VersionIdMarker,
        KeyMarker,
    })).then(data => cb(null, data)).catch(cb);
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
    log.info('listing objects by replication status from bucket', {
        bucket,
        replicationStatus: replicationStatusToProcess.join(',')
    });
    async.doWhilst(
        done => _listObjectVersions(
            bucketName,
            VersionIdMarker,
            KeyMarker,
            (err, data) => {
                if (err) {
                    log.error('error occured while listing', { error: err, bucketName });
                    return done(err);
                }
                const keys = _getKeys(data.Versions || []);
                return async.mapLimit(keys, 10, (k, next) => {
                    const { Key, VersionId } = k;
                    s3.send(new HeadObjectCommand({
                        Bucket: bucketName,
                        Key,
                        VersionId,
                    })).then(res => {
                        if (replicationStatusToProcess.includes(res.ReplicationStatus)) {
                            log.info('object with matching replication status found', {
                                Key,
                                ReplicationStatus: res.ReplicationStatus,
                                ...res
                            });
                        }
                        return next();
                    }).catch(next);
                }, err => {
                    if (err) {
                        return done(err);
                    }
                    VersionIdMarker = data.NextVersionIdMarker;
                    KeyMarker = data.NextKeyMarker;
                    return done();
                });
            },
        ),
        async () => {
            if (!VersionIdMarker || !KeyMarker) {
                log.debug(
                    'completed listing objects by replication status for bucket',
                    { bucket },
                );
                return false;
            }
            return true;
        },
        cb,
    );
}

async.mapSeries(
    BUCKETS,
    (bucket, done) => listBucket(bucket, done),
    err => {
        if (err) {
            log.error('error occured while listing objects by replication status', {
                error: err,
            });
        }
    },
);
