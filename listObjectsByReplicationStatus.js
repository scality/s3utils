const async = require('async');
const { S3Client, ListObjectVersionsCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const http = require('http');

const { Logger } = require('werelogs');

const LISTING_LIMIT = 1000;
const VALID_REPLICATION_STATUSES = ['NEW', 'PENDING', 'COMPLETED', 'FAILED', 'REPLICA'];

// Error messages
const ERR_NO_BUCKETS = 'No buckets given as input! Please provide a comma-separated list of buckets';
const ERR_NO_ENDPOINT = 'ENDPOINT not defined!';
const ERR_NO_ACCESS_KEY = 'ACCESS_KEY not defined';
const ERR_NO_SECRET_KEY = 'SECRET_KEY not defined';
const ERR_REPLICATION_STATUS_NOT_DEFINED = `REPLICATION_STATUS not defined! Please provide a comma-separated list of replication statuses: ${VALID_REPLICATION_STATUSES.join(',')}.`;
const ERR_INVALID_REPLICATION_STATUS = `invalid REPLICATION_STATUS: must be a comma-separated list of replication statuses: ${VALID_REPLICATION_STATUSES.join(',')}.`;

/**
 * List object versions from a bucket
 * @private
 */
function _listObjectVersions(s3, bucket, VersionIdMarker, KeyMarker, cb) {
    s3.send(new ListObjectVersionsCommand({
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        VersionIdMarker,
        KeyMarker,
    })).then(data => cb(null, data)).catch(cb);
}

/**
 * Extract keys and version IDs from version list
 * @private
 */
function _getKeys(list) {
    return list.map(v => ({
        Key: v.Key,
        VersionId: v.VersionId,
    }));
}

/**
 * List objects in a bucket by replication status
 * @private
 */
function _listBucket(s3, log, replicationStatusToProcess, bucket, cb) {
    const bucketName = bucket.trim();
    let VersionIdMarker = null;
    let KeyMarker = null;
    log.info('listing objects by replication status from bucket', {
        bucket,
        replicationStatus: replicationStatusToProcess.join(',')
    });
    async.doWhilst(
        done => _listObjectVersions(
            s3,
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

/**
 * Main function to list objects by replication status
 * @param {Object} options - Configuration options
 * @param {string} options.buckets - Comma-separated list of buckets
 * @param {string} options.accessKey - AWS access key
 * @param {string} options.secretKey - AWS secret key
 * @param {string} options.endpoint - S3 endpoint
 * @param {string} options.replicationStatus - Comma-separated replication statuses (required)
 * @param {Object} [options.logger] - Logger instance
 * @returns {Promise<void>}
 */
function listObjectsByReplicationStatus(options) {
    const {
        buckets,
        accessKey,
        secretKey,
        endpoint,
        replicationStatus,
        logger,
    } = options;

    const log = logger || new Logger('s3utils:listObjectsByReplicationStatus');

    // Validate inputs
    if (!buckets || buckets.trim().length === 0) {
        return Promise.reject(new Error(ERR_NO_BUCKETS));
    }
    if (!endpoint) {
        return Promise.reject(new Error(ERR_NO_ENDPOINT));
    }
    if (!accessKey) {
        return Promise.reject(new Error(ERR_NO_ACCESS_KEY));
    }
    if (!secretKey) {
        return Promise.reject(new Error(ERR_NO_SECRET_KEY));
    }
    if (!replicationStatus) {
        return Promise.reject(new Error(ERR_REPLICATION_STATUS_NOT_DEFINED));
    }

    const bucketList = buckets.split(',');
    const replicationStatusToProcess = replicationStatus.split(',');

    // Validate replication statuses
    for (const state of replicationStatusToProcess) {
        if (!VALID_REPLICATION_STATUSES.includes(state)) {
            return Promise.reject(new Error(ERR_INVALID_REPLICATION_STATUS));
        }
    }

    log.info('Objects with replication status '
        + `${replicationStatusToProcess.join(' or ')} will be listed`);

    const s3 = new S3Client({
        region: 'us-east-1',
        credentials: {
            accessKeyId: accessKey,
            secretAccessKey: secretKey,
        },
        endpoint,
        forcePathStyle: true,
        tls: false,
        requestHandler: new NodeHttpHandler({
            httpAgent: new http.Agent({ keepAlive: true }),
            requestTimeout: 60000,
        }),
    });

    return new Promise((resolve, reject) => {
        async.mapSeries(
            bucketList,
            (bucket, done) => _listBucket(s3, log, replicationStatusToProcess, bucket, done),
            err => {
                if (err) {
                    return reject(err);
                }
                // Cleanup S3 client
                if (s3 && typeof s3.destroy === 'function') {
                    s3.destroy();
                }
                return resolve();
            },
        );
    });
}

module.exports = {
    listObjectsByReplicationStatus,
    ERR_NO_BUCKETS,
    ERR_NO_ENDPOINT,
    ERR_NO_ACCESS_KEY,
    ERR_NO_SECRET_KEY,
    ERR_REPLICATION_STATUS_NOT_DEFINED,
    ERR_INVALID_REPLICATION_STATUS,
};

if (require.main === module) {
    const log = new Logger('s3utils:listObjectsByReplicationStatus');

    const BUCKETS = process.argv[2] || null;
    const { ACCESS_KEY, SECRET_KEY, ENDPOINT, REPLICATION_STATUS } = process.env;

    listObjectsByReplicationStatus({
        buckets: BUCKETS,
        accessKey: ACCESS_KEY,
        secretKey: SECRET_KEY,
        endpoint: ENDPOINT,
        replicationStatus: REPLICATION_STATUS,
        logger: log,
    }).then(() => {
        log.info('Completed successfully');
        process.exit(0);
    }).catch(err => {
        log.error('Failed with error', { error: err.message });
        process.exit(1);
    });
}
