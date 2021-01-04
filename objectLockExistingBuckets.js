const { MetadataWrapper } = require('arsenal').storage.metadata;
const { series } = require('async');
const { Logger } = require('werelogs');

const USAGE = `
objectLockExistingBuckets.js

This script enables Object Lock on existing bucket(s)

Usage:
    node objectLockExistingBuckets.js bucket1[,bucket2...]
`;

const log = new Logger('s3utils::objectLockExistingBuckets');
const replicaSetHosts = process.env.MONGODB_REPLICASET || 'localhost:27017';
const replicaSet = 'rs0';
const writeConcern = 'majority';
const readPreference = 'primary';
const database = process.env.MONGODB_DATABASE || 'metadata';
const implName = 'mongodb';
const params = {
    mongodb: {
        replicaSetHosts,
        writeConcern,
        replicaSet,
        readPreference,
        database,
    },
};
if (process.env.MONGODB_AUTH_USERNAME &&
    process.env.MONGODB_AUTH_PASSWORD) {
    params.authCredentials = {
        username: process.env.MONGODB_AUTH_USERNAME,
        password: process.env.MONGODB_AUTH_PASSWORD,
    };
}
const metadataClient = new MetadataWrapper(implName, params, null, log);

const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const LOG_PROGRESS_INTERVAL_MS = 10000;

if (!BUCKETS || BUCKETS.length === 0) {
    console.error('No buckets given as input, please provide ' +
                  'a comma-separated list of buckets on the command line');
    console.error(USAGE);
    process.exit(1);
}

let nSkipped = 0;
let nUpdated = 0;
let nErrors = 0;
let bucketInProgress = null;

function _logProgress() {
    log.info('progress update', {
        updated: nUpdated,
        skipped: nSkipped,
        errors: nErrors,
        bucket: bucketInProgress || null,
    });
}

const logProgressInterval = setInterval(_logProgress, LOG_PROGRESS_INTERVAL_MS);

function enableObjectLockOnBucket(bucketName, cb) {
    const bucket = bucketName.trim();
    bucketInProgress = bucket;
    log.info(`starting task for bucket: ${bucket}`);

    return metadataClient.getBucket(bucket, log, (err, bucketMD) => {
        if (err) {
            log.error('error getting bucket metadata', { error: err });
            return cb(err);
        }
        if (!bucketMD.isVersioningEnabled()) {
            const versioningConfig = {
                Status: 'Enabled',
                MfaDelete: 'Disabled',
            };
            bucketMD.setVersioningConfiguration(versioningConfig);
        }
        bucketMD.setObjectLockEnabled(true);
        return metadataClient.updateBucket(bucket, bucketMD, log, err => {
            if (err) {
                log.error('error updating bucket metadata', { error: err });
                return cb(err);
            }
            return cb();
        });
    });
}

series([
    next => metadataClient.setup(next),
    next => eachSeries(BUCKETS, enableObjectLockOnBucket, next),
    next => metadataClient.close(next),
], err => {
    clearInterval(logProgressInterval);
    if (err) {
        return log.error('error during task execution', { error: err });
    }
    console.log('Object Lock enabled for specified buckets');
    return log.info('completed task for all buckets');
});
