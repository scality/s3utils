#!/usr/bin/env node
'use strict'; // eslint-disable-line strict

const { models, versioning } = require('arsenal');
const async = require('async');
const AWS = require('aws-sdk');
const bucketclient = require('bucketclient');
const commander = require('commander');
const http = require('http');
const werelogs = require('werelogs');

werelogs.configure({ level: 'info', dump: 'error' });
const logger = new werelogs.Logger('s3utils:migrateBuckets')
    .newRequestLogger();
const { BucketInfo } = models;
const versionIdUtils = versioning.VersionID;

// configurable params
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;

commander.version('1.0.0')
.option('-b, --bucket-name <bucketName>', 'Name of the bucket')
.option('-s, --src-canonical-id <srcCanonicalId>',
    'Canonical id of source account')
.option('-d, --dest-canonical-id <destCanonicalId>',
    'Canonical id of destination account')
.option('-o, --dest-owner-name <destOwnerName>',
    'Owner display name of destination account')
.option('-e, --endpoint <endpoint>', 'S3Connector endpoint')
.option('-u, --bucketd <bucketd>', 'Bootstrap list of bucketd endpoints')
.option('--https-key [httpsKey]', 'HTTPS key')
.option('--https-cert [httpsCert]', 'HTTPS cert')
.option('--https-ca [httpsCa]', 'HTTPS ca')
.option('-p, --profile [profile]', 'AWS credentials')
.parse(process.argv);

const {
    bucketName,
    srcCanonicalId,
    destCanonicalId,
    destOwnerName,
    endpoint,
    bucketd,
    httpsKey,
    httpsCert,
    httpsCa,
    profile,
} = commander;

if ((!ACCESS_KEY || !SECRET_KEY) && !profile) {
    logger.error('Missing credentials. Set ACCESS_KEY & SECRET_KEY env ' +
        'variables or pass --profile option');
    process.exit(1);
}

if (!bucketName || !srcCanonicalId ||
!destCanonicalId || !destOwnerName || !bucketd) {
    logger.error('Missing command line parameter');
    commander.outputHelp();
    process.exit(1);
}

let keyInProgress;
let versionIdInProgress;
let nErrors = 0;
let nObjProcessed = 0;

const s3Opts = {
    region: 'us-east-1',
    sslEnabled: false,
    endpoint,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
    httpOptions: {
        maxRetries: 10,
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
};
if (ACCESS_KEY && SECRET_KEY) {
    s3Opts.accessKeyId = ACCESS_KEY;
    s3Opts.secretAccessKey = SECRET_KEY;
}
if (profile) {
    const credentials = new AWS.SharedIniFileCredentials({ profile });
    AWS.config.credentials = credentials;
}

const s3 = new AWS.S3(s3Opts);

let mdclient;
try {
    if (httpsKey && httpsCert) {
        mdclient = new bucketclient.RESTClient(
            bucketd, undefined, true, httpsKey, httpsCert, httpsCa);
    } else {
        mdclient = new bucketclient.RESTClient(bucketd, undefined, false);
    }
} catch (err) {
    logger.error(`Could not instantiate bucketclient: ${err.message}`);
    process.exit(1);
}

const splitter = '..|..';

function _createUsersBucketKey(bucketName, id) {
    return `${id}${splitter}${bucketName}`;
}

function _changeBucketAccount(bucket, log, cb) {
    const {
        bucketName,
        destCanonicalId,
        destOwnerName,
    } = bucket;

    mdclient.getBucketAttributes(bucketName, log.getSerializedUids(),
    (err, data) => {
        if (err) {
            ++nErrors;
            log.error(`Error getting bucket attributes: ${err}`);
            return cb(err);
        }
        const bucketMD = BucketInfo.fromObj(BucketInfo.deSerialize(data));
        const versioningEnabled = bucketMD.isVersioningEnabled();
        const bucketId = bucketMD.getOwner();
        const bucketOwner = bucketMD.getOwnerDisplayName();
        if (bucketId === destCanonicalId && bucketOwner === destOwnerName) {
            log.info(`Bucket ${bucketName} id already updated`);
            return cb(null, versioningEnabled);
        }
        bucketMD.setOwner(destCanonicalId);
        bucketMD.setOwnerDisplayName(destOwnerName);
        return mdclient.putBucketAttributes(bucketName, log.getSerializedUids(),
        bucketMD.serialize(), err => {
            if (err) {
                ++nErrors;
                log.error(`Error updating bucket ${bucketName} ` +
                    `metadata: ${err}`);
                return cb(err);
            }
            log.info(`Bucket ${bucketName} id successfully updated`);
            return cb(null, versioningEnabled);
        });
    });
}

function _changeUsersBucket(bucket, log, cb) {
    const metaBucket = 'users..bucket';
    const serUids = log.getSerializedUids();
    const { bucketName, srcCanonicalId, destCanonicalId } = bucket;

    return mdclient.listObject(metaBucket, serUids, {}, (err, data) => {
        if (err) {
            ++nErrors;
            log.error(`Error listing metadata bucket: ${err}`);
            return cb(err);
        }
        const objectList = JSON.parse(data);

        const searchKey = _createUsersBucketKey(bucketName, srcCanonicalId);
        const matchingBucket = objectList.Contents.find(object =>
            object.key === searchKey);

        if (!matchingBucket) {
            log.info(`users..bucket object ${bucketName} does not need ` +
            'to be updated');
            return cb();
        }
        return async.waterfall([
            next => mdclient.getObject(metaBucket, matchingBucket.key, serUids,
            (err, objectMD) => {
                if (err) {
                    log.error('Error getting users..bucket object ' +
                        `metadata: ${err}`);
                    return next(err);
                }
                const newObjectName = _createUsersBucketKey(
                    bucketName, destCanonicalId);
                return next(null, newObjectName, objectMD);
            }),
            (newObjectName, objectMD, next) => mdclient.putObject(
            metaBucket, newObjectName, objectMD, serUids, err => {
                if (err) {
                    log.error('Error updating users..bucket object ' +
                        `${matchingBucket.key}: ${err}`);
                    return next(err);
                }
                log.info(`users..bucket object ${matchingBucket.key} ` +
                    'successfully updated');
                return next();
            }),
            next => mdclient.deleteObject(metaBucket, matchingBucket.key,
            serUids, err => {
                if (err) {
                    log.error('Error deleting old bucket entry from ' +
                        `users bucket: ${err}`);
                } else {
                    log.info('Successfully deleted old bucket entry ' +
                        'from users bucket');
                }
                return next();
            }),
        ], err => {
            if (err) {
                ++nErrors;
                return cb(err);
            }
            log.info('Successfully updated users..bucket entry');
            return cb();
        });
    });
}

function _changeOneObjectAccount(bucket, object, log, cb) {
    const {
        bucketName,
        destCanonicalId,
        destOwnerName,
    } = bucket;
    const { Key, VersionId } = object;
    keyInProgress = Key;
    versionIdInProgress = VersionId;
    const decodedId = VersionId ? versionIdUtils.decode(VersionId) : undefined;

    // if versiondId is undefined, retrieval will return most current object md
    const getOptions = { versionId: decodedId };

    mdclient.getObject(bucketName, Key, log.getSerializedUids(),
    (err, data) => {
        if (err) {
            ++nErrors;
            log.error(`Error getting object ${Key} metadata: ${err}`);
            return cb(err);
        }
        const objectMD = JSON.parse(data);
        const objectId = objectMD['owner-id'];
        const objectOwner = objectMD['owner-display-name'];
        if (objectId === destCanonicalId && objectOwner === destOwnerName) {
            log.info(`Object ${Key} id already updated`);
            return cb();
        }
        objectMD['owner-id'] = destCanonicalId;
        objectMD['owner-display-name'] = destOwnerName;
        const putOptions = {};
        if (VersionId) {
            putOptions.versionId = decodedId;
        }
        return mdclient.putObject(bucketName, Key, JSON.stringify(objectMD),
        log.getSerializedUids(), err => {
            if (err) {
                ++nErrors;
                log.error(`Error updating object ${Key} metadata: ${err}`);
                return cb(err);
            }
            ++nObjProcessed;
            log.info(`Object ${Key}, Version ID ${VersionId} is ` +
                'successfully updated');
            return cb();
        }, putOptions);
    }, getOptions);
}

function _changeObjectsAccount(bucket, log, cb) {
    const bucketName = bucket.bucketName;
    let moreObjects = true;
    let marker = null;
    async.whilst(
        () => moreObjects, done => {
            const listParams = { Bucket: bucketName };
            if (marker) {
                listParams.Marker = marker;
            }
            // if versioning is not enabled on bucket, VersionId will be null
            s3.listObjectVersions(listParams, (err, data) => {
                if (err) {
                    ++nErrors;
                    return done(err);
                }
                if (data.IsTruncated) {
                    marker = data.Versions[data.Versions.length - 1].Key;
                } else {
                    moreObjects = false;
                }
                return async.each(data.Versions, (object, callback) =>
                    _changeOneObjectAccount(bucket, object, log, callback),
                    err => {
                        if (err) {
                            return done(err);
                        }
                        log.info(`All objects in bucket ${bucketName} ` +
                            'successfully updated');
                        return done();
                    });
            });
        },
        err => {
            if (err) {
                log.error('Error updating objects from bucket ' +
                    `${bucketName}: ${err}`);
                return cb(err);
            }
            return cb();
        }
    );
}

function migrateBuckets(bucket, cb) {
    async.waterfall([
        next => _changeBucketAccount(bucket, logger, next),
        (versioningEnabled, next) => {
            /* eslint-disable no-param-reassign */
            bucket.versioningEnabled = versioningEnabled;
            _changeObjectsAccount(bucket, logger, next);
        },
    ], err => {
        if (err) {
            logger.error(`Bucket ${bucketName} failed to process: ${err}`);
            return cb(err);
        }
        logger.info(`Bucket ${bucketName} transfer is complete`);
        return _changeUsersBucket(bucket, logger, cb);
    });
}

migrateBuckets(commander, err => {
    if (err) {
        logger.error(`Bucket migration failed: ${err}. ` +
            'Please run script again');
    } else {
        logger.info('Completed bucket migration');
    }
});

function _logProgress(message) {
    logger.info(message, {
        errors: nErrors,
        keyInProgress: keyInProgress || null,
        objectsProcessed: nObjProcessed,
        versionIdInProgress: versionIdInProgress || null,
    });
}

function stop() {
    logger.warn('stopping execution');
    _logProgress();
    process.exit(1);
}

process.on('SIGINT', stop);
process.on('SIGTERM', stop);

