#!/usr/bin/env node
'use strict'; // eslint-disable-line strict

const { BucketInfo } = require('arsenal').models;
const async = require('async');
const AWS = require('aws-sdk');
const bucketclient = require('bucketclient');
const commander = require('commander');
const fs = require('fs');
const http = require('http');

const werelogs = require('werelogs');
werelogs.configure({ level: 'info', dump: 'error' });
const logger = new werelogs.Logger('s3utils:migrateBuckets')
    .newRequestLogger();

const skipError = new Error('skip');

/* expected input JSON structure:
* {
*   bucketd: {
*       bootstrap: <>,
*   },
*   https: {
*       key: <>,
*       cert: <>,
*       ca: <>,
*   }
* }
*/

// configurable params
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;

commander
.version('0.0.1')
.option('-b, --bucket-name <bucketName>', 'Name of the bucket')
.option('-s, --src-canonical-id <srcCanonicalId',
    'Canonical id of source account')
.option('-o, --src-owner-name <srcOwnerName>',
    'Owner display name of source account')
.option('-d, --dest-canonical-id <destCanonicalId',
    'Canonical id of destination account')
.option('-e, --dest-owner-name <destOwnerName>',
    'Owner display name of destination account')
.option('-c, --config-path <configPath>', "Path to metadata config")
.parse(process.argv);

const {
    bucketName,
    srcCanonicalId,
    srcOwnerName,
    destCanonicalId,
    destOwnerName,
    configPath,
} = commander;

if (!bucketName || !srcCanonicalId || !srcOwnerName ||
!destCanonicalId || !destOwnerName || !configPath) {
    logger.error('Missing command line parameter');
    commander.outputHelp();
    process.exit(1);
}

let config;
try {
    const data = fs.readFileSync(configPath, { encoding: 'utf-8' });
    config = JSON.parse(data);
} catch (err) {
    logger.error(`Could not parse provided config file: ${err.message}`);
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

const { bootstrap } = config.bucketd;
if (!bootstrap || bootstrap.length === 0) {
    logger.error('Bucketd bootstrap list is empty');
}
let mdclient;
try {
    if (config.https) {
        const { key, cert, ca } = config.https;
        mdclient = new bucketclient.RESTClient(
            bootstrap, undefined, true, key, cert, ca);
    } else {
        mdclient = new bucketclient.RESTClient(bootstrap, undefined, false);
    }
} catch (err) {
    logger.error(`Could not instantiate bucketclient: ${err.message}`);
    process.exit(1);
}

const splitter = '..|..';

function _parseUsersBucketKey(key) {
    const splitStr = key.split(splitter);
    return { canonicalId: splitStr[0], bucketName: splitStr[1] };
}

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
            log.error(`Error getting bucket attributes: ${err}`);
            return cb(err);
        }
        const bucketMD = BucketInfo.fromObj(BucketInfo.deSerialize(data));
        const versioningEnabled = bucketMD.isVersioningEnabled();
        const bucketId = bucketMD.getOwner();
        const bucketOwner = bucketMD.getOwnerDisplayName();
        if (bucketId === destCanonicalId && bucketOwner === destOwnerName) {
            log.debug(`Bucket ${bucketName} id already updated`);
            return cb(null, versioningEnabled);
        }
        bucketMD.setOwner(destCanonicalId);
        bucketMD.setOwnerDisplayName(destOwnerName);
        return mdclient.putBucketAttributes(bucketName, log.getSerializedUids(),
        bucketMD.serialize(), err => {
            if (err) {
                log.error(`Error updating bucket ${bucketName} ` +
                    `metadata: ${err}`);
                return cb(err);
            }
            log.debug(`Bucket ${bucketName} id successfully updated`);
            return cb(null, versioningEnabled);
        });
    });
}

function _changeUsersBucket(bucket, log, cb) {
    const metaBucket = 'users..bucket';
    const serUids = log.getSerializedUids();
    const { bucketName, srcCanonicalId, destCanonicalId } = bucket;

    return mdclient.listObject(metaBucket, serUids, {}, (err, data) => {
        const objectList = JSON.parse(data);

        const searchKey = _createUsersBucketKey(bucketName, srcCanonicalId);
        const matchingBucket = objectList.Contents.find(object =>
            object.key === searchKey);
        
        if (!matchingBucket) {
            log.debug(`users..bucket object ${bucketName} does not need ` +
            'to be updated');
            return cb();
        }
        return async.waterfall([
            next => mdclient.getObject(metaBucket, matchingBucket.key, serUids,
            (err, objectMD) => {
                if (err) {
                    log.error(`Error getting object metadata: ${err}`);
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
                        `${objectName}: ${err}`);
                    return next(err);
                }
                log.debug(`users..bucket object ${matchingBucket.key} ` +
                    'successfully updated');
                return next();
            }),
            next => mdclient.deleteObject(metaBucket, matchingBucket.key,
            serUids, err => {
                if (err) {
                    log.error('Error deleting old bucket entry from ' +
                        `users bucket: ${err}`);
                } else {
                    log.debug('Successfully deleted old bucket entry ' +
                        'from users bucket');
                }
                return next();
            }),
        ], err => {
            if (err) {
                return done(err);
            }
            log.debug('Successfully updated users..bucket entry');
            return cb();
        });
    });
}

function _changeOneObjectAccount(bucket, object, log, cb) {
    const {
        bucketName,
        srcCanonicalId,
        srcOwnerName,
        destCanonicalId,
        destOwnerName,
    } = bucket;
    const { Key, VersionId } = object;

    // if VersiondId is undefined, retrieval will return most current object md
    const getOptions = { versionId: VersionId };

    mdclient.getObject(bucketName, Key, log.getSerializedUids(),
    (err, data) => {
        const objectMD = JSON.parse(data);
        if (err) {
            log.error(`Error getting object metadata: ${err}`);
            return cb(err);
        }
        const objectId = objectMD['owner-id'];
        const objectOwner = objectMD['owner-display-name'];
        if (objectId === destCanonicalId && objectOwner === destOwnerName) {
            log.debug(`Object ${Key} id already updated`);
            return cb();
        } else if (objectId !== srcCanonicalId
        && objectOwner !== srcOwnerName) {
            log.info(`Object ${Key} id does not match provided id ` +
                'so object is not being updated');
        }
        objectMD['owner-id'] = destCanonicalId;
        objectMD['owner-display-name'] = destOwnerName;
        const putOptions = {};
        if (VersionId) {
            putOptions.versionId = VersionId;
        }
        return mdclient.putObject(bucketName, Key, JSON.stringify(objectMD),
        log.getSerializedUids(), err => {
            if (err) {
                log.error(`Error updating object ${Key} metadata: ${err}`);
                return cb(err);
            }
            log.debug(`Object ${Key} id successfully updated`);
            return cb();
        }, putOptions);
    }, getOptions);
}

function _changeObjectsAccount(bucket, log, cb) {
    const bucketName = bucket.bucketName;
    const versioningEnabled = bucket.versioningEnabled;
    const listMethod = versioningEnabled ? 'listObjectVersions' : 'listObjects';
    let moreObjects = true;
    let objectsArr = [];
    let marker = null;
    async.whilst(
        () => moreObjects === true,
        done => {
            const listParams = { Bucket: bucketName };
            if (marker) {
                listParams.Marker = marker;
            }
            s3[listMethod](listParams, (err, data) => {
                if (err) {
                    return done(err);
                }
                objectsArr = objectsArr.concat(data.Contents);
                if (data.IsTruncated) {
                    marker = data.Contents[data.Contents.length - 1].Key;
                } else {
                    moreObjects = false;
                }
                return done();
            });
        },
        err => {
            if (err) {
                log.error('Error listing objects from bucket ' +
                    `${bucketName}: ${err}`);
                return cb(err);
            }
            return async.each(objectsArr,
                (object, done) =>
                    _changeOneObjectAccount(bucket, object, log, done),
                err => {
                    if (err) {
                        return cb(err);
                    }
                    log.debug(`All objects in bucket ${bucketName} ` +
                        'successfully updated');
                    return cb();
                }
            );
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
        if (err === skipError) {
            // move on to next bucket
            return done();
        }
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
        logger.error(`Bucket migration failed: ${err}\n` +
            'Please run script again');
    } else {
        logger.info('Completed bucket migration');
    }
});
