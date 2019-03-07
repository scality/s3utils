const { BucketInfo } = require('arsenal').models;
const async = require('async');
const AWS = require('aws-sdk');
const bucketclient = require('bucketclient');
const fs = require('fs');
const http = require('http');

const werelogs = require('werelogs');
werelogs.configure({ level: 'info', dump: 'error' });
const logger = new werelogs.Logger('s3utils:moveBucketAccounts')
    .newRequestLogger();

const skipError = new Error('skip');

/* expected input JSON structure:
* {
*   bucketd: {
*       bootstrap: <>,
*       log: <>,
*   },
*   https: {
*       key: <>,
*       cert: <>,
*       ca: <>,
*   },
*   buckets: [
        {
            bucketName: <>,
            srcCanonicalId: <>,
            srcOwnerName: <>,
            destCanonicalId: <>,
            destOwnerName: <>
        },
        ...
    ],
* }
*/

// configurable params
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const ENDPOINT = process.env.ENDPOINT;
const CONFIGPATH = process.argv[2];

if (!CONFIGPATH) {
    logger.error('No path to config file provided');
}
let config;
try {
    const data = fs.readFileSync(CONFIGPATH, { encoding: 'utf-8' });
    config = JSON.parse(data);
} catch (err) {
    logger.error(`Could not parse provided config file: ${err.message}`);
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

const { bootstrap, log } = config.bucketd;
if (!bootstrap || bootstrap.length === 0) {
    logger.error('Bucketd bootstrap list is empty');
}
let mdclient;
const mdlog = log || undefined;
try {
    if (config.https) {
        const { key, cert, ca } = config.https;
        mdclient = new bucketclient.RESTClient(
            bootstrap, mdlog, true, key, cert, ca);
    } else {
        mdclient = new bucketclient.RESTClient(bootstrap, mdlog, false);
    }
} catch (err) {
    logger.error(`Could not instantiate bucketclient: ${err.message}`);
}

const buckets = config.buckets;
if (!buckets || buckets.length === 0) {
    logger.error('No buckets included in config. Please provide an array of ' +
        'buckets');
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
        srcCanonicalId,
        srcOwnerName,
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
        } else if (bucketId !== srcCanonicalId
        && bucketName !== srcOwnerName) {
            log.info(`Bucket ${bucketName} id does not match provided id ` +
                'so bucket is not being updated');
            return cb(skipError);
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

function _changeUsersBucket(bucketArr, log, cb) {
    const metaBucket = 'users..bucket';
    const serUids = log.getSerializedUids();

    return mdclient.listObject(metaBucket, serUids, {}, (err, data) => {
        const objectList = JSON.parse(data);
        async.each(objectList.Contents, (object, done) => {
            const objectName = object.key;
            const { canonicalId, bucketName } =
                _parseUsersBucketKey(objectName);

            const matchingBucket = bucketArr.find(bucket =>
                ((bucket.bucketName === bucketName) &&
                (bucket.srcCanonicalId === canonicalId))
            );

            if (!matchingBucket) {
                log.debug(`users..bucket object ${bucketName} does not need ` +
                    'to be updated');
                return done();
            }

            return async.waterfall([
                next => mdclient.getObject(metaBucket, objectName, serUids,
                (err, objectMD) => {
                    if (err) {
                        log.error(`Error getting object metadata: ${err}`);
                        return next(err);
                    }
                    const newObjectName = _createUsersBucketKey(
                        bucketName, matchingBucket.destCanonicalId);
                    return next(null, newObjectName, objectMD);
                }),
                (newObjectName, objectMD, next) => mdclient.putObject(
                metaBucket, newObjectName, objectMD, serUids, err => {
                    if (err) {
                        log.error('Error updating users..bucket object ' +
                            `${objectName}: ${err}`);
                        return next(err);
                    }
                    log.debug(`users..bucket object ${objectName} ` +
                        'successfully updated');
                    return next();
                }),
                next => mdclient.deleteObject(metaBucket, objectName,
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
            ], err => done(err));
        }, err => {
            if (err) {
                return cb(err);
            }
            log.debug('All objects in users..bucket successfully updated');
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

function moveBucketAccounts(bucketArray, cb) {
    async.each(bucketArray, (bucket, done) => {
        const { bucketName } = bucket;
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
                return done(err);
            }
            logger.info(`Bucket ${bucketName} transfer is complete`);
            return done();
        });
    }, err => {
        if (err) {
            return cb(err);
        }
        return _changeUsersBucket(bucketArray, logger, cb);
    });
}

moveBucketAccounts(buckets, err => {
    if (err) {
        logger.error(`Bucket accounts transfers failed: ${err}\n` +
            'Please run script again');
    } else {
        logger.info('Completed all bucket accounts transfers');
    }
});
