/* eslint-disable no-console */
const async = require('async');
const readline = require('readline');
const { Logger } = require('werelogs');
const {
    fetchObjectMetadata,
    putObjectMetadata,
} = require('./repairDuplicateVersionsSuite');

const log = new Logger('s3utils:repairDuplicateVersionIds');

const {
    OBJECT_REPAIR_BUCKETD_HOSTPORT,
} = process.env;

const USAGE = `
repairDuplicateVersionIds.js

This script repairs object versions that have a duplicate versionId
field in the master key and a missing version key, in particular due
to bug S3C-7861 (CRR with incompatible S3C versions between source and
target).

The repair action consists of fetching the source metadata from the
master key, fixing the JSON versionId field to be the first versionId
present in the binary blob (assumed to be the original one, hence
correct), then copying it to both the master key and a new version
key.

Usage:
    node repairDuplicateVersionIds.js

Standard Input:
    The standard input must be fed with the JSON logs output by the
    verifyBucketSproxydKeys.js s3utils script. This script only
    processes the log entries containing the message 'object master
    metadata with duplicate "versionId" field found' and ignores
    other entries.

Mandatory environment variables:
    OBJECT_REPAIR_BUCKETD_HOSTPORT: ip:port of bucketd endpoint
`;

if (!OBJECT_REPAIR_BUCKETD_HOSTPORT) {
    console.error('ERROR: OBJECT_REPAIR_BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}

const objectsToRepair = [];

const status = {
    logLinesRead: 0,
    objectsRepaired: 0,
    objectsErrors: 0,
};

function logProgress(message) {
    log.info(message, { ...status, objectsToRepair: objectsToRepair.length });
}

function readVerifyLog(cb) {
    const logLines = readline.createInterface({ input: process.stdin });
    logProgress('start reading verify log');
    logLines.on('line', line => {
        status.logLinesRead += 1;
        try {
            const parsedLine = JSON.parse(line);
            if (parsedLine.message !== (
                'object master metadata with duplicate "versionId" field found'
            )) {
                return undefined;
            }
            if (!parsedLine.objectUrl || !parsedLine.firstVersionId || !parsedLine.versionedKeyUrl) {
                log.error('malformed verify log line: missing fields', {
                    lineNumber: status.logLinesRead,
                });
                return undefined;
            }
            objectsToRepair.push({
                objectUrl: parsedLine.objectUrl,
                firstVersionId: parsedLine.firstVersionId,
                versionedKeyUrl: parsedLine.versionedKeyUrl,
            });
        } catch (err) {
            log.info('ignoring malformed JSON line');
        }
        return undefined;
    });
    logLines.on('close', () => {
        logProgress('finished reading verify log');
        cb();
    });
}

function repairObject(objInfo, cb) {
    async.waterfall([
        next => fetchObjectMetadata(objInfo.objectUrl, (err, md) => {
            if (err) {
                log.error('error fetching object location', {
                    objectUrl: objInfo.objectUrl,
                    error: { message: err.message },
                });
                return next(err);
            }
            return next(null, md);
        }),
        (md, next) => {
            // eslint-disable-next-line no-param-reassign
            md.versionId = objInfo.firstVersionId;
            putObjectMetadata(objInfo.objectUrl, md, err => {
                if (err) {
                    log.error('error putting object metadata to master key', {
                        objectUrl: objInfo.objectUrl,
                        error: { message: err.message },
                    });
                    return next(err);
                }
                return next(null, md);
            });
        },
        (md, next) => putObjectMetadata(objInfo.versionedKeyUrl, md, err => {
            if (err) {
                log.error('error putting object metadata to versioned key', {
                    objectUrl: objInfo.objectUrl,
                    versionedKeyUrl: objInfo.versionedKeyUrl,
                    error: { message: err.message },
                });
                return next(err);
            }
            return next();
        }),
    ], err => {
        if (err) {
            log.error('an error occurred repairing object', {
                objectUrl: objInfo.objectUrl,
                error: { message: err.message },
            });
            status.objectsErrors += 1;
        } else {
            log.info('repaired object metadata', {
                objectUrl: objInfo.objectUrl,
            });
            status.objectsRepaired += 1;
        }
        return cb();
    });
}

function repairObjects(cb) {
    logProgress('start repairing objects');
    async.eachSeries(objectsToRepair, (objInfo, done) => {
        repairObject(objInfo, err => {
            if (err) {
                log.error('an error occurred repairing object', {
                    objectUrl: objInfo.objectUrl,
                    error: { message: err.message },
                });
                status.objectsErrors += 1;
            }
            done();
        });
    }, cb);
}

function main() {
    async.series([
        readVerifyLog,
        repairObjects,
    ], err => {
        if (err) {
            log.error('an error occurred during repair process', {
                error: { message: err.message },
            });
            process.exit(1);
        }
        logProgress('repair complete');
        if (status.objectsErrors > 0) {
            process.exit(101);
        }
        process.exit(0);
    });
}

main();

function stop() {
    log.info('stopping execution');
    logProgress('last status');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
