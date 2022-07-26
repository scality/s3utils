/* eslint-disable no-console */
const async = require('async');
const { Logger } = require('werelogs');
const {
    repairObjects,
    readVerifyLog,
    getSproxydAlias,
    logProgress,
    checkStatus,
} = require('./repairDuplicateVersionsSuite');

const log = new Logger('s3utils:repairDuplicateVersions');

const {
    OBJECT_REPAIR_BUCKETD_HOSTPORT, OBJECT_REPAIR_SPROXYD_HOSTPORT,
} = process.env;

const USAGE = `
repairDuplicateVersions.js

This script repairs object versions that share sproxyd keys with
another version, particularly due to bug S3C-2731.

The repair action consists of copying the data location that is
duplicated to a new sproxyd key (or set of keys for MPU), and updating
the metadata to reflect the new location, resulting in two valid
versions with distinct data, though identical in content.

The script does not remove any version even if the duplicate was due
to an internal retry in the metadata layer, because either version
might be referenced by S3 clients in some cases.

Usage:
    node repairDuplicateVersions.js

Standard Input:
    The standard input must be fed with the JSON logs output by the
    verifyBucketSproxydKeys.js s3utils script. This script only
    processes the log entries containing the message "duplicate
    sproxyd key found" and ignores other entries.

Mandatory environment variables:
    OBJECT_REPAIR_BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    OBJECT_REPAIR_SPROXYD_HOSTPORT: ip:port of sproxyd endpoint
`;

if (!OBJECT_REPAIR_BUCKETD_HOSTPORT) {
    console.error('ERROR: OBJECT_REPAIR_BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!OBJECT_REPAIR_SPROXYD_HOSTPORT) {
    console.error('ERROR: OBJECT_REPAIR_SPROXYD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}

function main() {
    async.series([
        getSproxydAlias,
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
        if (checkStatus('objectsErrors')) {
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
