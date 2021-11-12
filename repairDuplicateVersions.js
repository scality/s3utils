const async = require('async');
const {
    repairObjects,
    readVerifyLog,
    getSproxydAlias,
    logProgress,
    checkStatus,
} = require('./repairDuplicateVersionsSuite');
const { Logger } = require('werelogs');

const log = new Logger('s3utils:repairDuplicateVersions');

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
