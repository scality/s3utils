const werelogs = require('werelogs');
const { reshapeExceptionError } = require('arsenal').errorUtils;
const S3UtilsMongoClient = require('../utils/S3UtilsMongoClient');

const CountMaster = require('./CountMaster');
const CountManager = require('./CountManager');
const createMongoParams = require('../utils/createMongoParams');
const createWorkers = require('./utils/createWorkers');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';

const loggerConfig = {
    level: logLevel,
    dump: 'error',
};

werelogs.configure(loggerConfig);
const log = new werelogs.Logger('S3Utils::CountItems::Master');

const numWorkers = process.env.NUM_WORKERS && !Number.isNaN(process.env.NUM_WORKERS)
    ? Number.parseInt(process.env.NUM_WORKERS, 10)
    : 4;

const concurrentCursors = (process.env.CONCURRENT_CURSORS
    && !Number.isNaN(process.env.CONCURRENT_CURSORS))
    ? Number.parseInt(process.env.CONCURRENT_CURSORS, 10)
    : 5;

const countMaster = new CountMaster({
    log,
    manager: new CountManager({
        log: new werelogs.Logger('S3Utils::CountItems::Master'),
        workers: createWorkers(numWorkers),
        maxConcurrent: concurrentCursors,
    }),
    client: new S3UtilsMongoClient(createMongoParams(log)),
});

const handleSignal = sig => countMaster.stop(sig, () => process.exit(0));
process.on('SIGINT', handleSignal);
process.on('SIGHUP', handleSignal);
process.on('SIGQUIT', handleSignal);
process.on('SIGTERM', handleSignal);
process.on('uncaughtException', error => {
    log.error('Uncaught Exception', {
        error: reshapeExceptionError(error),
    });
    countMaster.stop(null, () => process.exit(1));
});

countMaster.start(err => {
    if (err) {
        process.exit(1);
    }
    process.exit(0);
});
