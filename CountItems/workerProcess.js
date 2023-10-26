const werelogs = require('werelogs');
const { reshapeExceptionError } = require('arsenal').errorUtils;
const S3UtilsMongoClient = require('../utils/S3UtilsMongoClient');

const CountWorker = require('./CountWorker');
const createMongoParams = require('../utils/createMongoParams');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';

const loggerConfig = {
    level: logLevel,
    dump: 'error',
};

werelogs.configure(loggerConfig);
const log = new werelogs.Logger('S3Utils::CountItems::Worker');

const worker = new CountWorker({
    log,
    sendFn: process.send.bind(process),
    client: new S3UtilsMongoClient(createMongoParams(log)),
});

process.on('message', data => worker.handleMessage(data));
process.on('uncaughtException', error => {
    log.error('Uncaught Exception', {
        error: reshapeExceptionError(error),
    });
    worker.clientTeardown(() => process.exit(1));
});

