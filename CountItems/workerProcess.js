const werelogs = require('werelogs');
const { reshapeExceptionError } = require('arsenal').errorUtils;
const { MongoClientInterface } = require('arsenal').storage.metadata.mongoclient;

const CountWorker = require('./CountWorker');
const { createMongoParams } = require('./utils');

const loggerConfig = {
    level: 'info',
    dump: 'error',
};

werelogs.configure(loggerConfig);
const log = new werelogs.Logger('S3Utils::CountItems::Worker');

const worker = new CountWorker({
    log,
    sendFn: process.send.bind(process),
    client: new MongoClientInterface(createMongoParams(log)),
});

process.on('message', data => worker.handleMessage(data));
process.on('uncaughtException', error => {
    log.error('Uncaught Exception', {
        error: reshapeExceptionError(error),
    });
    worker.clientTeardown(() => process.exit(1));
});

