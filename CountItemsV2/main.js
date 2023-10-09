const werelogs = require('werelogs');
const CountItems = require('./CountItems');
const { reshapeExceptionError } = require('arsenal').errorUtils;

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';
const loggerConfig = {
    level: logLevel,
    dump: 'error',
};
werelogs.configure(loggerConfig);
const log = new werelogs.Logger('s3utils::countItemsV2');

const MAX_CONCURRENT_OPERATIONS = Number.parseInt(process.env.MAX_CONCURRENT_OPERATIONS, 10) || 10;
const MAX_CONNECT_RETRIES = Number.parseInt(process.env.MAX_CONNECT_RETRIES, 10) || 5;

const config = {
    maxRetries: MAX_CONNECT_RETRIES,
    maxConcurrentOperations: MAX_CONCURRENT_OPERATIONS,
    mongoDBSupportsPreImages: process.env.MONGODB_SUPPORTS_PREIMAGES === 'true',
}
const worker = new CountItems(config, log);
worker.work();

const handleSignal = () => process.exit(0);
process.on('SIGINT', handleSignal);
process.on('SIGHUP', handleSignal);
process.on('SIGQUIT', handleSignal);
process.on('SIGTERM', handleSignal);
process.on('uncaughtException', error => {
    log.error('Uncaught Exception', {
        error: reshapeExceptionError(error),
    });
    delete worker;
});