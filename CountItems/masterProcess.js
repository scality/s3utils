const werelogs = require('werelogs');
const { network } = require('arsenal');
const { reshapeExceptionError } = require('arsenal').errorUtils;
const S3UtilsMongoClient = require('../utils/S3UtilsMongoClient');

const CountMaster = require('./CountMaster');
const CountManager = require('./CountManager');
const createMongoParams = require('../utils/createMongoParams');
const createWorkers = require('./utils/createWorkers');

const WebServer = network.http.server;
const monitoring = require('../utils/monitoring');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';

const loggerConfig = {
    level: logLevel,
    dump: 'error',
};

let waitingForPromScraping = false;

werelogs.configure(loggerConfig);
const log = new werelogs.Logger('S3Utils::CountItems::Master');

function tryParseInt(s, defaultValue) {
    const v = Number.parseInt(s, 10);
    return v > 0 ? v : defaultValue;
}

const prometheusPollingPeriod = tryParseInt(process.env.PROMETHEUS_POLLING_PERIOD, 30);

const prometheusPollingAttempts = tryParseInt(process.env.PROMETHEUS_POLLING_ATTEMPTS, 5);

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

const metricServer = new WebServer(8003, log).onRequest((req, res) => monitoring.metricsHandler(
    () => {
        if (waitingForPromScraping === true) {
            countMaster.stop(null, () => process.exit(0));
        }
    },
    req,
    res,
));
metricServer.start();

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
    waitingForPromScraping = true;
    setTimeout(() => {
        countMaster.stop(null, () => process.exit(0));
    }, prometheusPollingAttempts * prometheusPollingPeriod * 1000);
});
