const werelogs = require('werelogs');
const ReplicationStatusUpdater = require('./CRR/ReplicationStatusUpdater');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';
const loggerConfig = {
    level: logLevel,
    dump: 'error',
};
werelogs.configure(loggerConfig);
const log = new werelogs.Logger('s3utils::crrExistingObjects');

const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const { SITE_NAME } = process.env;
let { STORAGE_TYPE } = process.env;
let { TARGET_REPLICATION_STATUS } = process.env;
const { TARGET_PREFIX } = process.env;
const WORKERS = (process.env.WORKERS
    && Number.parseInt(process.env.WORKERS, 10)) || 10;
const MAX_UPDATES = (process.env.MAX_UPDATES
    && Number.parseInt(process.env.MAX_UPDATES, 10));
const MAX_SCANNED = (process.env.MAX_SCANNED
    && Number.parseInt(process.env.MAX_SCANNED, 10));
const { KEY_MARKER } = process.env;
const { VERSION_ID_MARKER } = process.env;

const {
    ACCESS_KEY,
    SECRET_KEY,
    ENDPOINT,
} = process.env;

const LISTING_LIMIT = (process.env.LISTING_LIMIT
    && Number.parseInt(process.env.LISTING_LIMIT, 10)) || 1000;

if (!BUCKETS || BUCKETS.length === 0) {
    log.fatal('No buckets given as input! Please provide '
      + 'a comma-separated list of buckets');
    process.exit(1);
}
if (!ENDPOINT) {
    log.fatal('ENDPOINT not defined!');
    process.exit(1);
}
if (!ACCESS_KEY) {
    log.fatal('ACCESS_KEY not defined');
    process.exit(1);
}
if (!SECRET_KEY) {
    log.fatal('SECRET_KEY not defined');
    process.exit(1);
}
if (!STORAGE_TYPE) {
    STORAGE_TYPE = '';
}
if (!TARGET_REPLICATION_STATUS) {
    TARGET_REPLICATION_STATUS = 'NEW';
}

const replicationStatusToProcess = TARGET_REPLICATION_STATUS.split(',');
replicationStatusToProcess.forEach(state => {
    if (!['NEW', 'PENDING', 'COMPLETED', 'FAILED', 'REPLICA'].includes(state)) {
        log.fatal('invalid TARGET_REPLICATION_STATUS environment: must be a '
            + 'comma-separated list of replication statuses to requeue, '
            + 'as NEW,PENDING,COMPLETED,FAILED,REPLICA.');
        process.exit(1);
    }
});
log.info('Objects with replication status '
    + `${replicationStatusToProcess.join(' or ')} `
    + 'will be reset to PENDING to trigger CRR');

const replicationStatusUpdater = new ReplicationStatusUpdater({
    buckets: BUCKETS,
    replicationStatusToProcess,
    workers: WORKERS,
    accessKey: ACCESS_KEY,
    secretKey: SECRET_KEY,
    endpoint: ENDPOINT,
    siteName: SITE_NAME,
    storageType: STORAGE_TYPE,
    targetPrefix: TARGET_PREFIX,
    listingLimit: LISTING_LIMIT,
    maxUpdates: MAX_UPDATES,
    maxScanned: MAX_SCANNED,
    keyMarker: KEY_MARKER,
    versionIdMarker: VERSION_ID_MARKER,
}, log);

replicationStatusUpdater.run(err => {
    if (err) {
        return log.error('error during task execution', { error: err });
    }
    return log.info('completed task for all buckets');
});

const stopCrr = replicationStatusUpdater.stop;
process.on('SIGINT', stopCrr);
process.on('SIGHUP', stopCrr);
process.on('SIGQUIT', stopCrr);
process.on('SIGTERM', stopCrr);
