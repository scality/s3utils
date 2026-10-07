const werelogs = require('werelogs');
const ReplicationSiteRemover = require('./CRR/ReplicationSiteRemover');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';
werelogs.configure({ level: logLevel, dump: 'error' });
const log = new werelogs.Logger('s3utils::removeReplicationSite');

const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const {
    ACCESS_KEY,
    SECRET_KEY,
    ENDPOINT,
    SITE_TO_REMOVE,
    TARGET_PREFIX,
    KEY_MARKER,
    VERSION_ID_MARKER,
} = process.env;
// dry run unless explicitly disabled
const DRY_RUN = process.env.DRY_RUN !== 'false' && process.env.DRY_RUN !== '0';
const WORKERS = (process.env.WORKERS
    && Number.parseInt(process.env.WORKERS, 10)) || 10;
const LISTING_LIMIT = (process.env.LISTING_LIMIT
    && Number.parseInt(process.env.LISTING_LIMIT, 10)) || 1000;
const MAX_UPDATES = (process.env.MAX_UPDATES
    && Number.parseInt(process.env.MAX_UPDATES, 10)) || undefined;

if (!BUCKETS || BUCKETS.length === 0) {
    log.fatal('No buckets given as input! Please provide '
        + 'a comma-separated list of buckets');
    process.exit(1);
}
['ENDPOINT', 'ACCESS_KEY', 'SECRET_KEY', 'SITE_TO_REMOVE'].forEach(name => {
    if (!process.env[name]) {
        log.fatal(`${name} not defined`);
        process.exit(1);
    }
});

const remover = new ReplicationSiteRemover({
    buckets: BUCKETS,
    siteToRemove: SITE_TO_REMOVE,
    accessKey: ACCESS_KEY,
    secretKey: SECRET_KEY,
    endpoint: ENDPOINT,
    dryRun: DRY_RUN,
    workers: WORKERS,
    listingLimit: LISTING_LIMIT,
    targetPrefix: TARGET_PREFIX,
    maxUpdates: MAX_UPDATES,
    keyMarker: KEY_MARKER,
    versionIdMarker: VERSION_ID_MARKER,
}, log);

function stop() {
    log.warn('stopping execution');
    remover._logProgress();
    process.exit(1);
}
process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);

remover.run()
    .then(stats => {
        if (stats.errors > 0) {
            process.exitCode = 1;
        }
    })
    .catch(err => {
        log.error('error during task execution', { error: err.message });
        process.exitCode = 1;
    });
