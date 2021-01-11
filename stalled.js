const assert = require('assert');
const { Logger } = require('werelogs');
const ZenkoClient = require('zenkoclient');

const {
    MongoClientInterfaceStalled,
} = require('./StalledRetry/MongoClientInterfaceStalled');

const {
    RateLimitingCursor,
} = require('./StalledRetry/CursorWrapper');

const {
    StalledRequestHandler,
} = require('./StalledRetry/StalledRequestHandler');

const { parseEnvInt } = require('./utils');

const ENDPOINT = process.env.ENDPOINT;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const MONGODB_REPLICASET = process.env.MONGODB_REPLICASET;
const MONGODB_DATABASE = process.env.MONGODB_DATABASE || 'metadata';
const DRY_RUN = process.env.DRY_RUN && process.env.DRY_RUN !== '0';

const BATCH_SIZE = parseEnvInt(process.env.REQUEST_BATCH_SIZE, 10);
const QUEUE_LIMIT = parseEnvInt(process.env.QUEUE_LIMIT, 1000);
const CONCURRENT_REQUESTS = parseEnvInt(process.env.CONCURRENT_REQUESTS, 5);
const EXPIRED_BY_HOUR =
    Math.max(parseEnvInt(process.env.EXPIRED_BY_HOUR, 1), 1);

assert(BATCH_SIZE <= QUEUE_LIMIT);

if (!ENDPOINT) {
    throw new Error('ENDPOINT not defined!');
}
if (!ACCESS_KEY) {
    throw new Error('ACCESS_KEY not defined');
}
if (!SECRET_KEY) {
    throw new Error('SECRET_KEY not defined');
}
if (!MONGODB_REPLICASET) {
    throw new Error('MONGODB_REPLICASET not defined');
}

const HEAP_PROFILER_INTERVAL_MS =
    parseEnvInt(process.env.HEAP_PROFILER_INTERVAL_MS, 10 * 60 * 1000);
const HEAP_PROFILER_PATH = process.env.HEAP_PROFILER_PATH;
require('./utils/heapProfiler')(HEAP_PROFILER_PATH, HEAP_PROFILER_INTERVAL_MS);

const log = new Logger('S3Utils::Stalled');

function wrapperFactory(bucketName, cmpDate, cursor, log) {
    return new RateLimitingCursor(
        cursor,
        {
            log,
            queueLimit: QUEUE_LIMIT,
            cmpDate,
            bucketName,
        }
    );
}

function handlerFactory(log) {
    const zenkoClient = new ZenkoClient({
        apiVersion: '2018-07-08-json',
        accessKeyId: ACCESS_KEY,
        secretAccessKey: SECRET_KEY,
        endpoint: ENDPOINT,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        maxRetries: 0,
        sslEnabled: false,
        httpOptions: { timeout: 0 },
    });

    return new StalledRequestHandler(
        zenkoClient,
        {
            dryRun: DRY_RUN,
            batchSize: BATCH_SIZE,
            concurrentRequests: CONCURRENT_REQUESTS,
            log,
        }
    );
}

const config = {
    replicaSetHosts: MONGODB_REPLICASET,
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: MONGODB_DATABASE,
    replicationGroupId: 'RG001',
    logger: log,
    cursorWrapperFactory: wrapperFactory,
    requestHandlerFactory: handlerFactory,
};

const mongoclient = new MongoClientInterfaceStalled(config);
mongoclient.setup(err => {
    if (err) {
        log.error('error connecting to mongodb', err);
        return process.exit(1);
    }
    return mongoclient.queueStalledObjects(EXPIRED_BY_HOUR, (err, res) => {
        if (err) {
            log.error('error occurred', err);
            return process.exit(1);
        }
        if (res > 0) {
            log.info('stalled objects are queued for retries');
        }
        log.info('stalled objects retry job completed');
        return process.exit(0);
    });
});
