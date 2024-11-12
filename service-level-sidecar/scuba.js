const arsenal = require('arsenal');
const bucketclient = require('bucketclient');

const { BucketClientInterface } = arsenal.storage.metadata.bucketclient;
const { splitter } = arsenal.constants;

const rootLogger = require('./log');
const env = require('./env');
const utils = require('./utils');

const params = {
    bucketdBootstrap: [env.scubaBucketd],
    https: env.scubaBucketdTls ? env.tls.certs : undefined,
};

const metadata = new BucketClientInterface(params, bucketclient, rootLogger);

const listObjects = utils.retryable(metadata.listObject.bind(metadata));

function roundToDay(timestamp) {
    return new Date(
        Date.UTC(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), 23, 59, 59, 999),
    );
}

const LENGTH_TS = 14;
const MAX_TS = parseInt(('9'.repeat(LENGTH_TS)), 10);

function formatMetricsKey(resourceName, timestamp) {
    const ts = (MAX_TS - roundToDay(timestamp).getTime()).toString().padStart(LENGTH_TS, '0');
    return `${resourceName}/${ts}`;
}

async function getMetrics(classType, resourceName, sessionId, timestamp, log) {
    const listingParams = {
        maxKeys: 1,
        listingType: 'Basic',
        gte: formatMetricsKey(resourceName, timestamp),
        lte: `${resourceName}/${MAX_TS.toString()}`,
    };

    const bucket = `${classType}${splitter}${sessionId}`;

    try {
        const resp = await listObjects(bucket, listingParams, log);
        if (resp.length === 0) {
            return null;
        }

        const { key, value } = resp[0];
        return {
            key,
            value: JSON.parse(value),
        };
    } catch (error) {
        if (error.NoSuchBucket) {
            return null;
        }
        log.error('error during metric listing', { error: error.message });
        throw error;
    }
}

module.exports = {
    getMetrics,
};
