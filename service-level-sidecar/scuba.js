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


/**
 * Left-pad a string representation of a value with a given template.
 * For example: pad('foo', '00000') gives '00foo'.
 *
 * @param {any} value - value to pad
 * @param {string} template - padding template
 * @returns {string} - padded string
 */
function padLeft(value, template) {
    return `${template}${value}`.slice(-template.length);
}

function roundToDay(timestamp) {
    return new Date(
        Date.UTC(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), 23, 59, 59, 999),
    );
}

const LENGTH_TS = 14;
const MAX_TS = 10 ** LENGTH_TS - 1; // good until 16 Nov 5138
const TEMPLATE_TS = new Array(LENGTH_TS + 1).join('0');

function formatMetricsKey(resourceName, timestamp) {
    const ts = padLeft(MAX_TS - roundToDay(timestamp).getTime(), TEMPLATE_TS);
    return `${resourceName}/${ts}`;
}

async function getMetrics(classType, resourceName, logId, timestamp, log) {
    const listingParams = {
        maxKeys: 1,
        listingType: 'Basic',
        gte: formatMetricsKey(resourceName, timestamp),
        lte: `${resourceName}/${padLeft(MAX_TS, TEMPLATE_TS)}`,
    };

    const bucket = `${classType}${splitter}${logId}`;

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
