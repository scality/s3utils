const { errors } = require('arsenal');
const promClient = require('prom-client');
const { Registry } = require('prom-client');
const { http } = require('httpagent');
const CountMaster = require('../CountItems/CountMaster');

const aggregatorRegistry = new promClient.AggregatorRegistry();
const { collectDefaultMetrics } = promClient;


// Histogram of the bucket processing duration, by the utilization service.
const bucketProcessingDuration = new promClient.Histogram({
    name: 's3_countitems_bucket_listing_duration_seconds',
    help: 'Bucket processing duration',
    buckets: [1, 10, 60, 600, 3600, 18000, 36000],
});

const consolidationDuration = new promClient.Histogram({
    name: 's3_countitems_bucket_merge_duration_seconds',
    help: 'Duration of metrics consolidation in seconds',
    buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10],
});

const bucketsCount = new promClient.Counter({
    name: 's3_countitems_total_buckets_count',
    help: 'Total number of buckets processed',
    labelNames: ['status'],
});

const objectsCount = new promClient.Counter({
    name: 's3_countitems_total_objects_count',
    help: 'Total number of objects processed',
    labelNames: ['status'],
});

/**
 * @param {http.ServerResponse} res - http response object
 * @param {Error | errors.ArsenalError} error - Error
 * @return {void}
 */
function _writeResponse(res, error) {
    let statusCode = 500;
    if (error instanceof errors.ArsenalError && Number.isInteger(error.code)) {
        statusCode = error.code;
    }
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end();
}

/**
 * @param {function} onScraped - callback to call when metrics are scraped
 * @param {http.IncomingMessage} req - http request object
 * @param {http.ServerResponse} res - http response object
 * @return {void}
 */
async function metricsHandler(onScraped, req, res) {
    if (req.method !== 'GET' || req.url !== '/metrics') {
        return _writeResponse(res, errors.MethodNotAllowed);
    }
    try {
        const [registerMetrics, clusterMetrics] = await Promise.all([
            promClient.register.metrics(),
            aggregatorRegistry.clusterMetrics(),
        ]);
        const promMetrics = `${registerMetrics}\n${clusterMetrics}`;
        const contentLen = Buffer.byteLength(promMetrics, 'utf8');
        return res.writeHead(200, {
            'content-length': contentLen,
            'content-type': promClient.register.contentType,
        }).end(promMetrics);
    } catch (ex) {
        return _writeResponse(res, ex);
    } finally {
        if (onScraped) {
            onScraped();
        }
    }
}

module.exports = {
    client: promClient,
    collectDefaultMetrics,
    metricsHandler,
    bucketProcessingDuration,
    consolidationDuration,
    bucketsCount,
    objectsCount,
};
