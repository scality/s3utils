const { errors } = require('arsenal');
const promClient = require('prom-client');
const { Registry } = require('prom-client');
const CountMaster = require('../CountItems/CountMaster');

const aggregatorRegistry = new promClient.AggregatorRegistry();
const { collectDefaultMetrics } = promClient;

const bucketCount = (promClient.register.getSingleMetric('bucketCount_metrics'))
    || new promClient.Gauge({
        name: 'bucketCount_metrics',
        help: 'Total number of buckets',
        labelNames: ['bucketName'],
    });

// Histogram of the bucket processing duration, by the utilization service.

const bucketProcessingDuration = (promClient.register.getSingleMetric('count_items_bucketProcessingDuration'))
    || new promClient.Histogram({
        name: 'count_items_bucketProcessingDuration',
        help: 'Duration of processing a bucket',
        labelNames: ['service'],
        buckets: [1, 10, 60, 600, 3600, 18000, 36000],
    });

const consolidationDuration = (promClient.register.getSingleMetric('count_items_consolidationDuration'))
    || new promClient.Histogram({
        name: 'count_items_consolidationDuration',
        help: 'Duration of metrics consolidation in seconds',
        labelNames: ['service'],
        buckets: [0.1, 0.5, 1, 2, 5, 10],
    });

const workersCount = (promClient.register.getSingleMetric('count_items_workersCountErrors'))
    || new promClient.Counter({
        name: 'count_items_workersCountErrors',
        help: 'Number of errors in counting workers',
        labelNames: ['state'],
    });

const objectsCount = (promClient.register.getSingleMetric('count_items_objectsCount'))
    || new promClient.Counter({
        name: 'count_items_objectsCount',
        help: 'Number of objects',
        labelNames: ['state'],
    });

const metricsCount = (promClient.register.getSingleMetric('count_items_metricsCount'))
    || new promClient.Counter({
        name: 'count_items_metricsCount',
        help: 'Number of metrics',
        labelNames: ['metricLevel'],
    });
/**
 * @param {http.ServerResponse} res - http response object
 * @param {errors.ArsenalError} error - Error code
 * @return {void}
 */
function _writeResponse(res, error) {
    let statusCode = 200;
    if (error) {
        if (Number.isInteger(error.code)) {
            statusCode = error.code;
        } else {
            statusCode = 500;
        }
    }
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end();
}

/**
 * @param {number} countMasterInstance - countMaster instance
 * @param {function} onScraped - callback to call when metrics are scraped
 * @param {http.IncomingMessage} req - http request object
 * @param {http.ServerResponse} res - http response object
 * @return {void}
 */
async function metricsHandler(countMasterInstance, onScraped, req, res) {
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
        return _writeResponse(res, errors.MethodNotAllowed);
    } finally {
        if (CountMaster.waitingForPromScraping === true) {
            countMasterInstance.stop(null, onScraped);
        }
    }
}

module.exports = {
    client: promClient,
    collectDefaultMetrics,
    metricsHandler,
    bucketCount,
    bucketProcessingDuration,
    consolidationDuration,
    workersCount,
    objectsCount,
    metricsCount,
};
