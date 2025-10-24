const async = require('async');
const util = require('util');

const arsenal = require('arsenal');

const { splitter } = arsenal.constants;

const bucketd = require('./bucketd');
const scuba = require('./scuba');
const env = require('./env');
const warp10 = require('./warp10');
const { getAccountIdForCanonicalId } = require('./vault');

/**
 * MetricReport
 */
class MetricReport {
    constructor(factor = 1) {
        this.factor = factor;
        this.count = 0;
        this.bytes = 0;
    }

    /**
     * Add the given data to the metric report
     * @param {integer} count - Amount to increment object count
     * @param {integer} bytes - Amount to increment bytes stored
     * @returns {undefined} -
     */
    update(count, bytes) {
        this.count += count;
        this.bytes += bytes;
    }

    /**
     * Returns metrics report data as an object
     */
    get data() {
        return {
            /* eslint-disable camelcase */
            obj_count: this.count,
            bytes_stored: this.bytes,
            bytes_stored_total: Math.ceil(this.factor * this.bytes),
            /* eslint-enable camelcase */
        };
    }
}


/**
 * @param {Array<Number>} sessionIds - raft session ids to retrieve metrics for (only contains info if scuba backend is enabled)
 * @param {integer} timestamp - timestamp to retrieve metrics in microseconds
 * @param {object} bucket - bucket  to retrieve metrics for
 * @param {string} bucket.name - bucket name
 * @param {string} bucket.account - bucket owner account canonical id
 * @param {object} log - werelogs logger instance
 * @returns {object} - object count and bytes stored for bucket
 */
async function getMetricsForBucket(sessionIds, timestamp, bucket, log) {
    log.debug('getting metrics for bucket', { bucket: bucket.name, timestamp });

    if (env.enableScuba) {
        const resourceName = `${bucket.account}${splitter}${bucket.name}`;
        const logResults = await util.promisify(async.mapLimit)(sessionIds, env.concurrencyLimit, async logId => {
            try {
                return await scuba.getMetrics('bucket', resourceName, logId, new Date(timestamp), log);
            } catch (err) {
                log.error('error getting metrics for bucket', { bucket: bucket.name, logId, error: err.message });
                throw err;
            }
        });

        return logResults
            .filter(result => result !== null)
            .reduce((acc, result) => ({
                count: acc.count + parseInt(result.value.metrics.objectsTotal, 10),
                bytes: acc.bytes + parseInt(result.value.metrics.bytesTotal, 10),
            }), { count: 0, bytes: 0 });
    }
    return warp10.getMetricsForBucket(timestamp, bucket.name, log);
}

/**
 * Generate a service level metrics report for the given timestamp.
 * The report is in the following format:
 *  {
 *    service: { obj_count, bytes_stored },
 *    account: [ { name, arn, obj_count, bytes_stored } ],
 *    bucket: { <account_arn>: [ { name, obj_count, bytes_stored } ] }
 *  }
 *
 * @param {number} timestamp - timestamp to generate the report for
 * @param {object} log - werelogs logger instance
 * @returns {ServiceMetricsReport} -
 */
async function getServiceReport(timestamp, log) {
    const serviceReport = new MetricReport(env.scaleFactor);
    const accountReports = {};
    const bucketReports = {};
    const accountInfoCache = {};

    let sessionIds = [];
    if (env.enableScuba) {
        sessionIds = await bucketd.getRaftSessionIds(log);
        sessionIds = sessionIds.filter(id => id !== '0');
    }

    for await (const buckets of bucketd.listBuckets(log)) {
        log.debug('got response from bucketd', { numBuckets: buckets.length });
        await util.promisify(async.eachLimit)(buckets, env.concurrencyLimit, async bucket => {
            const metrics = await getMetricsForBucket(sessionIds, timestamp, bucket, log);

            log.debug('fetched metrics for bucket', { bucket: bucket.name, accCanonicalId: bucket.account });

            if (accountReports[bucket.account] === undefined) {
                accountReports[bucket.account] = new MetricReport(env.scaleFactor);
            }

            if (bucketReports[bucket.account] === undefined) {
                bucketReports[bucket.account] = {};
            }

            if (bucketReports[bucket.account][bucket.name] === undefined) {
                bucketReports[bucket.account][bucket.name] = new MetricReport(env.scaleFactor);
            }

            serviceReport.update(metrics.count, metrics.bytes);
            accountReports[bucket.account].update(metrics.count, metrics.bytes);
            bucketReports[bucket.account][bucket.name].update(metrics.count, metrics.bytes);

            // pull account info into cache as we process the buckets
            // this is so we can spread the requests to vault out
            // and avoid needing to batch at the end
            if (accountInfoCache[bucket.account] === undefined) {
                log.debug('fetching info for account', { canonicalId: bucket.account });
                accountInfoCache[bucket.account] = await getAccountIdForCanonicalId(bucket.account, log);
            }
        });
        log.debug('finished processing batch', { numBuckets: buckets.length });
    }

    log.debug('finished retrieving metrics for report');

    log.debug('generating account reports');
    const accountReport = Object.entries(accountReports).map(([canonId, report]) => ({
        arn: accountInfoCache[canonId].arn,
        name: accountInfoCache[canonId].name,
        ...report.data,
    }));

    log.debug('generating bucket reports');
    const bucketReport = Object.fromEntries(
        Object.entries(bucketReports).map(([canonId, buckets]) => [
            accountInfoCache[canonId].arn,
            Object.entries(buckets).map(([name, report]) => ({ name, ...report.data })),
        ]),
    );

    log.debug('finished generating report');

    return {
        account: accountReport,
        bucket: bucketReport,
        service: serviceReport.data,
    };
}

module.exports = {
    MetricReport,
    getServiceReport,
};
