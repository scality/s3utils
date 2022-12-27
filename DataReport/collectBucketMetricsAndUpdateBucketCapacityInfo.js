const async = require('async');
const { MaxParallelLimit } = require('../utils/constants');

function isSOSCapacityInfoEnabled(bucketInfo) {
    /**
     * a valid SOS CapacityInfo Enabled bucketMD looks like
     * {
     *     _capabilities: {
     *         VeeamSOSApi: {
     *             SystemInfo: {
     *                 ...
     *                 ProtocolCapabilities: {
     *                     CapacityInfo: true,
     *                     ...
     *                 },
     *             ...
     *             },
     *             CapacityInfo: {
     *                 ...
     *             },
     *         }
     *     },
     * }
     */
    return !!(bucketInfo.getCapabilities() // should exist
        && bucketInfo.getCapabilities().VeeamSOSApi // should exist
        && bucketInfo.getCapabilities().VeeamSOSApi.SystemInfo // should exist
        && bucketInfo.getCapabilities().VeeamSOSApi.SystemInfo.ProtocolCapabilities // should exist
        && bucketInfo.getCapabilities().VeeamSOSApi.SystemInfo.ProtocolCapabilities.CapacityInfo === true // should be true
        && bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo); // should exist
}

function isValidBucketStorageMetrics(bucketMetric) {
    return bucketMetric
    && bucketMetric.usedCapacity
        && typeof bucketMetric.usedCapacity.current === 'number'
        && typeof bucketMetric.usedCapacity.nonCurrent === 'number'
    && bucketMetric.usedCapacity.current > -1
    && bucketMetric.usedCapacity.nonCurrent > -1;
}

function isValidCapacityValue(capacity) {
    return (Number.isSafeInteger(capacity) && capacity >= 0);
}

function collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, log, callback) {
    let bucketMetrics;
    return async.waterfall([
        next => mongoClient.readCountItems(log, (err, doc) => {
            if (err) {
                log.error('an error occurred during readCountItems', {
                    error: { message: err.message },
                });
                return next(err);
            }
            if (doc && doc.dataMetrics && doc.dataMetrics.bucket) {
                bucketMetrics = doc.dataMetrics.bucket;
            } else {
                log.info('no buckets metrics in countItems, exit');
                return callback(null);
            }
            return next();
        }),
        next => mongoClient.getBucketInfos(log, (err, bucketInfos) => {
            if (err) {
                log.error('error getting bucket list', {
                    error: err,
                });
                return next(err);
            }
            return next(null, bucketInfos.bucketInfos);
        }),
        (bucketInfoList, next) => async.eachLimit(
            bucketInfoList,
            MaxParallelLimit,
            (bucket, cb) => {
                if (!isSOSCapacityInfoEnabled(bucket)) {
                    return cb();
                }
                const bucketName = bucket.getName();

                // get bucket storage used
                let bucketStorageUsed = -1;
                if (isValidBucketStorageMetrics(bucketMetrics[bucketName])) {
                    bucketStorageUsed = bucketMetrics[bucketName].usedCapacity.current
                            + bucketMetrics[bucketName].usedCapacity.nonCurrent;
                }

                // read Capacity from bucket._capabilities
                const { Capacity } = bucket.getCapabilities().VeeamSOSApi.CapacityInfo;

                let available = -1;
                let capacity = -1;
                if (isValidCapacityValue(Capacity)) { // is Capacity value is valid
                    capacity = Capacity;
                    // if bucket storage used is valid and capacity is bigger than used
                    if (bucketStorageUsed !== -1 && (capacity - bucketStorageUsed) >= 0) {
                        available = capacity - bucketStorageUsed;
                    }
                }

                return mongoClient.updateBucketCapacityInfo(bucketName, {
                    Capacity: capacity,
                    Available: available,
                    Used: bucketStorageUsed,
                }, log, err => {
                    if (err) {
                        log.error('an error occurred during put bucket CapacityInfo attributes', {
                            error: { message: err.message },
                            bucketName,
                        });
                        return cb(err);
                    }
                    return cb();
                });
            },
            err => {
                if (err) {
                    return next(err);
                }
                log.info('successfully updated CapacityInfo for all qualified buckets');
                return next(null);
            },
        ),
    ], callback);
}

module.exports = {
    isSOSCapacityInfoEnabled,
    isValidBucketStorageMetrics,
    isValidCapacityValue,
    collectBucketMetricsAndUpdateBucketCapacityInfo,
};
