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
        // For backward compatibility reeasons, we accept bigint and numbers
        // But only bigints will be stored in the database
        && (typeof bucketMetric.usedCapacity.current === 'bigint' || typeof bucketMetric.usedCapacity.current === 'number')
        && (typeof bucketMetric.usedCapacity.nonCurrent === 'bigint' || typeof bucketMetric.usedCapacity.nonCurrent === 'number')
        && bucketMetric.usedCapacity.current > -1
        && bucketMetric.usedCapacity.nonCurrent > -1;
}

function isValidCapacityValue(capacity) {
    // For backward compatibility reasons, we accept bigint and numbers
    // But only bigints (Longs) will be stored in the database
    return ((typeof capacity === 'bigint' || (typeof capacity === 'number' && Number.isInteger(capacity))) && capacity >= 0);
}

function collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, log, callback) {
    return async.waterfall([
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
                return async.waterfall([
                    nxt => mongoClient.getUsersBucketCreationDate(bucket.getOwner(), bucketName, log, nxt),
                    (bucketCreationDate, nxt) => mongoClient.readStorageConsumptionMetrics(`bucket_${bucketName}_${new Date(bucketCreationDate).getTime()}`, log, (err, doc) => {
                        if (err && err.message !== 'NoSuchEntity') {
                            log.error('an error occurred during readStorageConsumptionMetrics', {
                                error: { message: err.message },
                            });
                            return nxt(err);
                        }
                        return nxt(null, doc);
                    }),
                    (storageMetricDoc, nxt) => {
                        let bucketStorageUsed = -1n;
                        if (isValidBucketStorageMetrics(storageMetricDoc)) {
                            // Do not count the objects in cold for SOSAPI
                            // numbers are converted into bigints to ensure we support more
                            // than 9PB of bytes stored and previous clusters where the
                            // values were stored as numbers.
                            bucketStorageUsed = BigInt(storageMetricDoc.usedCapacity.current)
                                + BigInt(storageMetricDoc.usedCapacity.nonCurrent);
                        }
                        // read Capacity from bucket._capabilities
                        const { Capacity } = bucket.getCapabilities().VeeamSOSApi.CapacityInfo;

                        let available = -1n;
                        let capacity = -1n;
                        if (isValidCapacityValue(Capacity)) {
                            // is Capacity value is valid
                            capacity = Capacity;
                            // if bucket storage used is valid and capacity is bigger than used
                            if (bucketStorageUsed !== -1n && (capacity - bucketStorageUsed) >= 0n) {
                                available = capacity - bucketStorageUsed;
                            } else if (bucketStorageUsed !== -1n && (capacity - bucketStorageUsed) < 0n) {
                                available = 0n;
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
                            }
                            return nxt(err);
                        });
                    },
                ], cb);
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
