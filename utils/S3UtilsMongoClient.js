const { MongoClientInterface } = require('arsenal').storage.metadata.mongoclient;
const { validStorageMetricLevels } = require('../CountItems/utils/constants');

class S3UtilsMongoClient extends MongoClientInterface {
    getObjectMDStats(bucketName, bucketInfo, isTransient, log, callback) {
        const c = this.getCollection(bucketName);
        const cursor = c.find({}, {
            projection: {
                '_id': 1,
                'value.last-modified': 1,
                'value.replicationInfo': 1,
                'value.dataStoreName': 1,
                'value.content-length': 1,
                'value.versionId': 1,
                'value.owner-display-name': 1,
                'value.isDeleteMarker': 1,
                'value.isNull': 1,
            },
        });
        const collRes = {
            bucket: {}, // bucket level metrics
            location: {}, // location level metrics
            account: {}, // account level metrics
        };
        let stalledCount = 0;
        const cmpDate = new Date();
        cmpDate.setHours(cmpDate.getHours() - 1);

        cursor.forEach(
            res => {
                const { data, error } = this._processEntryData(bucketName, res, isTransient);

                if (error) {
                    log.error('Failed to process entry data', {
                        method: 'getObjectMDStats',
                        entry: res,
                        error,
                    });
                }

                let targetCount;
                let targetData;
                if (res._id.indexOf('\0') !== -1) {
                    // versioned item
                    targetCount = 'versionCount';
                    targetData = 'versionData';

                    if (res.value.replicationInfo.backends.length > 0
                        && this._isReplicationEntryStalled(res, cmpDate)) {
                        stalledCount++;
                    }
                } else if (!!res.value.versionId && !res.value.isNull) {
                    // master version
                    // includes current objects in versioned bucket and
                    // objects uploaded before bucket suspended
                    targetCount = 'masterCount';
                    targetData = 'masterData';
                } else {
                    // null version
                    // include current objects in nonversioned bucket and
                    // objects uploaded after bucket suspended
                    targetCount = 'nullCount';
                    targetData = 'nullData';
                }
                Object.keys(data).forEach(metricLevel => {
                    // metricLevel can only be 'bucket', 'location' or 'account'
                    if (validStorageMetricLevels.has(metricLevel)) {
                        Object.keys(data[metricLevel]).forEach(resourceName => {
                            // resourceName can be the name of bucket, location or account
                            if (!collRes[metricLevel][resourceName]) {
                                collRes[metricLevel][resourceName] = {
                                    masterCount: 0,
                                    masterData: 0,
                                    nullCount: 0,
                                    nullData: 0,
                                    versionCount: 0,
                                    versionData: 0,
                                    deleteMarkerCount: 0,
                                };
                            }
                            collRes[metricLevel][resourceName][targetData] += data[metricLevel][resourceName];
                            collRes[metricLevel][resourceName][targetCount]++;
                            collRes[metricLevel][resourceName].deleteMarkerCount += res.value.isDeleteMarker ? 1 : 0;
                        });
                    }
                });
            },
            err => {
                if (err) {
                    log.error('Error when processing mongo entries', {
                        method: 'getObjectMDStats',
                        error: err,
                    });
                    return callback(err);
                }
                const bucketStatus = bucketInfo.getVersioningConfiguration();
                const isVer = (bucketStatus && (bucketStatus.Status === 'Enabled'
                    || bucketStatus.Status === 'Suspended'));
                const retResult = this._handleResults(collRes, isVer);
                retResult.stalled = stalledCount;
                return callback(null, retResult);
            },
        );
    }

    /**
     * @param{string} bucketName -
     * @param{object} entry -
     * @param{string} entry._id -
     * @param{object} entry.value -
     * @param{boolean} isTransient -
     * @returns{object.<string, number>} results -
     */
    _processEntryData(bucketName, entry, isTransient) {
        if (!bucketName) {
            return {
                data: {},
                error: new Error('no bucket name provided'),
            };
        }

        const size = Number.parseInt(entry.value['content-length'], 10);
        if (Number.isNaN(size)) {
            return {
                data: {},
                error: new Error('invalid content length'),
            };
        }

        const results = {
            // there will be only one bucket for an object entry
            bucket: { [bucketName]: size },
            // there can be multiple locations for an object entry
            location: {},
            // there will be only one account for an object entry
            account: { [entry.value['owner-display-name']]: size },
        };

        if (!isTransient
            || entry.value.replicationInfo.status !== 'COMPLETED') {
            // only count it in current dataStore if object is not in transient or replication not completed
            results.location[entry.value.dataStoreName] = size;
        }
        entry.value.replicationInfo.backends.forEach(rep => {
            // count it in the replication destination location if replication compeleted
            if (rep.status === 'COMPLETED') {
                results.location[rep.site] = size;
            }
        });
        return {
            data: results,
            error: null,
        };
    }

    _handleResults(res, isVersioned) {
        let totalNonCurrentCount = 0;
        let totalCurrentCount = 0;
        const totalBytes = { curr: 0, prev: 0 };
        const locationBytes = {};
        const dataMetrics = {
            bucket: {},
            location: {},
            account: {},
        };

        Object.keys(res).forEach(metricLevel => {
            // metricLevel can only be 'bucket', 'location' or 'account'
            if (validStorageMetricLevels.has(metricLevel)) {
                Object.keys(res[metricLevel]).forEach(resource => {
                    // resource can be the name of bucket, location or account
                    const resourceName = metricLevel === 'location' ? this._getLocName(resource) : resource;
                    if (!dataMetrics[metricLevel][resourceName]) {
                        dataMetrics[metricLevel][resourceName] = {
                            usedCapacity: {
                                current: 0,
                                nonCurrent: 0,
                            },
                            objectCount: {
                                current: 0,
                                nonCurrent: 0,
                                deleteMarker: 0,
                            },
                        };
                    }
                    const {
                        masterCount,
                        masterData,
                        nullCount,
                        nullData,
                        versionCount,
                        versionData,
                        deleteMarkerCount,
                    } = res[metricLevel][resourceName];

                    dataMetrics[metricLevel][resourceName].usedCapacity.current += nullData + masterData;
                    dataMetrics[metricLevel][resourceName].objectCount.current += nullCount + masterCount;

                    if (isVersioned) {
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent
                            += versionData - masterData; // masterData is duplicated in versionedData
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent
                            += versionCount - masterCount - deleteMarkerCount;
                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent = Math.max(dataMetrics[metricLevel][resourceName].objectCount.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += deleteMarkerCount;
                    }

                    if (metricLevel === 'location') { // calculate usedCapacity metrics at global and location level
                        totalBytes.curr += nullData + masterData;
                        if (!locationBytes[resourceName]) {
                            locationBytes[resourceName] = { curr: 0, prev: 0 };
                        }
                        locationBytes[resourceName].curr += nullData + masterData;
                        if (isVersioned) {
                            totalBytes.prev += versionData;
                            totalBytes.prev -= masterData;
                            totalBytes.prev = Math.max(0, totalBytes.prev);
                            locationBytes[resourceName].prev += versionData;
                            locationBytes[resourceName].prev -= masterData;
                            locationBytes[resourceName].prev = Math.max(0, locationBytes[resourceName].prev);
                        }
                    }
                    if (metricLevel === 'bucket') { // count objects up of all buckets
                        totalCurrentCount += (masterCount + nullCount);
                        totalNonCurrentCount += isVersioned ? (versionCount - masterCount - deleteMarkerCount) : 0;
                    }
                });
            }
        });
        return {
            versions: Math.max(0, totalNonCurrentCount),
            objects: totalCurrentCount,
            dataManaged: {
                total: totalBytes,
                locations: locationBytes,
            },
            dataMetrics,
        };
    }
}

module.exports = S3UtilsMongoClient;
