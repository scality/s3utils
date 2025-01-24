/* eslint-disable consistent-return */
const { MongoClientInterface } = require('arsenal').storage.metadata.mongoclient;
const { Long } = require('mongodb');
const { errors, constants } = require('arsenal');
const async = require('async');
const { validStorageMetricLevels } = require('../CountItems/utils/constants');
const getLocationConfig = require('./locationConfig');
const monitoring = require('./monitoring');

const METASTORE = '__metastore';
const INFOSTORE = '__infostore';
const USERSBUCKET = '__usersbucket';
const INFOSTORE_TMP = `${INFOSTORE}_tmp`;
const __COUNT_ITEMS = 'countitems';

const baseMetricsObject = {
    masterCount: 0,
    masterData: 0,
    nullCount: 0,
    nullData: 0,
    versionCount: 0,
    versionData: 0,
    deleteMarkerCount: 0,
    masterCountCold: 0,
    masterDataCold: 0,
    nullCountCold: 0,
    nullDataCold: 0,
    versionCountCold: 0,
    versionDataCold: 0,
    deleteMarkerCountCold: 0,
    masterCountRestoring: 0,
    masterDataRestoring: 0,
    nullCountRestoring: 0,
    nullDataRestoring: 0,
    versionCountRestoring: 0,
    versionDataRestoring: 0,
    deleteMarkerCountRestoring: 0,
    masterCountRestored: 0,
    masterDataRestored: 0,
    nullCountRestored: 0,
    nullDataRestored: 0,
    versionCountRestored: 0,
    versionDataRestored: 0,
    deleteMarkerCountRestored: 0,
    mpuUploadCounts: 0,
    mpuPartsData: 0,
};

class S3UtilsMongoClient extends MongoClientInterface {
    /**
     * Get the list of buckets and their location dates
     * @param {object} log - Werelogs logger
     * @returns {object} - Object with bucket names as keys
     * and their creation dates as values
     */
    async _getUsersBucketCreationDates(log) {
        let cursorUsersBucketCreationDates;
        try {
            cursorUsersBucketCreationDates = await this.getCollection(USERSBUCKET).find({}, {
                projection: {
                    'value.creationDate': 1,
                },
            });
            const usersBucketCreationDatesArray = await cursorUsersBucketCreationDates.toArray();
            return usersBucketCreationDatesArray
                .reduce((map, obj) => ({ ...map, [obj._id]: obj.value.creationDate }), {});
        } catch (err) {
            log.error('Failed to read __usersbucket collection', {
                method: 'getUsersBucketCreationDates',
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return null;
        } finally {
            await cursorUsersBucketCreationDates.close();
        }
    }

    async updateInflightDeltas(allMetrics, log) {
        let cursor;
        try {
            if (!allMetrics || !Array.isArray(allMetrics) || allMetrics.length === 0) {
                return allMetrics;
            }

            cursor = await this.getCollection(INFOSTORE).find({}, {
                projection: {
                    'usedCapacity._inflight': 1,
                },
            });

            const inflights = await cursor.toArray();
            // convert inflights to a map with _id: usedCapacity._inflight
            const inflightsMap = inflights.reduce((map, obj) => {
                const inflightLong = obj.usedCapacity && obj.usedCapacity._inflight ? obj.usedCapacity._inflight : 0;
                return {
                    ...map,
                    [obj._id]: inflightLong,
                };
            }, {});

            const accountInflights = {};
            allMetrics.forEach(entry => {
                const id = entry._id;
                if (id.startsWith('bucket_')) {
                    const inflightDocument = inflightsMap[id];
                    const inflight = Long.fromNumber(Number(inflightDocument ? Math.max(0, inflightDocument - entry.usedCapacity._inflightsPreScan) : 0));
                    if (inflight) {
                        const inflightLong = Long.fromNumber(Number(inflight));
                        // Inflights remaining after the scan are part of the "current" bytes,
                        // and stored in _inflightsDelta
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity.current = Long.fromNumber(Number(entry.usedCapacity.current)).add(inflightLong);
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity._inflightsDelta = inflightLong;
                        const accountOwnerId = `account_${entry.accountOwnerID}`;
                        if (accountInflights[accountOwnerId]) {
                            accountInflights[accountOwnerId] = Long.fromNumber(Number(accountInflights[accountOwnerId])).add(inflightLong);
                        } else {
                            accountInflights[accountOwnerId] = inflightLong;
                        }
                        // eslint-disable-next-line no-param-reassign
                        delete entry.usedCapacity._inflightsPreScan;
                        // eslint-disable-next-line no-param-reassign
                        delete entry.accountOwnerID;
                    }
                }
            });

            allMetrics.forEach(entry => {
                const id = entry._id;
                if (id.startsWith('account_')) {
                    if (accountInflights[id]) {
                        // Inflights remaining after the scan are part of the "current" bytes,
                        // and stored in _inflightsDelta
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity.current = Long.fromNumber(Number(entry.usedCapacity.current)).add(accountInflights[id]);
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity._inflightsDelta = accountInflights[id];
                    }
                }
            });

            return allMetrics;
        } catch (err) {
            log.error('An error occurred', {
                method: 'updateInflightDeltas',
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return allMetrics;
        } finally {
            if (cursor && !cursor.closed) {
                log.info('Finished processing cursor', {
                    method: 'updateInflightDeltas',
                });
                cursor.close();
            }
        }
    }

    async getObjectMDStats(bucketName, bucketInfo, isTransient, log, callback) {
        let cursor;
        let cursorMpuBucket;
        try {
            const c = this.getCollection(bucketName);
            cursor = c.find({}, {
                projection: {
                    '_id': 1,
                    'value.last-modified': 1,
                    'value.replicationInfo': 1,
                    'value.dataStoreName': 1,
                    'value.content-length': 1,
                    'value.versionId': 1,
                    'value.owner-id': 1,
                    'value.isDeleteMarker': 1,
                    'value.isNull': 1,
                    'value.archive': 1,
                    'value.x-amz-storage-class': 1,
                    'value.isPHD': 1,
                },
            });
            const collRes = {
                bucket: {}, // bucket level metrics
                location: {}, // location level metrics
                account: {}, // account level metrics
            };
            let stalledCount = 0;
            let bucketKey;
            let inflightsPreScan = 0;
            let accountBucket;
            const cmpDate = new Date();
            cmpDate.setHours(cmpDate.getHours() - 1);

            const locationConfig = getLocationConfig(log);

            const usersBucketCreationDatesMap = await this._getUsersBucketCreationDates(log);

            const bucketStatus = bucketInfo.getVersioningConfiguration();
            const isVer = (bucketStatus && (bucketStatus.Status === 'Enabled'
                || bucketStatus.Status === 'Suspended'));

            if (!usersBucketCreationDatesMap) {
                return callback(errors.InternalError);
            }

            const bucketDate = usersBucketCreationDatesMap[`${bucketInfo.getOwner()}${constants.splitter}${bucketName}`];
            if (bucketDate) {
                bucketKey = `bucket_${bucketName}_${new Date(bucketDate).getTime()}`;
                if (bucketKey) {
                    inflightsPreScan = await this.readStorageConsumptionInflights(bucketKey, log);
                }
            }

            let startCursorDate = new Date();
            let processed = 0;

            const processCursorEntry = (entry, isMPUPart = false, isOverviewKey = false) => {
                // Periodically display information about the cursor
                // if more than 30s elapsed
                const currentDate = Date.now();
                if (currentDate - startCursorDate > 30000) {
                    startCursorDate = currentDate;
                    log.info('Processing cursor', {
                        method: 'getObjectMDStats',
                        bucketName,
                        processed,
                    });
                }

                const isObjectCold = this._isObjectCold(entry);
                const isObjectRestoring = this._isObjectRestoring(entry);
                const isObjectRestored = this._isObjectRestored(entry);

                const { data, error } = this._processEntryData(
                    bucketName,
                    bucketInfo,
                    entry,
                    usersBucketCreationDatesMap[`${entry.value['owner-id']}${constants.splitter}${bucketName}`],
                    isTransient,
                    locationConfig,
                    {
                        isCold: isObjectCold,
                        isRestoring: isObjectRestoring,
                        isRestored: isObjectRestored,
                    },
                );

                if (error) {
                    log.error('Failed to process entry data', {
                        method: 'getObjectMDStats',
                        entry,
                        error,
                    });
                    monitoring.objectsCount.inc({ status: 'error' });
                    return;
                }

                if (!data) {
                    // Skipping entry, esp. in case of PHD
                    log.info('Skipping entry', {
                        method: 'getObjectMDStats',
                        entry,
                    });
                    monitoring.objectsCount.inc({ status: 'skipped' });
                    return;
                }

                let targetCount;
                let targetData;
                if (entry._id.indexOf('\0') !== -1) {
                    // versioned item
                    targetCount = 'versionCount';
                    targetData = 'versionData';

                    if (entry.value.replicationInfo.backends.length > 0
                        && this._isReplicationEntryStalled(entry, cmpDate)) {
                        stalledCount++;
                    }
                } else if (!!entry.value.versionId && !entry.value.isNull) {
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

                // Dynamically get the metrics based on the object state
                if (isObjectCold) {
                    targetCount += 'Cold';
                    targetData += 'Cold';
                } else if (isObjectRestoring) {
                    targetCount += 'Restoring';
                    targetData += 'Restoring';
                } else if (isObjectRestored) {
                    targetCount += 'Restored';
                    targetData += 'Restored';
                }

                if (isMPUPart || isOverviewKey) {
                    targetCount = 'mpuUploadCounts';
                    targetData = 'mpuPartsData';
                }

                Object.keys(data).forEach(metricLevel => {
                    // metricLevel can only be 'bucket', 'location' or 'account'
                    if (validStorageMetricLevels.has(metricLevel)) {
                        Object.keys(data[metricLevel]).forEach(resourceName => {
                            // resourceName can be the name of bucket, location or account
                            if (!collRes[metricLevel][resourceName]) {
                                collRes[metricLevel][resourceName] = {
                                    ...baseMetricsObject,
                                };
                            }
                            collRes[metricLevel][resourceName][targetData] += data[metricLevel][resourceName];
                            // Do not count the MPU parts as objects
                            if (!isMPUPart) {
                                collRes[metricLevel][resourceName][targetCount]++;
                            }
                            collRes[metricLevel][resourceName].deleteMarkerCount += entry.value.isDeleteMarker ? 1 : 0;
                        });
                    }
                });
                Object.keys(data.account).forEach(account => {
                    if (!collRes.account[account].locations) {
                        collRes.account[account].locations = {};
                    }

                    Object.keys(data.location).forEach(location => {
                        if (!collRes.account[account].locations[location]) {
                            collRes.account[account].locations[location] = {
                                ...baseMetricsObject,
                            };
                        }
                        collRes.account[account].locations[location][targetData] += data.location[location];
                        if (!isMPUPart) {
                            collRes.account[account].locations[location][targetCount]++;
                        }
                        collRes.account[account].locations[location].deleteMarkerCount += entry.value.isDeleteMarker ? 1 : 0;
                    });
                });
                // one bucket has only one account
                [accountBucket] = Object.keys(collRes.account);
                monitoring.objectsCount.inc({ status: 'success' });
                processed++;
            };

            await cursor.forEach(
                res => processCursorEntry(res),
            );

            const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
            const collectionMpu = this.getCollection(mpuBucket);
            cursorMpuBucket = collectionMpu.find({});

            // MPU entries from the mpu shadow bucket must be considered
            // as part of the current metrics.
            await cursorMpuBucket.forEach(
                res => {
                    if (res._id.startsWith(`overview${constants.splitter}`)) {
                        // For overview keys, only consider the number of object
                        return processCursorEntry({
                            _id: res._id,
                            value: {
                                'replicationInfo': {
                                    status: '',
                                    backends: [],
                                },
                                'dataStoreName': res.value.dataStoreName,
                                'content-length': 0,
                                'versionId': null,
                                'owner-id': res.value['owner-id'],
                                'isDeleteMarker': false,
                                'isNull': false,
                                'archive': null,
                                'x-amz-storage-class': 'STANDARD',
                                'isPHD': false,
                            },
                        }, false, true);
                    }
                    return processCursorEntry({
                        _id: res._id,
                        value: {
                            'replicationInfo': {
                                status: '',
                                backends: [],
                            },
                            'dataStoreName': res.value.partLocations[0].dataStoreName,
                            'content-length': res.value['content-length'],
                            'versionId': null,
                            'owner-id': res.value['owner-id'],
                            'isDeleteMarker': false,
                            'isNull': false,
                            'archive': null,
                            'x-amz-storage-class': 'STANDARD',
                            'isPHD': false,
                        },
                    }, true);
                },
            );

            const retResult = this._handleResults(collRes, isVer);
            retResult.stalled = stalledCount;

            if (inflightsPreScan > 0 && retResult && retResult.dataMetrics) {
                Object.keys(retResult.dataMetrics.bucket).forEach(key => {
                    retResult.dataMetrics.bucket[key].usedCapacity = {
                        ...retResult.dataMetrics.bucket[key].usedCapacity,
                        _inflightsPreScan: inflightsPreScan,
                    };
                    retResult.dataMetrics.bucket[key].accountOwnerID = accountBucket;
                });
            }

            return callback(null, retResult);
        } catch (err) {
            log.error('An error occurred', {
                method: 'getObjectMDStats',
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return callback(err);
        } finally {
            if (cursor && !cursor.closed) {
                log.info('Finished processing cursor', {
                    method: 'getObjectMDStats',
                    bucketName,
                });
                cursor.close();
            }
        }
    }

    /**
     * @param{string} bucketName -
     * @param{object} bucketInfo - bucket attributes
     * @param{object} entry -
     * @param{string} entry._id -
     * @param{object} entry.value -
     * @param{object} bucketCreationDate -
     * @param{boolean} isTransient -
     * @param{object} locationConfig - locationConfig.json
     * @param{object} objectState - whether the object is cold, restoring or restored
     * @returns{object} results -
     */
    _processEntryData(bucketName, bucketInfo, entry, bucketCreationDate, isTransient, locationConfig, objectState = {
        isCold: false,
        isRestoring: false,
        isRestored: false,
    }) {
        if (!bucketName) {
            return { error: new Error('no bucket name provided') };
        }

        if (entry.value.isPHD) {
            // PHD are created transiently in place of a master when it is deleted, until
            // they get replaced with the "new" master. They may either hold no information
            // (and cannot be processed) or information related to the earlier master (and
            // thus not correct): so best to just ignore them.
            return {};
        }

        const size = Number.parseInt(entry.value['content-length'], 10);
        if (Number.isNaN(size)) {
            return { error: new Error('invalid content length') };
        }

        if (!locationConfig) {
            return { error: new Error('empty locationConfig') };
        }
        const results = {
            // there will be only one bucket for an object entry, and use `bucketName_creationDate` as key
            // creationDate comes from __userbucket collection
            bucket: { [`${bucketName}_${new Date(bucketCreationDate).getTime()}`]: size },
            // there can be multiple locations for an object entry, and use `locationId` as key
            location: {},
            // there will be only one account for an object entry, and use `accountCanonicalId` as key
            account: { [entry.value['owner-id']]: size },
        };

        // only count it in current dataStore if object is not in transient or replication not completed
        if (!isTransient || entry.value.replicationInfo.status !== 'COMPLETED') {
            results.location[entry.value.dataStoreName] = size;
            // We do not support restores to custom location yet. If we do,
            // the destination would be present in the object metadata. For now,
            // we only default to the location constraint of the bucket.
            // The metric is added to the destination location to be consistent
            // with the quotas checks, where the data, while not yet restored,
            // is considered as part of the destination location.
            if (objectState.isRestoring) {
                results.location[bucketInfo.getLocationConstraint()] = size;
            }
        }
        entry.value.replicationInfo.backends.forEach(rep => {
            // count it in the replication destination location if replication compeleted
            if (rep.status === 'COMPLETED') {
                results.location[rep.site] = size;
            }
        });
        // count in both dataStoreName and cold location if object is restored
        if (this._isObjectRestored(entry)) {
            const coldLocation = entry.value['x-amz-storage-class'];
            if (coldLocation && coldLocation !== entry.value.dataStoreName) {
                if (results.location[coldLocation]) {
                    results.location[coldLocation] += size;
                } else {
                    results.location[coldLocation] = size;
                }
            }
        }

        // use location.objectId as key instead of location name
        // return error if location is not in locationConfig
        for (const location of Object.keys(results.location)) {
            if (locationConfig[location]) {
                if (locationConfig[location].objectId !== location) {
                    results.location[locationConfig[location].objectId] = results.location[location];
                    delete results.location[location];
                }
            } else {
                // ignore location if it is not in locationConfig
                delete results.location[location];
            }
        }

        return { data: results };
    }

    _handleResults(res, isVersioned) {
        let totalNonCurrentCount = 0;
        let totalCurrentCount = 0;
        let totalNonCurrentColdCount = 0;
        let totalCurrentColdCount = 0;
        let totalRestoringCount = 0;
        let totalRestoredCount = 0;
        let totalVersionRestoringCount = 0;
        let totalVerionsRestoredCount = 0;

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
                                _currentCold: 0,
                                _nonCurrentCold: 0,
                                _currentRestored: 0,
                                _currentRestoring: 0,
                                _nonCurrentRestored: 0,
                                _nonCurrentRestoring: 0,
                                _incompleteMPUParts: 0,
                            },
                            objectCount: {
                                current: 0,
                                nonCurrent: 0,
                                _currentCold: 0,
                                _nonCurrentCold: 0,
                                _currentRestored: 0,
                                _currentRestoring: 0,
                                _nonCurrentRestored: 0,
                                _nonCurrentRestoring: 0,
                                _incompleteMPUUploads: 0,
                                deleteMarker: 0,
                            },
                        };
                    }
                    const {
                        masterCount = 0,
                        masterData = 0,
                        nullCount = 0,
                        nullData = 0,
                        versionCount = 0,
                        versionData = 0,
                        deleteMarkerCount = 0,
                        masterCountCold = 0,
                        masterDataCold = 0,
                        nullCountCold = 0,
                        nullDataCold = 0,
                        versionCountCold = 0,
                        versionDataCold = 0,
                        deleteMarkerCountCold = 0,
                        masterCountRestoring = 0,
                        masterDataRestoring = 0,
                        nullCountRestoring = 0,
                        nullDataRestoring = 0,
                        versionCountRestoring = 0,
                        versionDataRestoring = 0,
                        deleteMarkerCountRestoring = 0,
                        masterCountRestored = 0,
                        masterDataRestored = 0,
                        nullCountRestored = 0,
                        nullDataRestored = 0,
                        versionCountRestored = 0,
                        versionDataRestored = 0,
                        deleteMarkerCountRestored = 0,
                        mpuUploadCounts = 0,
                        mpuPartsData = 0,
                    } = res[metricLevel][resourceName];

                    dataMetrics[metricLevel][resourceName].usedCapacity.current += nullData + masterData;
                    dataMetrics[metricLevel][resourceName].usedCapacity._currentCold += nullDataCold + masterDataCold;
                    dataMetrics[metricLevel][resourceName].usedCapacity._currentRestoring += nullDataRestoring + masterDataRestoring;
                    dataMetrics[metricLevel][resourceName].usedCapacity._currentRestored += nullDataRestored + masterDataRestored;
                    dataMetrics[metricLevel][resourceName].usedCapacity._incompleteMPUParts += mpuPartsData;
                    dataMetrics[metricLevel][resourceName].objectCount.current += nullCount + masterCount;
                    dataMetrics[metricLevel][resourceName].objectCount._currentCold += nullCountCold + masterCountCold;
                    dataMetrics[metricLevel][resourceName].objectCount._currentRestoring += nullCountRestoring + masterCountRestoring;
                    dataMetrics[metricLevel][resourceName].objectCount._currentRestored += nullCountRestored + masterCountRestored;
                    dataMetrics[metricLevel][resourceName].objectCount._incompleteMPUUploads += mpuUploadCounts;

                    if (isVersioned) {
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent
                            += versionData - masterData; // masterData is duplicated in versionedData
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentCold
                            += versionDataCold - masterDataCold;
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestoring
                            += versionDataRestoring - masterDataRestoring;
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestored
                            += versionDataRestored - masterDataRestored;

                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentCold = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentCold, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestoring = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestoring, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestored = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestored, 0);

                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent
                            += versionCount - masterCount - deleteMarkerCount;
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentCold
                            += versionCountCold - masterCountCold;
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestoring
                            += versionCountRestoring - masterCountRestoring;
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestored
                            += versionCountRestored - masterCountRestored;

                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent = Math.max(dataMetrics[metricLevel][resourceName].objectCount.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentCold = Math.max(dataMetrics[metricLevel][resourceName].objectCount._nonCurrentCold, 0);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestoring = Math.max(dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestoring, 0);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestored = Math.max(dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestored, 0);

                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += deleteMarkerCount;
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += deleteMarkerCountCold;
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += deleteMarkerCountRestoring;
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += deleteMarkerCountRestored;
                    }

                    if (metricLevel === 'location') { // calculate usedCapacity metrics at global and location level
                        // we only count the restoring and restored for non-cold locations
                        totalBytes.curr += (nullData + masterData + nullDataCold + masterDataCold + nullDataRestoring + masterDataRestoring + nullDataRestored + masterDataRestored);
                        if (!locationBytes[resourceName]) {
                            locationBytes[resourceName] = { curr: 0, prev: 0 };
                        }
                        locationBytes[resourceName].curr += (nullData + masterData + nullDataCold + masterDataCold + nullDataRestoring + masterDataRestoring + nullDataRestored + masterDataRestored);
                        if (isVersioned) {
                            totalBytes.prev += (versionData + versionDataCold + versionDataRestoring + versionDataRestored);
                            totalBytes.prev -= (masterData + masterDataCold + masterDataRestoring + masterDataRestored);
                            totalBytes.prev = Math.max(0, totalBytes.prev);
                            locationBytes[resourceName].prev += (versionData + versionDataCold + versionDataRestoring + versionDataRestored);
                            locationBytes[resourceName].prev -= (masterData + masterDataCold + masterDataRestoring + masterDataRestored);
                            locationBytes[resourceName].prev = Math.max(0, locationBytes[resourceName].prev);
                        }
                    }
                    if (metricLevel === 'bucket') { // count objects up of all buckets
                        totalCurrentCount += (masterCount + nullCount);
                        totalNonCurrentCount += isVersioned ? (versionCount - masterCount - deleteMarkerCount) : 0;
                        totalCurrentColdCount += (masterCountCold + nullCountCold);
                        totalNonCurrentColdCount += isVersioned ? (versionCountCold - masterCountCold) : 0;
                        totalRestoringCount += (masterCountRestoring + nullCountRestoring);
                        totalRestoredCount += (masterCountRestored + nullCountRestored);
                        totalVersionRestoringCount += isVersioned ? (versionCountRestoring - masterCountRestoring) : 0;
                        totalVerionsRestoredCount += isVersioned ? (versionCountRestored - masterCountRestored) : 0;
                    }
                });
            }
        });

        // parse all locations and reflect the data in the account
        Object.keys((res.account || {})).forEach(account => {
            if (!dataMetrics.account[account].locations) {
                dataMetrics.account[account].locations = {};
            }
            Object.keys(res.location || {}).forEach(location => {
                if (!dataMetrics.account[account].locations[location]) {
                    dataMetrics.account[account].locations[location] = {};
                }
                const accountLocation = dataMetrics.account[account].locations[location];
                if (!accountLocation.usedCapacity) {
                    accountLocation.usedCapacity = {
                        current: 0,
                        nonCurrent: 0,
                        _currentCold: 0,
                        _nonCurrentCold: 0,
                        _currentRestored: 0,
                        _currentRestoring: 0,
                        _nonCurrentRestored: 0,
                        _nonCurrentRestoring: 0,
                        _incompleteMPUParts: 0,
                    };
                }
                if (!accountLocation.objectCount) {
                    accountLocation.objectCount = {
                        current: 0,
                        nonCurrent: 0,
                        _currentCold: 0,
                        _nonCurrentCold: 0,
                        _currentRestored: 0,
                        _currentRestoring: 0,
                        _nonCurrentRestored: 0,
                        _nonCurrentRestoring: 0,
                        _incompleteMPUUploads: 0,
                        deleteMarker: 0,
                    };
                }
                accountLocation.usedCapacity.current += dataMetrics.location[location].usedCapacity.current;
                accountLocation.usedCapacity.nonCurrent += dataMetrics.location[location].usedCapacity.nonCurrent;
                accountLocation.usedCapacity._currentCold += dataMetrics.location[location].usedCapacity._currentCold;
                accountLocation.usedCapacity._nonCurrentCold += dataMetrics.location[location].usedCapacity._nonCurrentCold;
                accountLocation.usedCapacity._currentRestoring += dataMetrics.location[location].usedCapacity._currentRestoring;
                accountLocation.usedCapacity._nonCurrentRestoring += dataMetrics.location[location].usedCapacity._nonCurrentRestoring;
                accountLocation.usedCapacity._currentRestored += dataMetrics.location[location].usedCapacity._currentRestored;
                accountLocation.usedCapacity._nonCurrentRestored += dataMetrics.location[location].usedCapacity._nonCurrentRestored;
                accountLocation.usedCapacity._incompleteMPUParts += dataMetrics.location[location].usedCapacity._incompleteMPUParts;

                accountLocation.objectCount.current += dataMetrics.location[location].objectCount.current;
                accountLocation.objectCount.nonCurrent += dataMetrics.location[location].objectCount.nonCurrent;
                accountLocation.objectCount._currentCold += dataMetrics.location[location].objectCount._currentCold;
                accountLocation.objectCount._nonCurrentCold += dataMetrics.location[location].objectCount._nonCurrentCold;
                accountLocation.objectCount._currentRestoring += dataMetrics.location[location].objectCount._currentRestoring;
                accountLocation.objectCount._nonCurrentRestoring += dataMetrics.location[location].objectCount._nonCurrentRestoring;
                accountLocation.objectCount._currentRestored += dataMetrics.location[location].objectCount._currentRestored;
                accountLocation.objectCount._nonCurrentRestored += dataMetrics.location[location].objectCount._nonCurrentRestored;
                accountLocation.objectCount._incompleteMPUUploads += dataMetrics.location[location].objectCount._incompleteMPUUploads;

                accountLocation.objectCount.deleteMarker += dataMetrics.location[location].objectCount.deleteMarker;
            });
        });

        return {
            versions: Math.max(0, totalNonCurrentCount + totalNonCurrentColdCount + totalVersionRestoringCount + totalVerionsRestoredCount),
            objects: totalCurrentCount + totalCurrentColdCount + totalRestoringCount + totalRestoredCount,
            dataManaged: {
                total: totalBytes,
                locations: locationBytes,
            },
            dataMetrics,
        };
    }

    async updateBucketCapacityInfo(bucketName, capacityInfo, log, cb) {
        try {
            const m = this.getCollection(METASTORE);
            const updateResult = await m.findOneAndUpdate({
                _id: bucketName,
            }, {
                $set: {
                    '_id': bucketName,
                    'value.capabilities.VeeamSOSApi.CapacityInfo': {
                        Capacity: new Long(capacityInfo.Capacity),
                        Available: new Long(capacityInfo.Available),
                        Used: new Long(capacityInfo.Used),
                        LastModified: (new Date()).toISOString(),
                    },
                },
            }, {
                upsert: false,
            });
            if (!updateResult.ok) {
                log.error('updateBucketCapacityInfo: failed to update bucket CapacityInfo', {
                    bucketName,
                    capacityInfo,
                });
                return cb(new Error('Failed to update bucket CapacityInfo'));
            }
            return cb();
        } catch (err) {
            log.error('updateBucketCapacityInfo: error putting bucket CapacityInfo', {
                error: err.message,
                errDetails: { ...err },
                errorString: err.toString(),
                bucketName,
                capacityInfo,
            });
            return cb(errors.InternalError);
        }
    }

    static convertNumberToLong(obj) {
        if (typeof obj !== 'object' || obj === null) {
            return obj;
        }
        const newObj = {};
        for (const key in obj) {
            if (typeof obj[key] === 'number') {
                // convert number to Long
                newObj[key] = Long.fromNumber(obj[key]);
            } else {
                // recursively convert nested object properties to Long
                newObj[key] = S3UtilsMongoClient.convertNumberToLong(obj[key]);
            }
        }
        return newObj;
    }

    async updateStorageConsumptionMetrics(countItems, dataMetrics, log, cb) {
        try {
            let updatedStorageMetricsList = [
                { _id: __COUNT_ITEMS, value: countItems },
                // iterate every resource through dataMetrics and add to updatedStorageMetricsList
                ...Object.entries(dataMetrics)
                    .filter(([metricLevel]) => validStorageMetricLevels.has(metricLevel))
                    .flatMap(([metricLevel, result]) => Object.entries(result)
                        .map(([resource, metrics]) => ({
                            _id: `${metricLevel}_${resource}`,
                            measuredOn: new Date().toJSON(),
                            ...S3UtilsMongoClient.convertNumberToLong(metrics),
                        }))),
            ];
            log.info('updateStorageConsumptionMetrics: updating storage metrics');

            // update the inflights
            updatedStorageMetricsList = await this.updateInflightDeltas(updatedStorageMetricsList, log);

            // Drop the temporary collection if it exists
            try {
                await this.getCollection(INFOSTORE_TMP).drop();
            } catch (err) {
                if (err.codeName !== 'NamespaceNotFound') {
                    throw err;
                }
            }
            const tempCollection = await this.db.createCollection(INFOSTORE_TMP);
            await tempCollection.insertMany(updatedStorageMetricsList, { ordered: false });
            await async.retry(
                3,
                async () => tempCollection.rename(INFOSTORE, { dropTarget: true }),
            );
            return cb();
        } catch (err) {
            log.error('updateStorageConsumptionMetrics: error updating storage metrics', {
                error: err,
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return cb(errors.InternalError);
        }
    }

    async readStorageConsumptionMetrics(entityName, log, cb) {
        try {
            const i = this.getCollection(INFOSTORE);
            const doc = await i.findOne({ _id: entityName });
            if (!doc) {
                return cb(errors.NoSuchEntity);
            }

            // Keep only relevant metrics: the values are either
            // number or Long, so we first stringify them and
            // create a BigInt for processing.
            const convertedDoc = {
                usedCapacity: {
                    current: BigInt(doc.usedCapacity.current.toString()),
                    nonCurrent: BigInt(doc.usedCapacity.nonCurrent.toString()),
                },
            };

            return cb(null, convertedDoc);
        } catch (err) {
            log.error('readStorageConsumptionMetrics: error reading metrics', {
                error: err,
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return cb(errors.InternalError);
        }
    }

    async readStorageConsumptionInflights(entityName, log) {
        try {
            const i = this.getCollection(INFOSTORE);
            const doc = await i.findOne({ _id: entityName });
            if (!doc || !doc.usedCapacity || !doc.usedCapacity._inflight) {
                return 0;
            }
            return doc.usedCapacity._inflight;
        } catch (err) {
            log.error('readStorageConsumptionInflights: error reading metrics', {
                error: err,
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return 0;
        }
    }

    async getUsersBucketCreationDate(ownerId, bucketName, log, cb) {
        try {
            const usersBucketCol = this.getCollection(USERSBUCKET);
            const res = await usersBucketCol.findOne({
                _id: `${ownerId}${constants.splitter}${bucketName}`,
            }, {
                projection: {
                    'value.creationDate': 1,
                },
            });
            if (!res || !res.value || !res.value.creationDate) {
                log.error('bucket entry not found in __usersbucket', {
                    bucketName,
                    ownerId,
                });
                return cb(new Error('Bucket entry not found'));
            }
            return cb(null, res.value.creationDate);
        } catch (err) {
            log.error('failed to read bucket entry from __usersbucket', {
                bucketName,
                ownerId,
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return cb(err);
        }
    }

    /**
     * Check if the entry is currently in a cold backend
     * @param {Object} entry - the entry to check
     * @return {boolean} - true if the entry is in a cold backend, false otherwise
     */
    _isObjectCold(entry) {
        return entry.value.archive
            && (!entry.value.archive.restoreRequestedAt || entry.value.archive.restoreWillExpireAt <= Date.now());
    }

    /**
     * Check if the entry is currently being restored
     * @param {Object} entry - the entry to check
     * @return {boolean} - true if the entry is being restored, false otherwise
     */
    _isObjectRestoring(entry) {
        return entry.value.archive
            && entry.value.archive.restoreRequestedAt <= Date.now()
            && (!entry.value.archive.restoreCompletedAt || entry.value.archive.restoreCompletedAt > Date.now());
    }

    /**
     * Check if the entry is currently restored
     * @param {Object} entry - the entry to check
     * @return {boolean} - true if the entry is restored, false otherwise
     */
    _isObjectRestored(entry) {
        return entry.value.archive
            && entry.value.archive.restoreCompletedAt && (entry.value.archive.restoreCompletedAt <= Date.now())
            && entry.value.archive.restoreWillExpireAt > Date.now();
    }
}

module.exports = S3UtilsMongoClient;
