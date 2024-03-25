/* eslint-disable consistent-return */
const { MongoClientInterface } = require('arsenal').storage.metadata.mongoclient;
const { Long } = require('mongodb');
const { errors, constants } = require('arsenal');
const async = require('async');
const { validStorageMetricLevels } = require('../CountItems/utils/constants');
const getLocationConfig = require('./locationConfig');

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
};

class S3UtilsMongoClient extends MongoClientInterface {
    async getObjectMDStats(bucketName, bucketInfo, isTransient, log, callback) {
        try {
            const c = this.getCollection(bucketName);
            const cursor = c.find({}, {
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
            const cmpDate = new Date();
            cmpDate.setHours(cmpDate.getHours() - 1);

            const locationConfig = getLocationConfig(log);

            const usersBucketCreationDatesArray = await this.getCollection(USERSBUCKET).find({}, {
                projection: {
                    'value.creationDate': 1,
                },
            }).toArray();

            const usersBucketCreationDatesMap = usersBucketCreationDatesArray
                .reduce((map, obj) => ({ ...map, [obj._id]: obj.value.creationDate }), {});
            let startCursorDate = new Date();
            let processed = 0;
            await cursor.forEach(
                res => {
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

                    const isObjectCold = this._isObjectCold(res);
                    const isObjectRestoring = this._isObjectRestoring(res);
                    const isObjectRestored = this._isObjectRestored(res);

                    const { data, error } = this._processEntryData(
                        bucketName,
                        bucketInfo,
                        res,
                        usersBucketCreationDatesMap[`${res.value['owner-id']}${constants.splitter}${bucketName}`],
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
                            entry: res,
                            error,
                        });
                        return;
                    }

                    if (!data) {
                        // Skipping entry, esp. in case of PHD
                        log.info('Skipping entry', {
                            method: 'getObjectMDStats',
                            entry: res,
                        });
                        return;
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
                                collRes[metricLevel][resourceName][targetCount]++;
                                collRes[metricLevel][resourceName].deleteMarkerCount += res.value.isDeleteMarker ? 1 : 0;
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
                            collRes.account[account].locations[location][targetCount]++;
                            collRes.account[account].locations[location].deleteMarkerCount += res.value.isDeleteMarker ? 1 : 0;
                        });
                    });
                    processed++;
                },
                err => {
                    if (err) {
                        log.error('Error when processing mongo entries', {
                            method: 'getObjectMDStats',
                            errDetails: { ...err },
                            errorString: err.toString(),
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

            const bucketStatus = bucketInfo.getVersioningConfiguration();
            const isVer = (bucketStatus && (bucketStatus.Status === 'Enabled'
                || bucketStatus.Status === 'Suspended'));
            const retResult = this._handleResults(collRes, isVer);
            retResult.stalled = stalledCount;

            return callback(null, retResult);
        } catch (err) {
            log.error('An error occurred', {
                method: 'getObjectMDStats',
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return callback(err);
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
                                currentCold: 0,
                                nonCurrentCold: 0,
                                restored: 0,
                                restoring: 0,
                            },
                            objectCount: {
                                current: 0,
                                nonCurrent: 0,
                                currentCold: 0,
                                nonCurrentCold: 0,
                                restored: 0,
                                restoring: 0,
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
                    } = res[metricLevel][resourceName];

                    dataMetrics[metricLevel][resourceName].usedCapacity.current += nullData + masterData;
                    dataMetrics[metricLevel][resourceName].usedCapacity.currentCold += nullDataCold + masterDataCold;
                    dataMetrics[metricLevel][resourceName].usedCapacity.restoring += nullDataRestoring + masterDataRestoring;
                    dataMetrics[metricLevel][resourceName].usedCapacity.restored += nullDataRestored + masterDataRestored;
                    dataMetrics[metricLevel][resourceName].objectCount.current += nullCount + masterCount;
                    dataMetrics[metricLevel][resourceName].objectCount.currentCold += nullCountCold + masterCountCold;
                    dataMetrics[metricLevel][resourceName].objectCount.restoring += nullCountRestoring + masterCountRestoring;
                    dataMetrics[metricLevel][resourceName].objectCount.restored += nullCountRestored + masterCountRestored;

                    if (isVersioned) {
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent
                            += versionData - masterData; // masterData is duplicated in versionedData
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrentCold
                            += versionDataCold - masterDataCold;
                        dataMetrics[metricLevel][resourceName].usedCapacity.restoring
                            += versionDataRestoring - masterDataRestoring;
                        dataMetrics[metricLevel][resourceName].usedCapacity.restored
                            += versionDataRestored - masterDataRestored;

                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrentCold = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrentCold, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity.restoring = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity.restoring, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity.restored = Math.max(dataMetrics[metricLevel][resourceName].usedCapacity.restored, 0);

                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent
                            += versionCount - masterCount - deleteMarkerCount;
                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrentCold
                            += versionCountCold - masterCountCold;
                        dataMetrics[metricLevel][resourceName].objectCount.restoring
                            += versionCountRestoring - masterCountRestoring;
                        dataMetrics[metricLevel][resourceName].objectCount.restored
                            += versionCountRestored - masterCountRestored;

                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent = Math.max(dataMetrics[metricLevel][resourceName].objectCount.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrentCold = Math.max(dataMetrics[metricLevel][resourceName].objectCount.nonCurrentCold, 0);
                        dataMetrics[metricLevel][resourceName].objectCount.restoring = Math.max(dataMetrics[metricLevel][resourceName].objectCount.restoring, 0);
                        dataMetrics[metricLevel][resourceName].objectCount.restored = Math.max(dataMetrics[metricLevel][resourceName].objectCount.restored, 0);

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
                        currentCold: 0,
                        nonCurrentCold: 0,
                        restoring: 0,
                        restored: 0,
                    };
                }
                if (!accountLocation.objectCount) {
                    accountLocation.objectCount = {
                        current: 0,
                        nonCurrent: 0,
                        currentCold: 0,
                        nonCurrentCold: 0,
                        restoring: 0,
                        restored: 0,
                        deleteMarker: 0,
                    };
                }
                accountLocation.usedCapacity.current += dataMetrics.location[location].usedCapacity.current;
                accountLocation.usedCapacity.nonCurrent += dataMetrics.location[location].usedCapacity.nonCurrent;
                accountLocation.usedCapacity.currentCold += dataMetrics.location[location].usedCapacity.currentCold;
                accountLocation.usedCapacity.nonCurrentCold += dataMetrics.location[location].usedCapacity.nonCurrentCold;
                accountLocation.usedCapacity.restoring += dataMetrics.location[location].usedCapacity.restoring;
                accountLocation.usedCapacity.restored += dataMetrics.location[location].usedCapacity.restored;

                accountLocation.objectCount.current += dataMetrics.location[location].objectCount.current;
                accountLocation.objectCount.nonCurrent += dataMetrics.location[location].objectCount.nonCurrent;
                accountLocation.objectCount.currentCold += dataMetrics.location[location].objectCount.currentCold;
                accountLocation.objectCount.nonCurrentCold += dataMetrics.location[location].objectCount.nonCurrentCold;
                accountLocation.objectCount.restoring += dataMetrics.location[location].objectCount.restoring;
                accountLocation.objectCount.restored += dataMetrics.location[location].objectCount.restored;

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
                        Capacity: capacityInfo.Capacity,
                        Available: capacityInfo.Available,
                        Used: capacityInfo.Used,
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
            const updatedStorageMetricsList = [
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
            return cb(null, doc);
        } catch (err) {
            log.error('readStorageConsumptionMetrics: error reading metrics', {
                error: err,
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return cb(errors.InternalError);
        }
    }

    /*
     * Overwrite the getBucketInfos method to specially handle the cases that
     * bucket collection exists but bucket is not in metastore collection.
     * For now, to make the count-items cronjob more robust, we ignore those "bad buckets"
     */
    async getBucketInfos(log, cb) {
        try {
            const bucketInfos = [];
            const collInfos = await this.db.listCollections().toArray();
            for (const value of collInfos) {
                if (this._isSpecialCollection(value.name)) {
                    // skip
                    continue;
                }
                const bucketName = value.name;
                try {
                    // eslint-disable-next-line no-await-in-loop
                    const bucketInfo = await new Promise((resolve, reject) => {
                        this.getBucketAttributes(bucketName, log, (err, info) => {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(info);
                            }
                        });
                    });
                    bucketInfos.push(bucketInfo);
                } catch (err) {
                    if (err.message === 'NoSuchBucket') {
                        log.debug('bucket does not exist in metastore, ignore it', {
                            bucketName,
                        });
                    } else {
                        log.error('failed to get bucket attributes', {
                            bucketName,
                            errDetails: { ...err },
                            errorString: err.toString(),
                        });
                        throw errors.InternalError;
                    }
                }
            }
            return cb(null, {
                bucketCount: bucketInfos.length,
                bucketInfos,
            });
        } catch (err) {
            log.error('could not get list of collections', {
                method: '_getBucketInfos',
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return cb(err);
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
