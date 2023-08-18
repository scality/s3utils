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

        return this.getCollection(USERSBUCKET).find({}, {
            projection: {
                'value.creationDate': 1,
            },
        }).toArray((err, usersBucketCreationDatesArray) => {
            if (err) {
                log.error('Failed to get users bucket creation dates', {
                    method: 'getObjectMDStats',
                    error: err,
                });
                return callback(err);
            }
            const usersBucketCreationDatesMap = usersBucketCreationDatesArray
                .reduce((map, obj) => ({ ...map, [obj._id]: obj.value.creationDate }), {});
            return cursor.forEach(
                res => {
                    const { data, error } = this._processEntryData(
                        bucketName,
                        bucketInfo,
                        res,
                        usersBucketCreationDatesMap[`${res.value['owner-id']}${constants.splitter}${bucketName}`],
                        isTransient,
                        locationConfig,
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
                    Object.keys(data.account).forEach(account => {
                        if (!collRes.account[account].locations) {
                            collRes.account[account].locations = {};
                        }

                        Object.keys(data.location).forEach(location => {
                            if (!collRes.account[account].locations[location]) {
                                collRes.account[account].locations[location] = {
                                    masterCount: 0,
                                    masterData: 0,
                                    nullCount: 0,
                                    nullData: 0,
                                    versionCount: 0,
                                    versionData: 0,
                                    deleteMarkerCount: 0,
                                };
                            }
                            collRes.account[account].locations[location][targetData] += data.location[location];
                            collRes.account[account].locations[location][targetCount]++;
                            collRes.account[account].locations[location].deleteMarkerCount += res.value.isDeleteMarker ? 1 : 0;
                        });
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
        });
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
     * @returns{object} results -
     */
    _processEntryData(bucketName, bucketInfo, entry, bucketCreationDate, isTransient, locationConfig) {
        if (!bucketName) {
            return { error: new Error('no bucket name provided') };
        }

        if (entry.value.isPHD && !entry.value.hasOwnProperty('content-length')) {
            // In some case, PHD are re-created form scratch and do not hold any information
            // (when deleting a delete marker which is the latest version). This should be very
            // transient (they should be cleaned after 5 seconds), but should not create any
            // error.
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

        // count in both dataStoreName and cold location if object is restored
        if (entry.value.archive
            && entry.value.archive.restoreCompletedAt <= Date.now()
            && entry.value.archive.restoreWillExpireAt > Date.now()) {
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
                    };
                }
                if (!accountLocation.objectCount) {
                    accountLocation.objectCount = {
                        current: 0,
                        nonCurrent: 0,
                        deleteMarker: 0,
                    };
                }
                accountLocation.usedCapacity.current += dataMetrics.location[location].usedCapacity.current;
                accountLocation.usedCapacity.nonCurrent += dataMetrics.location[location].usedCapacity.nonCurrent;
                accountLocation.objectCount.current += dataMetrics.location[location].objectCount.current;
                accountLocation.objectCount.nonCurrent += dataMetrics.location[location].objectCount.nonCurrent;
                accountLocation.objectCount.deleteMarker += dataMetrics.location[location].objectCount.deleteMarker;
            });
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

    updateBucketCapacityInfo(bucketName, capacityInfo, log, cb) {
        const m = this.getCollection(METASTORE);
        m.findOneAndUpdate({
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
        }, err => {
            if (err) {
                log.error(
                    'updateBucketCapacityInfo: error putting bucket CapacityInfo',
                    { error: err.message, bucketName, capacityInfo },
                );
                return cb(errors.InternalError);
            }
            return cb();
        });
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

    updateStorageConsumptionMetrics(countItems, dataMetrics, log, cb) {
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
        let tempCollection;
        async.series([
            next => this.getCollection(INFOSTORE_TMP).drop(err => {
                if (err && err.codeName !== 'NamespaceNotFound') {
                    return next(err);
                }
                return next();
            }),
            next => this.db.createCollection(INFOSTORE_TMP, (err, collection) => {
                if (err) {
                    return next(err);
                }
                tempCollection = collection;
                return next();
            }),
            next => tempCollection.insertMany(updatedStorageMetricsList, { ordered: false }, next),
            next => async.retry(
                3,
                done => tempCollection.rename(INFOSTORE, { dropTarget: true }, done),
                err => {
                    if (err) {
                        log.error('updateStorageConsumptionMetrics: error renaming temp collection, try again', {
                            error: err.message,
                        });
                        return next(err);
                    }
                    return next();
                },
            ),
        ], err => {
            if (err) {
                log.error('updateStorageConsumptionMetrics: error updating count items', {
                    error: err.message,
                });
                return cb(errors.InternalError);
            }
            return cb();
        });
    }

    readStorageConsumptionMetrics(entityName, log, cb) {
        const i = this.getCollection(INFOSTORE);
        return async.retry(
            3,
            done => i.findOne({
                _id: entityName,
            }, done),
            (err, doc) => {
                if (err) {
                    log.error('readStorageConsumptionMetrics: error reading count items', {
                        error: err.message,
                    });
                    return cb(errors.InternalError);
                }
                if (!doc) {
                    return cb(errors.NoSuchEntity);
                }
                return cb(null, doc);
            },
        );
    }

    /*
     * Overwrite the getBucketInfos method to specially handle the cases that
     * bucket collection exists but bucket is not in metastore collection.
     * For now, to make the count-items cronjob more robust, we ignore those "bad buckets"
     */
    getBucketInfos(log, cb) {
        const bucketInfos = [];
        this.db.listCollections().toArray((err, collInfos) => {
            if (err) {
                log.error('could not get list of collections', {
                    method: '_getBucketInfos',
                    error: err,
                });
                return cb(err);
            }
            return async.eachLimit(collInfos, 10, (value, next) => {
                if (this._isSpecialCollection(value.name)) {
                    // skip
                    return next();
                }
                const bucketName = value.name;
                // FIXME: there is currently no way of distinguishing
                // master from versions and searching for VID_SEP
                // does not work because there cannot be null bytes
                // in $regex
                return this.getBucketAttributes(bucketName, log, (err, bucketInfo) => {
                    if (err) {
                        if (err.message === 'NoSuchBucket') {
                            log.debug('bucket does not exist in metastore, ignore it', {
                                bucketName,
                            });
                            return next();
                        }
                        log.error('failed to get bucket attributes', {
                            bucketName,
                            error: err,
                        });
                        return next(errors.InternalError);
                    }
                    bucketInfos.push(bucketInfo);
                    return next();
                });
            }, err => {
                if (err) {
                    return cb(err);
                }
                return cb(null, {
                    bucketCount: bucketInfos.length,
                    bucketInfos,
                });
            });
        });
    }

    getUsersBucketCreationDate(ownerId, bucketName, log, cb) {
        const usersBucketCol = this.getCollection(USERSBUCKET);
        return usersBucketCol.findOne({
            _id: `${ownerId}${constants.splitter}${bucketName}`,
        }, {
            projection: {
                'value.creationDate': 1,
            },
        }, (err, res) => {
            if (err) {
                log.error('failed to read bucket entry from __usersbucket', {
                    bucketName,
                    ownerId,
                    error: err,
                });
                return cb(err);
            }
            return cb(null, res.value.creationDate);
        });
    }
}

module.exports = S3UtilsMongoClient;
