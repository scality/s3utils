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

const bigIntMax = (...args) => args.reduce((m, e) => (e > m ? BigInt(e) : BigInt(m)));

BigInt.prototype.toJSON = () => this.toString();

const baseMetricsObject = {
    masterCount: BigInt(0),
    masterData: BigInt(0),
    nullCount: BigInt(0),
    nullData: BigInt(0),
    versionCount: BigInt(0),
    versionData: BigInt(0),
    deleteMarkerCount: BigInt(0),
    masterCountCold: BigInt(0),
    masterDataCold: BigInt(0),
    nullCountCold: BigInt(0),
    nullDataCold: BigInt(0),
    versionCountCold: BigInt(0),
    versionDataCold: BigInt(0),
    deleteMarkerCountCold: BigInt(0),
    masterCountRestoring: BigInt(0),
    masterDataRestoring: BigInt(0),
    nullCountRestoring: BigInt(0),
    nullDataRestoring: BigInt(0),
    versionCountRestoring: BigInt(0),
    versionDataRestoring: BigInt(0),
    deleteMarkerCountRestoring: BigInt(0),
    masterCountRestored: BigInt(0),
    masterDataRestored: BigInt(0),
    nullCountRestored: BigInt(0),
    nullDataRestored: BigInt(0),
    versionCountRestored: BigInt(0),
    versionDataRestored: BigInt(0),
    deleteMarkerCountRestored: BigInt(0),
    mpuUploadCounts: BigInt(0),
    mpuPartsData: BigInt(0),
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
            const TEST = await this.getCollection(USERSBUCKET).countDocuments({});
            const TEST2 = await this.getCollection(METASTORE).countDocuments({});
            const TEST3 = await this.getCollection(INFOSTORE).countDocuments({});
            console.log('TEST', { TEST, TEST2, TEST3 });
            const usersBucketCreationDatesArray = await cursorUsersBucketCreationDates.toArray();
            console.log('USERS BUCKET CREATION DATES ARRAY', usersBucketCreationDatesArray);
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
        console.log('WE ARE HEEEEEEEEERE', allMetrics);
        try {
            if (!allMetrics || !Array.isArray(allMetrics) || allMetrics.length === 0) {
                return allMetrics;
            }

            cursor = await this.getCollection(INFOSTORE).find({}, {
                projection: {
                    'usedCapacity._inflight': 1,
                },
            });

            // console.log('CURSOR', cursor);
            const inflights = await cursor.toArray();
            // convert inflights to a map with _id: usedCapacity._inflight
            const inflightsMap = inflights.reduce((map, obj) => {
                const inflightLong = obj.usedCapacity && obj.usedCapacity._inflight ? obj.usedCapacity._inflight : BigInt(0);
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
                    const inflight = inflightDocument ? bigIntMax(0, inflightDocument - entry.usedCapacity._inflightsPreScan) : BigInt(0);
                    if (inflight) {
                        const inflightLong = BigInt(inflight);
                        // Inflights remaining after the scan are part of the "current" bytes,
                        // and stored in _inflightsDelta
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity.current = BigInt(entry.usedCapacity.current) + inflightLong;
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity._inflightsDelta = inflightLong;
                        const accountOwnerId = `account_${entry.accountOwnerID}`;
                        if (accountInflights[accountOwnerId]) {
                            accountInflights[accountOwnerId] = BigInt(accountInflights[accountOwnerId]) + inflightLong;
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
                        entry.usedCapacity.current = BigInt(entry.usedCapacity.current) + BigInt(accountInflights[id]);
                        // eslint-disable-next-line no-param-reassign
                        entry.usedCapacity._inflightsDelta = BigInt(accountInflights[id]);
                    }
                }
            });
            console.log('ALL METRICS', allMetrics);
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
        console.log('WE ARE HEEERE', bucketName);
        try {
            const c = this.getCollection(bucketName);
            console.log('HERE C ', c);
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
            console.log('CURSOR', cursor);
            const collRes = {
                bucket: {}, // bucket level metrics
                location: {}, // location level metrics
                account: {}, // account level metrics
            };
            let stalledCount = 0;
            let bucketKey;
            let inflightsPreScan = BigInt(0);
            let accountBucket;
            const cmpDate = new Date();
            cmpDate.setHours(cmpDate.getHours() - 1);

            const locationConfig = getLocationConfig(log);

            const usersBucketCreationDatesMap = await this._getUsersBucketCreationDates(log);
            console.log('USERS BUCKET CREATION DATES MAP', usersBucketCreationDatesMap);
            const bucketStatus = bucketInfo.getVersioningConfiguration();
            const isVer = (bucketStatus && (bucketStatus.Status === 'Enabled'
                || bucketStatus.Status === 'Suspended'));

            if (!usersBucketCreationDatesMap) {
                console.log('ERRORS INTERNAL', usersBucketCreationDatesMap);
                return callback(errors.InternalError);
            }

            const bucketDate = usersBucketCreationDatesMap[`${bucketInfo.getOwner()}${constants.splitter}${bucketName}`];
            console.log('BUCKET DATE', bucketDate);
            if (bucketDate) {
                bucketKey = `bucket_${bucketName}_${new Date(bucketDate).getTime()}`;
                console.log('BUCKET KEY', bucketKey);
                if (bucketKey) {
                    inflightsPreScan = await this.readStorageConsumptionInflights(bucketKey, log);
                    console.log('INGLIGHTS PRE SCAN', inflightsPreScan);
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
                            collRes[metricLevel][resourceName][targetData] += BigInt(data[metricLevel][resourceName]);
                            // Do not count the MPU parts as objects
                            if (!isMPUPart) {
                                collRes[metricLevel][resourceName][targetCount]++;
                            }
                            collRes[metricLevel][resourceName].deleteMarkerCount += entry.value.isDeleteMarker ? BigInt(1) : BigInt(0);
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
                        collRes.account[account].locations[location][targetData] += BigInt(data.location[location]);
                        if (!isMPUPart) {
                            collRes.account[account].locations[location][targetCount]++;
                        }
                        collRes.account[account].locations[location].deleteMarkerCount += entry.value.isDeleteMarker ? BigInt(1) : BigInt(0);
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

            if (inflightsPreScan > BigInt(0) && retResult && retResult.dataMetrics) {
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
        console.log('HEEEEEERE 2');
        if (!bucketName) {
            return { error: new Error('no bucket name provided') };
        }

        console.log('bucketCreationDate', bucketCreationDate);

        if (entry.value.isPHD) {
            // PHD are created transiently in place of a master when it is deleted, until
            // they get replaced with the "new" master. They may either hold no information
            // (and cannot be processed) or information related to the earlier master (and
            // thus not correct): so best to just ignore them.
            return {};
        }

        console.log('entry.value', entry.value);

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
        console.log('DATAAAAAA RESULTS', results);
        return { data: results };
    }

    _handleResults(res, isVersioned) {
        let totalNonCurrentCount = BigInt(0);
        let totalCurrentCount = BigInt(0);
        let totalNonCurrentColdCount = BigInt(0);
        let totalCurrentColdCount = BigInt(0);
        let totalRestoringCount = BigInt(0);
        let totalRestoredCount = BigInt(0);
        let totalVersionRestoringCount = BigInt(0);
        let totalVerionsRestoredCount = BigInt(0);

        const totalBytes = { curr: BigInt(0), prev: BigInt(0) };
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
                                current: BigInt(0),
                                nonCurrent: BigInt(0),
                                _currentCold: BigInt(0),
                                _nonCurrentCold: BigInt(0),
                                _currentRestored: BigInt(0),
                                _currentRestoring: BigInt(0),
                                _nonCurrentRestored: BigInt(0),
                                _nonCurrentRestoring: BigInt(0),
                                _incompleteMPUParts: BigInt(0),
                            },
                            objectCount: {
                                current: BigInt(0),
                                nonCurrent: BigInt(0),
                                _currentCold: BigInt(0),
                                _nonCurrentCold: BigInt(0),
                                _currentRestored: BigInt(0),
                                _currentRestoring: BigInt(0),
                                _nonCurrentRestored: BigInt(0),
                                _nonCurrentRestoring: BigInt(0),
                                _incompleteMPUUploads: BigInt(0),
                                deleteMarker: BigInt(0),
                            },
                        };
                    }
                    const {
                        masterCount = BigInt(0),
                        masterData = BigInt(0),
                        nullCount = BigInt(0),
                        nullData = BigInt(0),
                        versionCount = BigInt(0),
                        versionData = BigInt(0),
                        deleteMarkerCount = BigInt(0),
                        masterCountCold = BigInt(0),
                        masterDataCold = BigInt(0),
                        nullCountCold = BigInt(0),
                        nullDataCold = BigInt(0),
                        versionCountCold = BigInt(0),
                        versionDataCold = BigInt(0),
                        deleteMarkerCountCold = BigInt(0),
                        masterCountRestoring = BigInt(0),
                        masterDataRestoring = BigInt(0),
                        nullCountRestoring = BigInt(0),
                        nullDataRestoring = BigInt(0),
                        versionCountRestoring = BigInt(0),
                        versionDataRestoring = BigInt(0),
                        deleteMarkerCountRestoring = BigInt(0),
                        masterCountRestored = BigInt(0),
                        masterDataRestored = BigInt(0),
                        nullCountRestored = BigInt(0),
                        nullDataRestored = BigInt(0),
                        versionCountRestored = BigInt(0),
                        versionDataRestored = BigInt(0),
                        deleteMarkerCountRestored = BigInt(0),
                        mpuUploadCounts = BigInt(0),
                        mpuPartsData = BigInt(0),
                    } = res[metricLevel][resourceName];

                    dataMetrics[metricLevel][resourceName].usedCapacity.current += BigInt(nullData) + BigInt(masterData);
                    dataMetrics[metricLevel][resourceName].usedCapacity._currentCold += BigInt(nullDataCold) + BigInt(masterDataCold);
                    dataMetrics[metricLevel][resourceName].usedCapacity._currentRestoring += BigInt(nullDataRestoring) + BigInt(masterDataRestoring);
                    dataMetrics[metricLevel][resourceName].usedCapacity._currentRestored += BigInt(nullDataRestored) + BigInt(masterDataRestored);
                    dataMetrics[metricLevel][resourceName].usedCapacity._incompleteMPUParts += BigInt(mpuPartsData);
                    dataMetrics[metricLevel][resourceName].objectCount.current += BigInt(nullCount) + BigInt(masterCount);
                    dataMetrics[metricLevel][resourceName].objectCount._currentCold += BigInt(nullCountCold) + BigInt(masterCountCold);
                    dataMetrics[metricLevel][resourceName].objectCount._currentRestoring += BigInt(nullCountRestoring) + BigInt(masterCountRestoring);
                    dataMetrics[metricLevel][resourceName].objectCount._currentRestored += BigInt(nullCountRestored) + BigInt(masterCountRestored);
                    dataMetrics[metricLevel][resourceName].objectCount._incompleteMPUUploads += BigInt(mpuUploadCounts);

                    if (isVersioned) {
                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent
                            += BigInt(versionData) - BigInt(masterData); // masterData is duplicated in versionedData
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentCold
                            += BigInt(versionDataCold) - BigInt(masterDataCold);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestoring
                            += BigInt(versionDataRestoring) - BigInt(masterDataRestoring);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestored
                            += BigInt(versionDataRestored) - BigInt(masterDataRestored);

                        dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent = bigIntMax(dataMetrics[metricLevel][resourceName].usedCapacity.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentCold = bigIntMax(dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentCold, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestoring = bigIntMax(dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestoring, 0);
                        dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestored = bigIntMax(dataMetrics[metricLevel][resourceName].usedCapacity._nonCurrentRestored, 0);

                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent
                            += BigInt(versionCount) - BigInt(masterCount) - BigInt(deleteMarkerCount);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentCold
                            += BigInt(versionCountCold) - BigInt(masterCountCold);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestoring
                            += BigInt(versionCountRestoring) - BigInt(masterCountRestoring);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestored
                            += BigInt(versionCountRestored) - BigInt(masterCountRestored);

                        dataMetrics[metricLevel][resourceName].objectCount.nonCurrent = bigIntMax(dataMetrics[metricLevel][resourceName].objectCount.nonCurrent, 0);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentCold = bigIntMax(dataMetrics[metricLevel][resourceName].objectCount._nonCurrentCold, 0);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestoring = bigIntMax(dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestoring, 0);
                        dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestored = bigIntMax(dataMetrics[metricLevel][resourceName].objectCount._nonCurrentRestored, 0);

                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += BigInt(deleteMarkerCount);
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += BigInt(deleteMarkerCountCold);
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += BigInt(deleteMarkerCountRestoring);
                        dataMetrics[metricLevel][resourceName].objectCount.deleteMarker += BigInt(deleteMarkerCountRestored);
                    }

                    if (metricLevel === 'location') { // calculate usedCapacity metrics at global and location level
                        // we only count the restoring and restored for non-cold locations
                        totalBytes.curr += BigInt(nullData) + BigInt(masterData) + BigInt(nullDataCold) + BigInt(masterDataCold) + BigInt(nullDataRestoring) + BigInt(masterDataRestoring) + BigInt(nullDataRestored) + BigInt(masterDataRestored);
                        if (!locationBytes[resourceName]) {
                            locationBytes[resourceName] = { curr: BigInt(0), prev: BigInt(0) };
                        }
                        locationBytes[resourceName].curr += BigInt(nullData) + BigInt(masterData) + BigInt(nullDataCold) + BigInt(masterDataCold) + BigInt(nullDataRestoring) + BigInt(masterDataRestoring) + BigInt(nullDataRestored) + BigInt(masterDataRestored);
                        if (isVersioned) {
                            totalBytes.prev += BigInt(versionData) + BigInt(versionDataCold) + BigInt(versionDataRestoring) + BigInt(versionDataRestored);
                            totalBytes.prev -= BigInt(masterData) + BigInt(masterDataCold) + BigInt(masterDataRestoring) + BigInt(masterDataRestored);
                            totalBytes.prev = bigIntMax(0, totalBytes.prev);
                            locationBytes[resourceName].prev += BigInt(versionData) + BigInt(versionDataCold) + BigInt(versionDataRestoring) + BigInt(versionDataRestored);
                            locationBytes[resourceName].prev -= BigInt(masterData) + BigInt(masterDataCold) + BigInt(masterDataRestoring) + BigInt(masterDataRestored);
                            locationBytes[resourceName].prev = bigIntMax(0, locationBytes[resourceName].prev);
                        }
                    }
                    if (metricLevel === 'bucket') { // count objects up of all buckets
                        totalCurrentCount += BigInt(masterCount + nullCount);
                        totalNonCurrentCount += isVersioned ? BigInt(versionCount - masterCount - deleteMarkerCount) : BigInt(0);
                        totalCurrentColdCount += BigInt(masterCountCold + nullCountCold);
                        totalNonCurrentColdCount += isVersioned ? BigInt(versionCountCold - masterCountCold) : BigInt(0);
                        totalRestoringCount += BigInt(masterCountRestoring + nullCountRestoring);
                        totalRestoredCount += BigInt(masterCountRestored + nullCountRestored);
                        totalVersionRestoringCount += isVersioned ? BigInt(versionCountRestoring - masterCountRestoring) : BigInt(0);
                        totalVerionsRestoredCount += isVersioned ? BigInt(versionCountRestored - masterCountRestored) : BigInt(0);
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
                        current: BigInt(0),
                        nonCurrent: BigInt(0),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(0),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    };
                }
                if (!accountLocation.objectCount) {
                    accountLocation.objectCount = {
                        current: BigInt(0),
                        nonCurrent: BigInt(0),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(0),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                        deleteMarker: BigInt(0),
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
            versions: bigIntMax(0, totalNonCurrentCount + totalNonCurrentColdCount + totalVersionRestoringCount + totalVerionsRestoredCount),
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
            console.log('THIS IS THE OBJ', obj);
            return obj;
        }

        const newObj = {};
        for (const key in obj) {
            if (typeof obj[key] === 'bigint') {
                newObj[key] = Long.fromString(obj[key].toString());
            } else {
                newObj[key] = S3UtilsMongoClient.convertNumberToLong(obj[key]);
            }
        }
        console.log('NEW OBJ', newObj);
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
            console.log('updatedStorageMetricsList', updatedStorageMetricsList);
            updatedStorageMetricsList = await this.updateInflightDeltas(updatedStorageMetricsList, log);

            // Drop the temporary collection if it exists
            try {
                await this.getCollection(INFOSTORE_TMP).drop();
            } catch (err) {
                if (err.codeName !== 'NamespaceNotFound') {
                    console.log('we are actually heeeere');
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

            const convertedDoc = {
                ...doc,
                usedCapacity: {
                    current: BigInt(doc.usedCapacity.current),
                    nonCurrent: BigInt(doc.usedCapacity.nonCurrent),
                },
                objectCount: {
                    current: BigInt(doc.objectCount.current),
                    nonCurrent: BigInt(doc.objectCount.nonCurrent),
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
                return BigInt(0);
            }
            return BigInt(doc.usedCapacity._inflight.toString());
        } catch (err) {
            log.error('readStorageConsumptionInflights: error reading metrics', {
                error: err,
                errDetails: { ...err },
                errorString: err.toString(),
            });
            return BigInt(0);
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
