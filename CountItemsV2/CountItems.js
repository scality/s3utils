/**
 * The CountItems class handle, while being single-processed,
 * the collection and aggregation of the metrics, while
 * offloading the processing work to MongoDB, saving computing
 * resources, and reducing by a factor of 4.7 the collection
 * duration as compared with the CountItemsV1.
 */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-promise-executor-return */
const { errors, constants } = require('arsenal');
const S3UtilsMongoClient = require('../utils/S3UtilsMongoClient');
const createMongoParams = require('../utils/createMongoParams');
const getLocationConfig = require('../utils/locationConfig');
const { validStorageMetricLevels } = require('../CountItems/utils/constants');

const locationConfig = getLocationConfig(this.log);
const METASTORE_COLLECTION = '__metastore';
const INFOSTORE = '__infostore';
const INFOSTORE_TMP = `${INFOSTORE}_tmp`;
const __COUNT_ITEMS = 'countitems';

// Null location as set for delete markers
const INTERNAL_NULL_LOCATION = '___internal_null_location___';

class CountItems {
    /**
     * Constructor, initializes the MongoDB client
     *
     * @param {Object} config - configuration object
     * @param {Object} config.maxRetries - max number of mongodb conneciton retries
     * @param {werelogs} log - logger
     */
    constructor(config, log) {
        this.db = new S3UtilsMongoClient(createMongoParams(log, {
            readPreference: 'secondaryPreferred',
            readPreferenceOptions: {
                // This option is ensuring that with the artificial lag
                // used to ensure all documents from a given second are
                // properly processed without race conditions, we don't
                // read from a secondary that is too far behind.
                maxStalenessSeconds: config.lastModifiedLagSeconds || 1,
            },
        }));
        this.log = log;
        this.connected = false;
        this.maxRetries = config.maxRetries || 10;

        // CurrentRound holds the number of successful and sequential
        // runs that the service completed.
        this.currentRound = 0;
        // TODO monitoring for the service

        this.maxConcurrentBucketProcessing = config.maxConcurrentOperations || 10;
        this.mongoDBSupportsPreImages = config.mongoDBSupportsPreImages || false;
        this.lastModifiedLagSeconds = config.lastModifiedLagSeconds || 1;
        this.refreshFrequencySeconds = config.refreshFrequencySeconds || 86400;
        this.sleepDurationSecondsBetweenRounds = config.sleepDurationSecondsBetweenRounds || 2;

        // Backward compatibility
        this.store = {
            objects: 0,
            versions: 0,
            bucketList: [],
            dataManaged: {
                total: {
                    curr: 0,
                    prev: 0,
                },
                byLocation: [],
            },
        };

        // bulkedCheckpoints ia sn object used to store all buckets and
        // associated checkpoints, and writen using bulk to mongodb
        // after each round completion.
        this.bulkedCheckpoints = {};

        // pool of async workers. This job is single-processed as all the
        // computations are offloaded to mongodb. This pool is used to
        // ensure that no more than the configured number of concurrent
        // operations are run at the same time.
        this.pool = {};
        this.previousBucketList = {};

        // Detect object deletion
        this.watcher = null;
    }

    /**
     * Refreshes the connection state.
     * @returns {undefined}
     */
    refresh() {
        if (this.db && this.db.client) {
            this.connected = this.db.client.isConnected();
        } else {
            this.connected = false;
        }
    }

    /**
     * Connects to MongoDB, with retries.
     * @returns {Promise} - resolves when the connection is established
     */
    connectWithRetries() {
        this.refresh();

        if (this.connected) {
            this.log.debug('MongoClient is already connected. Skipping setup.');
            return Promise.resolve();
        }

        let retries = 0;
        const delay = 2000;

        return new Promise((resolve, reject) => {
            const tryConnect = () => {
                if (retries >= this.maxRetries) {
                    this.log.error('Max retries reached. Could not connect to MongoDB.');
                    return reject(new Error('Max retries reached'));
                }

                return this.db.setup(err => {
                    if (err) {
                        retries += 1;
                        this.log.error('Error connecting to MongoDB', {
                            error: err,
                            retryCount: retries,
                        });

                        setTimeout(() => tryConnect(), delay);
                    } else {
                        this.connected = true;
                        this.log.debug('Successfully connected to MongoDB.');
                        resolve();
                    }
                });
            };
            tryConnect();
        });
    }

    /**
     * Main function, starts the worker.
     * @returns {undefined}
     */
    async work() {
        this.log.info('Starting worker...');
        await this.connectWithRetries();
        // Initialize the ChangeStream
        this.changeStreamListenDeletion();
        this.resetPool();
        const stop = false;
        let startTime;

        while (!stop) {
            startTime = process.hrtime();
            this.log.info('Starting a new round...');
            await this.listAllBuckets();
            this.log.info(`Found ${Object.keys(this.pool).length} buckets`);

            const bucketNames = Object.keys(this.pool);
            let activeOperations = 0;
            const promises = [];
            for (const bucketName of bucketNames) {
                if (activeOperations >= this.maxConcurrentOperations) {
                    // Wait for one to finish
                    await Promise.race(promises);
                }
                const bucketInfo = this.pool[bucketName];
                if (bucketInfo && !bucketInfo.ongoing) {
                    bucketInfo.ongoing = true;
                    const promise = this.processBucket(bucketName, bucketInfo.doc.value.ownerDisplayName, bucketInfo.doc.value.locationConstraint, bucketInfo.doc.value.transient, bucketInfo.first)
                        .then(result => {
                            bucketInfo.first = false;
                            this.consolidateResults(bucketName, result);
                            this.log.info(`Successfully processed bucket: ${bucketName}`, result);
                            bucketInfo.ongoing = false;
                            promises.splice(promises.indexOf(promise), 1);
                        })
                        .catch(err => {
                            // Force refresh the full bucket in case of error
                            bucketInfo.first = true;
                            this.log.error(`Error processing bucket: ${bucketName}`, { err });
                            bucketInfo.ongoing = false;
                            promises.splice(promises.indexOf(promise), 1);
                        });
                    promises.push(promise);
                    activeOperations++;
                }
            }
            // Wait for all remaining operations to finish
            await Promise.all(promises);
            // then save all the checkpoints
            await this.setCheckPoints();
            // then compute all metrics and save them
            await this.aggregateResults();
            this.log.info(`Round completed in ${process.hrtime(startTime)[0]}s. Restarting in 2 seconds...`);
            // Sleep between two round to avoid overloading the cluster
            await new Promise(r => setTimeout(r, this.sleepDurationSecondsBetweenRounds * 1000));
            // Periodically flush all data according to the configuration
            if (new Date() - this.lastRefreshDate > this.refreshFrequencySeconds * 1000) {
                this.resetPool();
            }
        }
    }

    /**
     * Periodically, the service performs a full refresh of the pool,
     * to ensure no deviation with the truth.
     * @returns {undefined}
     */
    resetPool() {
        this.log.info('Resetting pool...');
        this.lastRefreshDate = new Date();
        for (const bucketName in this.pool) {
            if (Object.hasOwn(this.pool, bucketName)) {
                this.pool[bucketName].metrics = {};
                this.pool[bucketName].first = true;
            }
        }
    }

    /**
     * Checks whether the bucket has the SOS CapacityInfo enabled
     * @param {object} bucketInfo - bucket metadata
     * @returns {boolean} - whether the bucket has the SOS CapacityInfo enabled
     */
    isSOSCapacityInfoEnabled(bucketInfo) {
        return !!(bucketInfo._capabilities
            && bucketInfo._capabilities.VeeamSOSApi
            && bucketInfo._capabilities.VeeamSOSApi.SystemInfo
            && bucketInfo._capabilities.VeeamSOSApi.SystemInfo.ProtocolCapabilities
            && bucketInfo._capabilities.VeeamSOSApi.SystemInfo.ProtocolCapabilities.CapacityInfo === true
            && bucketInfo._capabilities.VeeamSOSApi.CapacityInfo);
    }

    /**
     * Lists all buckets in the METASTORE collection, and
     * updates the pool accordingly.
     * @param {boolean} onlySelectSOSAPIEnabledBuckets - whether to only select buckets with SOSAPI enabled
     * @returns {Promise} - resolves when the bucket list is complete
     */
    async listAllBuckets(onlySelectSOSAPIEnabledBuckets = false) {
        this.log.info('Listing all buckets...');
        const collection = this.db.getCollection(METASTORE_COLLECTION);
        // Store the current bucket list to later compare it with the previous list
        const currentBucketList = {};
        return new Promise((resolve, reject) => {
            const cursor = collection.find({});
            let i = 0;
            cursor.each((err, doc) => {
                i++;
                if (err) {
                    this.log.error('Error while listing buckets', {
                        error: err,
                    });
                    reject(err);
                    return;
                }
                if (!doc) {
                    // At this point, we've processed all documents. Time to check for added/deleted buckets.
                    this.syncPoolWithBucketList(currentBucketList);
                    resolve();
                    return;
                }
                if (onlySelectSOSAPIEnabledBuckets && doc && doc.value && !this.isSOSCapacityInfoEnabled(doc.value)) {
                    return;
                }
                const bucketName = doc._id;
                if (bucketName.includes('..bucket')) {
                    return;
                }
                this.log.info('Listing all buckets: cursor processing...', {
                    bucketNumber: i,
                    bucketName,
                });
                // Assuming the bucket name is stored in `doc._id`
                // Update the current bucket list
                currentBucketList[bucketName] = true;
                // Add the bucket to the async pool if not already present
                if (!this.pool[bucketName]) {
                    this.pool[bucketName] = {
                        doc,
                        ongoing: false,
                        metrics: {},
                        first: true,
                    };
                }
            });
        });
    }

    /**
     * Compares the current bucket list with the previous one, and
     * updates the pool accordingly.
     * @param {array} currentBucketList - current bucket list
     * @returns {undefined}
     */
    syncPoolWithBucketList(currentBucketList) {
        // Detect new buckets and remove deleted ones
        for (const [bucketName] of Object.entries(this.previousBucketList)) {
            if (!currentBucketList[bucketName]) {
                // Bucket has been deleted
                this.log.info(`Bucket ${bucketName} has been deleted.`);
                delete this.pool[bucketName];
            }
        }
        for (const [bucketName] of Object.entries(currentBucketList)) {
            if (!this.previousBucketList[bucketName]) {
                this.log.info(`New bucket ${bucketName} detected.`);
            }
        }
        // Update the previousBucketList to the current state for the next iteration
        this.previousBucketList = currentBucketList;
    }

    /**
     * The checkpoint defines the date from which the objects are being
     * considered, when computing the metrics.
     * The checkpoint is stored directly in the associated
     * bucket metadata, un the METASTORE collection, after
     * successful completion of an aggregation.
     * @param {string} bucketName  - name of the bucket
     * @returns {Promise} - resolves to the checkpoint value
     */
    getCheckpoint(bucketName) {
        this.log.info(`Getting checkpoint for bucket ${bucketName}.`);
        const collection = this.db.getCollection(METASTORE_COLLECTION);
        // find the document whose _id matches the bucket name
        // and get the propery `metrics_checkpoint` as a date string
        return new Promise((resolve, reject) => collection.findOne({ _id: bucketName }, (err, doc) => {
            if (err) {
                // by default, we restart from scratch, in case of error
                this.log.error('Error while retrieving checkpoint', {
                    error: err,
                    bucketName,
                });
                return resolve(0);
            }
            if (!doc) {
                return resolve(0);
            }
            return resolve(doc.value.metrics_checkpoint);
        }));
    }

    /**
     * Same as getCheckpoint, but here we bulk all the writes to mongodb
     * based on the current dictionnary
     * @returns {Promise} - resolves to the checkpoint value
     */
    setCheckPoints() {
        this.log.info('Setting checkpoints...', {
            checkpoints: this.bulkedCheckpoints,
        });
        return new Promise((resolve, reject) => {
            if (Object.keys(this.bulkedCheckpoints).length === 0) {
                this.log.info('No checkpoints to set.');
                return resolve();
            }
            const collection = this.db.getCollection(METASTORE_COLLECTION);
            const bulk = collection.initializeUnorderedBulkOp();
            Object.keys(this.bulkedCheckpoints).forEach(bucketName => {
                bulk.find({ _id: bucketName }).updateOne({
                    $set: {
                        'value.metrics_checkpoint': this.bulkedCheckpoints[bucketName],
                    },
                });
            });
            return bulk.execute((err, result) => {
                if (err) {
                    this.log.error('Error while bulk updating checkpoints', {
                        error: err,
                    });
                    return reject();
                }
                this.log.debug('Bulked checkpoints updated', {
                    result,
                });
                return resolve();
            });
        });
    }

    /**
     * Function to issue an aggregation to MongoDB, in order to extract the
     * metrics from one bucket.
     * The function will extract the stored size of the master, null and version
     * objects, and will return a formatted object with all the metrics
     * for the current bucket and associated account/location, for later
     * processing.
     *
     * The function also accepts a filter, named `last-modified` set as an index,
     * used to limit the number of scanned entries between two scan runs. In this case, a
     * $match is added to the aggregation, on this field, to ensure the objects are
     * $gt the provided value;
     * @param {string} bucketName - name of the bucket
     * @param {string} accountName - name of the account
     * @param {string} bucketLocation - location of the bucket
     * @param {boolean} isTransient - whether this is a transient bucket
     * @param {boolean} isFirstRun - whether this is the first run for this bucket
     * @returns {Promise} - resolves to the metrics object
     */
    async processBucket(bucketName, accountName, bucketLocation, isTransient, isFirstRun = false) {
        this.log.info('Processing bucket...', {
            bucketName,
            accountName,
            bucketLocation,
            isFirstRun,
        });
        // TODO handle mongos
        let replStatus;
        let secondaryInfo;
        try {
            replStatus = await this.db.adminDb.command({ replSetGetStatus: 1 });
            secondaryInfo = replStatus.members.find(member => member.self);
        } catch (err) {
            this.log.warn('Error while getting replSetGetStatus', {
                reason: err,
            });
        }
        const checkpoint = await this.getCheckpoint(bucketName);
        let lastSyncedTimestamp = new Date();

        // TODO exclude user..bucket entries
        const result = await new Promise((resolve, reject) => {
            // Step 1: Get the last replicated optime timestamp from the secondary
            // The reason is that we read from secondaries, and there is no consistency
            // guarantee that the data is replicated to all secondaries at the same time.
            // We cannot use the linearizable read concern as we read from the secondaries,
            // so we need to first get the last sync date of the secondary, and adapt
            // the query 'last-modified' filter based on that.
            lastSyncedTimestamp.setSeconds(lastSyncedTimestamp.getSeconds() - this.lastModifiedLagSeconds);
            lastSyncedTimestamp = lastSyncedTimestamp.toISOString();

            try {
                if (!secondaryInfo) {
                    throw new Error('No secondary member found in replica set');
                }
                const unixTimeInSeconds = secondaryInfo.optime.ts.high_;
                lastSyncedTimestamp = new Date(unixTimeInSeconds * 1000);
                lastSyncedTimestamp.setSeconds(lastSyncedTimestamp.getSeconds() - this.lastModifiedLagSeconds);
                lastSyncedTimestamp = lastSyncedTimestamp.toISOString();
            } catch (err) {
                this.log.warn('Error while getting secondary optime', {
                    reason: err,
                });
            }

            // Step 2: Setup collection and checkpoint
            // We get the current bucket status from the pool;
            if (!this.pool[bucketName]) {
                this.log.error('Bucket not found in pool', {
                    bucketName,
                });
                return reject(new Error('Bucket not found in pool'));
            }
            const collection = this.db.getCollection(bucketName);

            // Step 3: Set the aggregation filter
            let filter = {
                'value.last-modified': { $gt: checkpoint },
            };
            // for the first run, we exclude all objects starting Date.now()
            if (isFirstRun) {
                filter = {
                    'value.last-modified': { $lt: lastSyncedTimestamp },
                };
            } else {
                filter = {
                    'value.last-modified': { $gte: checkpoint, $lt: lastSyncedTimestamp },
                };
            }

            // Step 4: Run the aggregation pipeline
            const operation = collection.aggregate([
                {
                    $match: filter,
                },
                {
                    $project: {
                        _id: 1,
                        // extract the value.dataStoreName ad dataStoreName
                        dataStoreName: '$value.dataStoreName',
                        locationsNames: {
                            $cond: [
                                {
                                    $or: [
                                        { $eq: [isTransient, true] },
                                        { $ne: ['$value.replicationInfo.status', 'COMPLETED'] },
                                    ],
                                },
                                '$value.location.dataStoreName',
                                '$value.replicationInfo.backends.site',
                            ],
                        },
                        coldLocation: {
                            $cond: [
                                {
                                    // detection of deleted restored objects is done by the changestream,
                                    // so we can compute these objects as others.
                                    $and: [
                                        { $eq: ['$value.archive', true] },
                                        { $lte: ['$value.archive.restoreCompletedAt', lastSyncedTimestamp] },
                                        { $gt: ['$value.archive.restoreWillExpireAt', lastSyncedTimestamp] },
                                        { $ne: ['$value["x-amz-storage-class"]', '$value.location.dataStoreName'] },
                                    ],
                                },
                                '$value["x-amz-storage-class"]', // Cold storage location
                                null,
                            ],
                        },
                        contentLength: {
                            $cond: [{ $eq: ['$value.isPHD', true] }, 0, '$value.content-length'],
                        },
                        isNull: {
                            $cond: [{ $eq: ['$value.isPHD', true] }, 0,
                                { $cond: [{ $eq: ['$value.isNull', true] }, 1, 0] }],
                        },
                        isMaster: {
                            $cond: [
                                { $eq: ['$value.isPHD', true] }, 0,
                                {
                                    $cond: [
                                        {
                                            $and: [
                                                { $eq: [{ $indexOfBytes: ['$_id', '\0'] }, -1] },
                                                {
                                                    $or: [
                                                        { $eq: [{ $ifNull: ['$value.isNull', null] }, false] },
                                                        { $eq: [{ $ifNull: ['$value.isNull', null] }, null] },
                                                    ],
                                                },
                                            ],
                                        },
                                        1, 0,
                                    ],
                                },
                            ],
                        },
                        isVersioned: {
                            $cond: [{ $eq: ['$value.isPHD', true] }, 0,
                                { $cond: [{ $ne: [{ $indexOfBytes: ['$_id', '\0'] }, -1] }, 1, 0] }],
                        },
                        isDeleteMarker: {
                            $cond: [{ $eq: ['$value.isPHD', true] }, 0,
                                { $cond: [{ $eq: ['$value.isDeleteMarker', true] }, 1, 0] }],
                        },
                    },
                },
                {
                    $group: {
                        _id: {
                            $ifNull: [
                                { $ifNull: ['$coldLocation', '$locationsNames'] },
                                `${INTERNAL_NULL_LOCATION}_${'$dataStoreName'}`,
                            ],
                        },
                        masterData: { $sum: { $multiply: ['$isMaster', '$contentLength'] } },
                        nullData: { $sum: { $multiply: ['$isNull', '$contentLength'] } },
                        versionData: { $sum: { $multiply: ['$isVersioned', '$contentLength'] } },
                        masterCount: { $sum: '$isMaster' },
                        nullCount: { $sum: '$isNull' },
                        versionCount: { $sum: '$isVersioned' },
                        deleteMarkerCount: { $sum: '$isDeleteMarker' },
                    },
                },
            ], { allowDiskUse: true });

            return resolve(operation.toArray());
        });

        // compute metrics for each location
        const metrics = {};
        result.forEach(_metricsForLocation => {
            if (!metrics[_metricsForLocation._id]) {
                metrics[_metricsForLocation._id] = {};
            }
            metrics[_metricsForLocation._id].masterData = _metricsForLocation.masterData || 0;
            metrics[_metricsForLocation._id].nullData = _metricsForLocation.nullData || 0;
            metrics[_metricsForLocation._id].versionData = _metricsForLocation.versionData || 0;
            metrics[_metricsForLocation._id].masterCount = _metricsForLocation.masterCount || 0;
            metrics[_metricsForLocation._id].nullCount = _metricsForLocation.nullCount || 0;
            metrics[_metricsForLocation._id].versionCount = _metricsForLocation.versionCount || 0;
            metrics[_metricsForLocation._id].deleteMarkerCount = _metricsForLocation.deleteMarkerCount || 0;
        });

        this.bulkedCheckpoints[bucketName] = lastSyncedTimestamp;

        // return the computed metrics as a single object holding all the data
        return new Promise(resolve => resolve({
            accountName,
            bucketName,
            bucketLocation,
            metrics,
        }));
    }

    /**
     * When a full run is complete, the service will compute the final
     * Metrics for each location, each account and each bucket.
     * This function aggregates all the data and dynamically saves the values
     * in the INFOSTORE collection.
     * @returns {Promise} - resolves when the aggregation is complete
     */
    async aggregateResults() {
        try {
            this.log.info('Aggregating results...');
            const dataMetrics = this.aggregateMetrics();
            // eslint-disable-next-line no-console
            console.log(JSON.stringify(dataMetrics), this.store);
            const updatedStorageMetricsList = [
                { _id: __COUNT_ITEMS, value: this.store },
                ...Object.entries(dataMetrics)
                    .filter(([metricLevel]) => validStorageMetricLevels.has(metricLevel))
                    .flatMap(([metricLevel, result]) => Object.entries(result)
                        .map(([resource, metrics]) => ({
                            _id: `${metricLevel}_${resource}`,
                            measuredOn: new Date().toJSON(),
                            ...S3UtilsMongoClient.convertNumberToLong(metrics),
                        }))),
            ];

            // Step 1: Drop the collection
            try {
                await this.db.getCollection(INFOSTORE_TMP).drop();
            } catch (err) {
                if (err.codeName !== 'NamespaceNotFound') {
                    throw err;
                }
            }

            // Step 2: Create a new collection
            const tempCollection = await this.db.db.createCollection(INFOSTORE_TMP);

            // Step 3: Insert many documents into the collection
            await tempCollection.insertMany(updatedStorageMetricsList, { ordered: false });

            // Step 4: Rename the collection
            let renameError = null;
            for (let i = 0; i < 3; i++) {
                try {
                    await tempCollection.rename(INFOSTORE, { dropTarget: true });
                    renameError = null;
                    break;
                } catch (err) {
                    renameError = err;
                    this.log.error('updateStorageConsumptionMetrics: error renaming temp collection, try again', {
                        error: err.message,
                    });
                }
            }

            if (renameError) {
                throw renameError;
            }
        } catch (err) {
            this.log.error('updateStorageConsumptionMetrics: error updating count items', {
                error: err.message,
            });
            throw errors.InternalError;
        }
    }

    /**
     * Recreates the change stream watcher, in case of error.
     * @returns {undefined}
     */
    _recreateWatcher() {
        this.watcher = this.db.watch([{
            $match: {
                'fullDocument.value.deleted': true,
            },
        }]);
    }

    /**
     * Consolidates the results of the aggregation, and computes the
     * final metrics for each location, each account and each bucket.
     * @param {string} bucketName - name of the bucket
     * @param {object} result - result of the aggregation
     * @returns {undefined}
     */
    consolidateResults(bucketName, result) {
        const updateMetrics = (target, source) => {
            if (!target) {
                return;
            }
            for (const key in source) {
                if (Object.hasOwn(source, key)) {
                    // eslint-disable-next-line no-param-reassign
                    target[key] = (target[key] || 0) + source[key];
                }
            }
        };

        if (!bucketName || !this.pool[bucketName]) {
            this.log.error('Bucket not found in pool', { bucketName });
            return;
        }

        if (!result) {
            this.log.error('No result provided', { bucketName });
            return;
        }

        // For each metric location, sum the metrics
        for (const location in result.metrics) {
            if (!Object.hasOwn(result.metrics, location)) {
                continue;
            }
            const objectId = locationConfig[location] ? locationConfig[location].objectId : null;
            // No location config must be ignored
            if (!objectId && !location.startsWith(INTERNAL_NULL_LOCATION)) {
                this.log.warn('No location config found for location', { location });
                continue;
            }
            let realLocation = location;
            // This code is not (yet) useful but helps detecting delete markers, if custom
            // logic is needed here
            if (location.startsWith(INTERNAL_NULL_LOCATION)) {
                realLocation = location.replace(`${INTERNAL_NULL_LOCATION}_`, '');
            }
            // Initialize metrics object for objectId if it doesn't exist
            if (!this.pool[bucketName].metrics[objectId]) {
                this.pool[bucketName].metrics[objectId] = {};
            }
            // If there was an old location with the same objectId, we need to consolidate
            if (this.pool[bucketName].metrics[location] && objectId !== location) {
                updateMetrics(this.pool[bucketName].metrics[objectId], this.pool[bucketName].metrics[location]);
                delete this.pool[bucketName].metrics[location];
            }
            // Update metrics
            updateMetrics(this.pool[bucketName].metrics[objectId], result.metrics[location]);
        }
    }

    /**
     * Aggregates the metrics for each bucket, location and account.
     *
     * @returns {object} - the aggregated metrics
     */
    aggregateMetrics() {
        const result = {
            bucket: {},
            location: {},
            account: {},
        };

        let totalObjects = 0;
        let totalVersions = 0;
        let totalCurrent = 0;
        let totalNonCurrent = 0;

        for (const [bucketName, bucketData] of Object.entries(this.pool)) {
            const { doc: { value: { owner, locationConstraint } }, metrics } = bucketData;
            const isVersioned = !!((bucketData.doc.value.versioningConfiguration
                && bucketData.doc.value.versioningConfiguration.Status === 'Enabled'));

            // Initialize if not already
            if (!result.bucket[bucketName]) {
                result.bucket[bucketName] = {
                    usedCapacity: { current: 0, nonCurrent: 0 },
                    objectCount: { current: 0, nonCurrent: 0, deleteMarker: 0 },
                };
            }
            if (!result.location[locationConstraint]) {
                result.location[locationConstraint] = {
                    usedCapacity: { current: 0, nonCurrent: 0 },
                    objectCount: { current: 0, nonCurrent: 0, deleteMarker: 0 },
                };
            }
            if (!result.account[owner]) {
                result.account[owner] = {
                    usedCapacity: { current: 0, nonCurrent: 0 },
                    objectCount: { current: 0, nonCurrent: 0, deleteMarker: 0 },
                    locations: {},
                };
            }

            // Aggregate metrics
            for (const [_, metric] of Object.entries(metrics)) {
                const {
                    masterData, nullData, versionData, masterCount, nullCount, versionCount, deleteMarkerCount,
                } = metric;
                const currentData = masterData + nullData;
                const currentCount = masterCount + nullCount;
                const nonCurrentData = isVersioned ? versionData - masterData : 0;
                const nonCurrentCount = isVersioned ? versionCount - masterCount - deleteMarkerCount : 0;
                totalObjects += (currentCount + nonCurrentCount);
                totalVersions += (nonCurrentCount + deleteMarkerCount); // TODO: is a delete marker a version? depends on requirements.
                totalCurrent += currentData;
                totalNonCurrent += nonCurrentData;

                // Aggregate bucket metrics
                result.bucket[bucketName].usedCapacity.current += currentData;
                result.bucket[bucketName].objectCount.current += currentCount;
                if (isVersioned) {
                    result.bucket[bucketName].usedCapacity.nonCurrent += nonCurrentData;
                    result.bucket[bucketName].objectCount.nonCurrent += nonCurrentCount;
                    result.bucket[bucketName].objectCount.deleteMarker += deleteMarkerCount;
                }

                // Aggregate location metrics
                result.location[locationConstraint].usedCapacity.current += currentData;
                result.location[locationConstraint].objectCount.current += currentCount;
                if (isVersioned) {
                    result.location[locationConstraint].usedCapacity.nonCurrent += nonCurrentData;
                    result.location[locationConstraint].objectCount.nonCurrent += nonCurrentCount;
                    result.location[locationConstraint].objectCount.deleteMarker += deleteMarkerCount;
                }

                // Aggregate account metrics
                if (!result.account[owner].locations[locationConstraint]) {
                    result.account[owner].locations[locationConstraint] = {
                        usedCapacity: { current: 0, nonCurrent: 0 },
                        objectCount: { current: 0, nonCurrent: 0, deleteMarker: 0 },
                    };
                }
                result.account[owner].usedCapacity.current += currentData;
                result.account[owner].objectCount.current += currentCount;
                if (isVersioned) {
                    result.account[owner].usedCapacity.nonCurrent += nonCurrentData;
                    result.account[owner].objectCount.nonCurrent += nonCurrentCount;
                    result.account[owner].objectCount.deleteMarker += deleteMarkerCount;
                }
            }
        }

        // compute this.store
        this.store.objects = totalObjects;
        this.store.versions = totalVersions;
        this.store.bucketList = Object.keys(this.pool).map(bucketName => ({
            name: bucketName,
            location: this.pool[bucketName].doc.value.locationConstraint,
            isVersioned: !!((this.pool[bucketName].doc.value.versioningConfiguration
                && this.pool[bucketName].doc.value.versioningConfiguration.Status === 'Enabled')),
            ownerCanonicalId: this.pool[bucketName].doc.value.owner,
            ingestion: !!this.pool[bucketName].doc.value.ingestion,
        }));
        this.store.dataManaged.total.curr = totalCurrent;
        this.store.dataManaged.total.prev = totalNonCurrent;
        this.store.dataManaged.byLocation = Object.keys(result.location).map(location => ({
            location,
            curr: result.location[location].usedCapacity.current,
            prev: result.location[location].usedCapacity.current + result.location[location].usedCapacity.nonCurrent,
        }));

        return result;
    }

    /**
     * Detect objects that are deleted while the aggregations are running.
     * The documents whose bucketName is in the pool (after the first successful run)
     * are the eligible events. In this case, simply substract the values
     * from the associated and known metrics.
     * @returns {Promise} - resolves to the checkpoint value
     */
    changeStreamListenDeletion() {
        const dbClient = this.db.client.db(this.db.database);
        // filter of operation type with fullDocument.value.deleted set to true
        let watcher = dbClient.watch([{
            $match: {
                'operationType': 'update',
                'updateDescription.updatedFields.value.deleted': true,
            },
        }]);

        // Listen for changes
        watcher.on('change', change => {
            // ignore unknown buckets: they are yet to be processed
            if (!this.pool[change.ns.coll]) {
                return;
            }
            this.log.debug('Change stream event', {
                change,
            });
            const size = change.updateDescription.updatedFields.value['content-length'];
            let type;
            let typeCount;
            if (change.documentKey._id.indexOf('\0') !== -1) {
                type = 'versionData';
                typeCount = 'versionCount';
            } else if (
                !change.updateDescription.updatedFields.value.versionId
                || (!!change.updateDescription.updatedFields.value.versionId
                    && !change.updateDescription.updatedFields.value.isNull)) {
                type = 'masterData';
                typeCount = 'masterCount';
            } else {
                type = 'nullData';
                typeCount = 'nullCount';
            }
            // Do not process object if last modified date is after the current
            // scan date.
            if (change.updateDescription.updatedFields.value['last-modified']
                > this.bulkedCheckpoints[change.ns.coll]) {
                return;
            }
            if (!change.updateDescription.updatedFields.value.location || !Array.isArray(change.updateDescription.updatedFields.value.location)) {
                // delete marker: ignore
                return;
            }
            change.updateDescription.updatedFields.value.location.forEach(_location => {
                const location = locationConfig[_location.dataStoreName] ? locationConfig[_location.dataStoreName].objectId : null;
                // Check to avoid race conditions, while the bucket was processed from Mongo
                // but not yet in the service.
                if (!this.pool[change.ns.coll].metrics[location]) {
                    return;
                }
                if (!this.pool[change.ns.coll].metrics[location][type]) {
                    this.pool[change.ns.coll].metrics[location][type] = 0;
                    this.pool[change.ns.coll].metrics[location][typeCount] = 0;
                }
                // Process the sizes
                this.pool[change.ns.coll].metrics[location][type] = Math.max(0, this.pool[change.ns.coll].metrics[location][type] - size);
                // Process the counts
                this.pool[change.ns.coll].metrics[location][typeCount] = Math.max(0, this.pool[change.ns.coll].metrics[location][typeCount] - 1);
            });
        });

        // Listen for errors
        watcher.on('error', error => {
            this.log.error('Error in change stream', { error });
            // Close the errored change stream
            watcher.close();
            // Recreate the watcher
            watcher = dbClient.watch([{
                $match: {
                    'fullDocument.value.deleted': true,
                },
            }]);
            // Since the watcher is recreated, we need to set up the event handlers again
            watcher.removeAllListeners();
            watcher.on('change', this.changeStreamListenDeletion.bind(this));
            watcher.on('error', this.changeStreamListenDeletion.bind(this));
        });
    }
}

module.exports = CountItems;

// todo:
// - fix secondary optime check
// - detect object replacement
