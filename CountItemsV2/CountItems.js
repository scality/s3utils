/**
 * The CountItems class handle, while being single-processed,
 * the collection and aggregation of the metrics, while
 * offloading the processing work to MongoDB, saving computing
 * resources, and reducing by a factor of 4.7 the collection
 * duration as compared with the CountItemsV1.
 */

const S3UtilsMongoClient = require('../utils/S3UtilsMongoClient');
const createMongoParams = require('../utils/createMongoParams');

const METASTORE_COLLECTION = '__metastore';
const INFOSTORE = '__infostore';

class CountItems {
    /**
     * Constructor, initializes the MongoDB client
     *
     * @param {Object} config - configuration object
     * @param {Object} config.maxRetries - max number of mongodb conneciton retries
     * @param {werelogs} log - logger
     */
    constructor(config, log, numberOfReplicas = 1) {
        this.db = new S3UtilsMongoClient(createMongoParams(log));
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

    refresh() {
        if (this.db?.client) {
            this.connected = this.db.client.isConnected();
        } else {
            this.connected = false
        }
    }

    async connectWithRetries() {
        this.refresh(); // Assuming this refreshes the connection state

        if (this.connected) {
            this.log.debug('MongoClient is already connected. Skipping setup.');
            return Promise.resolve();
        }

        let retries = 0;
        const delay = 2000; // Delay in milliseconds between retries

        return new Promise(async (resolve, reject) => {
            while (!this.connected && retries < this.maxRetries) {
                try {
                    await new Promise((innerResolve, innerReject) => {
                        this.db.setup(err => {
                            if (err) {
                                this.log.error('Error connecting to MongoDB', {
                                    error: err,
                                    retryCount: retries,
                                });
                                return innerReject(err);
                            }
                            this.connected = true;
                            return innerResolve();
                        });
                    });

                    this.log.debug('Successfully connected to MongoDB.');
                    resolve();
                    return;

                } catch (err) {
                    retries += 1;
                    if (retries < this.maxRetries) {
                        this.log.error(`Retrying connection to MongoDB. Attempt ${retries} of ${this.maxRetries}.`, {
                            error: err,
                        });
                        await new Promise(r => setTimeout(r, delay));
                    } else {
                        this.log.error('Max retries reached. Could not connect to MongoDB.', {
                            error: err,
                        });
                        reject(new Error('Max retries reached'));
                        return;
                    }
                }
            }
        });
    }

    async work() {
        this.log.info('Starting worker...');
        await this.connectWithRetries();
        // Initialize the ChangeStream
        this.changeStreamListenDeletion();
        this.resetPool();
        let stop = false;
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
                    const promise = this.processBucket(bucketName, bucketInfo.doc.value.ownerDisplayName, bucketInfo.doc.value.locationConstraint, bucketInfo.first)
                        .then((result) => {
                            bucketInfo.first = false;
                            this.consolidateResults(bucketName, result);
                            this.log.info(`Successfully processed bucket: ${bucketName}`, result);
                            bucketInfo.ongoing = false;
                            promises.splice(promises.indexOf(promise), 1);
                        })
                        .catch((err) => {
                            // Force refresh the full bucket in case of error
                            bucketInfo.first = true;
                            this.log.error(`Error processing bucket: ${bucketName}`, { error: err });
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
     */
    resetPool() {
        this.log.info('Resetting pool...');
        this.lastRefreshDate = new Date();
        for (const bucketName in this.pool) {
            this.pool[bucketName].metrics = {};
            this.pool[bucketName].first = true;
        }
    }


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
                this.log.info('Listing all buckets: cursor processing...', {
                    bucketNumber: i,
                    bucketId: doc._id,
                });
                // Assuming the bucket name is stored in `doc._id`
                const bucketName = doc._id;
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
     * @param {array} currentBucketList 
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
        return new Promise((resolve, reject) =>
            collection.findOne({ _id: bucketName }, (err, doc) => {
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
            bulk.execute((err, result) => {
                if (err) {
                    this.log.error('Error while bulk updating checkpoints', {
                        error: err,
                    });
                    return reject();
                }
                this.log.debug('Bulked checkpoints updated', {
                    result,
                });
                resolve();
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
     */
    async processBucket(bucketName, accountName, bucketLocation, isFirstRun = false) {
        this.log.info('Processing bucket...', {
            bucketName,
            accountName,
            bucketLocation,
            isFirstRun,
        });
        // TODO exclude user..bucket entries
        return new Promise(async (resolve, reject) => {
            // Step 1: Get the last replicated optime timestamp from the secondary
            // The reason is that we read from secondaries, and there is no consistency
            // guarantee that the data is replicated to all secondaries at the same time.
            // We cannot use the linearizable read concern as we read from the secondaries,
            // so we need to first get the last sync date of the secondary, and adapt
            // the query 'last-modified' filter based on that.
            let lastSyncedTimestamp = new Date();
            lastSyncedTimestamp.setSeconds(lastSyncedTimestamp.getSeconds() - this.lastModifiedLagSeconds);
            lastSyncedTimestamp = lastSyncedTimestamp.toISOString();

            try {
                const replStatus = await this.db.adminDb.command({ replSetGetStatus: 1 });
                const secondaryInfo = replStatus.members.find(member => member.self);
                if (!secondaryInfo) {
                    this.log.warn('No secondary member found in replica set');
                    return;
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
            this.bulkedCheckpoints[bucketName] = lastSyncedTimestamp;

            // Step 2: Setup collection and checkpoint
            // We get the current bucket status from the pool;
            if (!this.pool[bucketName]) {
                this.log.error('Bucket not found in pool', {
                    bucketName,
                });
                return reject(new Error('Bucket not found in pool'));
            }
            const collection = this.db.getCollection(bucketName);
            const checkpoint = await this.getCheckpoint(bucketName);

            // Step 3: Set the aggregation filter
            let filter = {
                'value.last-modified': { $gt: checkpoint },
            };
            // for the first run, we exclude all objects starting Date.now()
            if (isFirstRun) {
                filter = {
                    'value.last-modified': { $lt: lastSyncedTimestamp }
                };
            } else {
                filter = {
                    'value.last-modified': { $gte: checkpoint, $lt: lastSyncedTimestamp }
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
                        'value.content-length': 1,
                        'value.isNull': 1,
                        'value.isMaster': 1,
                        'value.isDeleteMarker ': 1,
                    },
                },
                {
                    $project: {
                        isMaster: {
                            $cond: [
                                {
                                    $and: [
                                        { $eq: [{ $indexOfBytes: ["$_id", "\0"] }, -1] },
                                        {
                                            $or: [
                                                { $eq: [{ $ifNull: ["$value.isNull", null] }, false] },
                                                { $eq: [{ $ifNull: ["$value.isNull", null] }, null] }
                                            ],
                                        },
                                    ],
                                },
                                1, 0
                            ]
                        },
                        isNull: {
                            $cond: [{ $eq: ["$value.isNull", true] }, 1, 0],
                        },
                        isVersioned: {
                            $cond: [{ $ne: [{ $indexOfBytes: ["$_id", "\0"] }, -1] }, 1, 0],
                        },
                        isDeleteMarker: {
                            $cond: [{ $eq: [{ $ifNull: ["$value.isDeleteMarker", null] }, true] }, 1, 0],
                        },
                        contentLength: "$value.content-length",
                    }
                },
                {
                    $group: {
                        _id: null,
                        masterData: { $sum: { $multiply: ['$isMaster', '$contentLength'] } },
                        nullData: { $sum: { $multiply: ['$isNull', '$contentLength'] } },
                        versionData: { $sum: { $multiply: ['$isVersioned', '$contentLength'] } },
                        masterCount: { $sum: "$isMaster" },
                        nullCount: { $sum: '$isNull' },
                        versionCount: { $sum: '$isVersioned' },
                        deleteMarkerCount: { $sum: '$isDeleteMarker' },
                    }
                }
            ], { allowDiskUse: true });

            // wait till the aggregation is done
            const result = await operation.toArray();
            const metrics = {
                masterData: result?.[0]?.masterData || 0,
                nullData: result?.[0]?.nullData || 0,
                versionData: result?.[0]?.versionData || 0,
                masterCount: result?.[0]?.masterCount || 0,
                nullCount: result?.[0]?.nullCount || 0,
                versionCount: result?.[0]?.versionCount || 0,
                deleteMarkerCount: result?.[0]?.deleteMarkerCount || 0,
            };

            // return the computed metrics as a single object holding all the data
            return resolve({
                accountName,
                bucketName,
                bucketLocation,
                metrics: metrics,
            });
        });
    }

    /**
     * When a full run is complete, the service will compute the final
     * Metrics for each location, each account and each bucket.
     * This function aggregates all the data and dynamically saves the values
     * in the INFOSTORE collection.
     */
    async aggregateResults() {
        this.log.info('Aggregating results...');
    }

    _recreateWatcher() {
        this.watcher = this.db.watch([{
            $match: {
                'fullDocument.value.deleted': true,
            },
        }]);
    }

    consolidateResults(bucketName, result) {
        // TODO rework with actual metrics computations
        const updateMetrics = (target, source) => {
            if (!target) return;
            for (const key in source.metrics) {
                target.metrics[key] = (target.metrics[key] || 0) + source.metrics[key];
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

        updateMetrics(this.pool[bucketName], result);

        const accountMetrics = {};
        const locationMetrics = {};
        const bucketMetrics = {};

        for (const currentBucketName in this.pool) {
            const bucketInfo = this.pool[currentBucketName];
            if (!accountMetrics[bucketInfo.doc.value.ownerDisplayName]) {
                accountMetrics[bucketInfo.doc.value.ownerDisplayName] = { metrics: {} };
            }
            updateMetrics(accountMetrics[bucketInfo.doc.value.ownerDisplayName], bucketInfo);
            if (!locationMetrics[bucketInfo.doc.value.locationConstraint]) {
                locationMetrics[bucketInfo.doc.value.locationConstraint] = { metrics: {} };
            }
            updateMetrics(locationMetrics[bucketInfo.doc.value.locationConstraint], bucketInfo);
            if (!bucketMetrics[currentBucketName]) {
                bucketMetrics[currentBucketName] = { metrics: {} };
            }
            updateMetrics(bucketMetrics[currentBucketName], bucketInfo);
        }
        console.log(bucketMetrics);
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
        watcher.on('change', (change) => {
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
                !change.updateDescription.updatedFields.value.versionId ||
                (!!change.updateDescription.updatedFields.value.versionId &&
                    !change.updateDescription.updatedFields.value.isNull)) {
                type = 'masterData';
                typeCount = 'masterCount';
            } else {
                type = 'nullData';
                typeCount = 'nullCount';
            }
            // Do not process object if last modified date is after the current
            // scan date.
            if (change.updateDescription.updatedFields.value['last-modified'] >
                this.bulkedCheckpoints[change.ns.coll]) {
                return;
            }
            // Process the sizes
            this.pool[change.ns.coll].metrics[type] = Math.max(0, this.pool[change.ns.coll].metrics[type] - size);
            // Process the counts
            this.pool[change.ns.coll].metrics[typeCount] = Math.max(0, this.pool[change.ns.coll].metrics[typeCount] - 1);
        });

        // Listen for errors
        watcher.on('error', (error) => {
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
