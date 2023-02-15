const async = require('async');
const { once } = require('arsenal').jsutil;
const { validStorageMetricLevels } = require('./utils/constants');
const { consolidateDataMetrics } = require('./utils/utils');

class CountManager {
    constructor(params) {
        this.log = params.log;
        this.workers = params.workers;
        this.maxConcurrent = params.maxConcurrent;
        this.temporaryStore = {};
        this.store = {
            objects: 0,
            versions: 0,
            buckets: 0,
            bucketList: [],
            dataManaged: {
                total: { curr: 0, prev: 0 },
                byLocation: {},
            },
            stalled: 0,
        };
        this.dataMetrics = {
            bucket: {},
            location: {},
            account: {},
        };
        this.workerList = [];
        this._setupQueue();
    }

    _setupQueue() {
        this.q = async.queue((bucketInfo, done) => {
            if (this.workerList.length === 0) {
                return done(new Error('emptyWorkerList'));
            }
            const id = this.workerList.shift();
            return this.workers[id].count(bucketInfo, (err, res) => {
                if (err) {
                    return done(err);
                }
                this._consolidateData(res);
                this.workerList.push(id);
                return done();
            });
        }, Object.keys(this.workers).length * this.maxConcurrent);
        this.q.pause();
    }

    _consolidateData(results) {
        if (!results) {
            return;
        }
        this.store.versions += results.versions;
        this.store.objects += results.objects;
        this.store.stalled += results.stalled;
        if (results.dataManaged
            && results.dataManaged.locations
            && results.dataManaged.total) {
            const { locations, total } = results.dataManaged;
            this.store.dataManaged.total.curr += total.curr;
            this.store.dataManaged.total.prev += total.prev;
            Object.keys(locations).forEach(site => {
                if (!this.store.dataManaged.byLocation[site]) {
                    this.store.dataManaged.byLocation[site] = { ...locations[site] };
                } else {
                    this.store.dataManaged.byLocation[site].curr
                        += locations[site].curr;
                    this.store.dataManaged.byLocation[site].prev
                        += locations[site].prev;
                }
            });
        }
        if (results.dataMetrics
            && results.dataMetrics.bucket
            && results.dataMetrics.location
            && results.dataMetrics.account) {
            Object.keys(results.dataMetrics).forEach(metricLevel => {
                // metricLevel can only be 'bucket', 'location' or 'account'
                if (validStorageMetricLevels.has(metricLevel)) {
                    Object.keys(results.dataMetrics[metricLevel]).forEach(resourceName => {
                        // resourceName can be the name of bucket, location or account
                        this.dataMetrics[metricLevel][resourceName] = consolidateDataMetrics(
                            this.dataMetrics[metricLevel][resourceName],
                            results.dataMetrics[metricLevel][resourceName],
                        );
                        // if metricLevel is account, add the locations details
                        if (metricLevel === 'account') {
                            Object.keys((results.dataMetrics[metricLevel][resourceName].locations || {})).forEach(locationName => {
                                if (!this.temporaryStore[resourceName]) {
                                    this.temporaryStore[resourceName] = {};
                                }
                                this.temporaryStore[resourceName][locationName] = consolidateDataMetrics(
                                    this.temporaryStore[resourceName][locationName],
                                    results.dataMetrics[metricLevel][resourceName].locations[locationName],
                                );
                            });
                        }
                    });
                }
            });
            // Add the accounts details for locations from the temporary store
            Object.keys(this.temporaryStore).forEach(accountName => {
                this.dataMetrics.account[accountName].locations = this.temporaryStore[accountName];
            });
        } else {
            this.dataMetrics = results.dataMetrics;
        }
    }

    setup(callback) {
        async.series([
            next => async.forEach(
                this.workers,
                (worker, done) => worker.init(done),
                next,
            ),
            next => async.forEach(
                this.workers,
                (worker, done) => worker.setup(done),
                next,
            ),
        ], callback);
    }

    teardown(callback) {
        async.forEach(
            this.workers,
            (worker, done) => worker.teardown(done),
            callback,
        );
    }

    stop(callback) {
        this.teardown(err => {
            Object.values(this.workers).forEach(worker => worker.kill());
            if (err) {
                this.log.error('unable to gracefully kill workers', {
                    error: err,
                    method: 'CountManager::stop',
                });
            }
            return callback(err);
        });
    }

    addWork(bucketList) {
        const { bucketCount, bucketInfos } = bucketList;
        const transformedInfos = bucketInfos.map(bucket => ({
            name: bucket.getName(),
            location: bucket.getLocationConstraint(),
            isVersioned: !!bucket.getVersioningConfiguration(),
            ownerCanonicalId: bucket.getOwner(),
            ingestion: bucket.isIngestionBucket(),
        }));
        this.store.buckets += bucketCount;
        this.store.bucketList = this.store.bucketList.concat(transformedInfos);
        this.q.push(bucketInfos);
        this.log.debug('added work', {
            workInQueue: this.q.length(),
            workInProgress: this.q.running(),
        });
    }

    start(callback) {
        if (!this.q.paused) {
            this.log.error('count task in progress', {
                method: 'CountManager::start',
            });
            process.nextTick(callback, new Error('countInProgress'));
            return;
        }
        const onceCB = once(callback);
        this.workerList = [];
        for (let i = 0; i < this.maxConcurrent; ++i) {
            Object.values(this.workers)
                .forEach(worker => this.workerList.push(worker.id));
        }
        this.q.error = err => {
            this.q.pause();
            this.q.kill();
            return process.nextTick(onceCB, err);
        };
        this.q.drain = () => {
            if (this.q.idle()) {
                this.q.pause();
                this.q.kill();
                process.nextTick(onceCB);
            }
        };
        this.q.resume();
    }
}

module.exports = CountManager;
