const async = require('async');
const { reshapeExceptionError } = require('arsenal').errorUtils;
const { threshold, frequency } = require('../utils/constants');

class CountMaster {
    constructor(params) {
        this.log = params.log;
        this.manager = params.manager;
        this.client = params.client;
        this.metrics = params.metrics;
        CountMaster.waitingForPromScraping = false;
    }

    stop(signal, callback) {
        if (signal) {
            this.log.info(`received ${signal}; terminating workers`);
        }
        return this.manager.stop(err => {
            if (err) {
                this.log.error('unable to terminate all worker connections', {
                    error: reshapeExceptionError(err),
                    method: 'CountMaster::stop',
                });
            }
            this.client.close(callback);
        });
    }

    start(callback) {
        async.series([
            next => this.client.setup(err => {
                if (err) {
                    this.log.error('error connecting to mongodb', {
                        error: err,
                    });
                    return next(err);
                }
                return next();
            }),
            next => {
                this.metrics.start();
                this.log.info('metrics server started', {
                    port: 8003,
                });
                return next();
            },
            next => this.manager.setup(next),
            next => this.client.getBucketInfos(this.log, (err, bucketList) => {
                if (err) {
                    this.log.error('error getting bucket list', {
                        error: err,
                    });
                    return next(err);
                }
                this.log.info('got buckets infos', {
                    bucketCount: bucketList.bucketCount,
                });
                this.manager.addWork(bucketList);
                return next();
            }),
            next => this.manager.start(next),
            next => this.client.updateStorageConsumptionMetrics(this.manager.store, this.manager.dataMetrics, this.log, next),
        ], err => {
            if (err) {
                this.log.error('error occurred in count items job', {
                    error: reshapeExceptionError(err),
                    method: 'CountMaster::start',
                });
                return this.stop(null, () => callback(err));
            }
            CountMaster.waitingForPromScraping = true;
            return setTimeout(() => this.stop(null, () => callback()), threshold * frequency * 1000 * 4);
        });
    }
}

module.exports = CountMaster;
