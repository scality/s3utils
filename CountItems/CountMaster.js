const async = require('async');
const { reshapeExceptionError } = require('arsenal').errorUtils;

class CountMaster {
    constructor(params) {
        this.log = params.log;
        this.manager = params.manager;
        this.client = params.client;
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
            next => this.manager.setup(next),
            next => this.client.getBucketInfos(this.log, (err, bucketList) => {
                if (err) {
                    this.log.error('error getting bucket list', {
                        error: err,
                    });
                    return next(err);
                }
                this.manager.addWork(bucketList);
                return next();
            }),
            next => this.manager.start(next),
            next => this.client.updateCountItems(this.manager.store, this._log, next),
        ], err => {
            if (err) {
                this.log.error('error occurred in count items job', {
                    error: reshapeExceptionError(err),
                    method: 'CountMaster::start',
                });
                return this.stop(null, () => callback(err));
            }
            this.log.info('finished count items job and updated metrics in db', {
                countItems: this.manager.store,
                method: 'CountMaster::start',
            });
            return this.stop(null, () => callback());
        });
    }
}

module.exports = CountMaster;
