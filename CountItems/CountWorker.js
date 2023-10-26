const assert = require('assert');
const async = require('async');
const { BucketInfo } = require('arsenal').models;

class CountWorker {
    constructor(params) {
        this.log = params.log;
        assert(typeof params.sendFn === 'function');
        this._sendFn = params.sendFn;
        assert(typeof params.client === 'object');
        this.client = params.client;

        this.clientSetup = this.clientSetup.bind(this);
        this.countItems = this.countItems.bind(this);
        this.clientTeardown = this.clientTeardown.bind(this);
        this.handleMessage = this.handleMessage.bind(this);
    }

    clientSetup(callback) {
        if (this.client.client
            && this.client.client.isConnected()) {
            this.log.debug('mongoclient is connected...skipping setup');
            return callback();
        }
        return this.client.setup(callback);
    }

    countItems(bucketInfoObj, callback) {
        if (!this.client.client) {
            return callback(new Error('NotConnected'));
        }
        const bucketInfo = BucketInfo.fromObj(bucketInfoObj);
        const bucketName = bucketInfo.getName();
        this.log.info(`${process.pid} handling ${bucketName}`);
        return async.waterfall([
            next => this.client._getIsTransient(bucketInfo, this.log, next),
            (isTransient, next) => this.client.getObjectMDStats(bucketName, bucketInfo, isTransient, this.log, next),
        ], callback);
    }

    clientTeardown(callback) {
        if (!this.client.client) {
            return callback();
        }
        this.log.debug('mongoclient is connected...closing client');
        return this.client.close(callback);
    }

    handleMessage(data) {
        if (data.owner !== 'scality') {
            return;
        }
        switch (data.type) {
        case 'count':
            this.countItems(data.bucketInfo, (err, results) => {
                if (err) {
                    return this._sendFn({
                        id: data.id,
                        owner: 'scality',
                        type: 'count',
                        status: 'failed',
                        error: err.message,
                    });
                }
                return this._sendFn({
                    id: data.id,
                    owner: 'scality',
                    type: 'count',
                    status: 'passed',
                    results,
                });
            });
            break;
        case 'setup':
            this.clientSetup(err => {
                if (err) {
                    return this._sendFn({
                        id: data.id,
                        owner: 'scality',
                        type: 'setup',
                        status: 'failed',
                        error: err.message,
                    });
                }
                return this._sendFn({
                    id: data.id,
                    owner: 'scality',
                    type: 'setup',
                    status: 'passed',
                });
            });
            break;
        case 'teardown':
            this.clientTeardown(err => {
                if (err) {
                    return this._sendFn({
                        id: data.id,
                        owner: 'scality',
                        type: 'teardown',
                        status: 'failed',
                        error: err.message,
                    });
                }
                return this._sendFn({
                    id: data.id,
                    owner: 'scality',
                    type: 'teardown',
                    status: 'passed',
                });
            });
            break;
        default:
            break;
        }
    }
}

module.exports = CountWorker;
