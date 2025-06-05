const assert = require('assert');
const async = require('async');
const { BucketInfo } = require('arsenal').models;
const monitoring = require('../utils/monitoring');
const { deserializeBigInts, serializeBigInts } = require('./utils/utils');

const PENSIEVE = 'PENSIEVE';
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

    getIsTransient(bucketInfo, cb) {
        const locConstraint = bucketInfo.getLocationConstraint();

        if (this.client.isLocationTransient) {
            this.client.isLocationTransient(locConstraint, this.log, cb);
            return;
        }
        this.pensieveLocationIsTransient(locConstraint, cb);
    }

    pensieveLocationIsTransient(locConstraint, cb) {
        const overlayVersionId = 'configuration/overlay-version';

        async.waterfall([
            next => this.client.getObject(PENSIEVE, overlayVersionId, null, this.log, next),
            (version, next) => {
                const overlayConfigId = `configuration/overlay/${version}`;
                return this.client.getObject(PENSIEVE, overlayConfigId, null, this.log, next);
            },
        ], (err, res) => {
            if (err) {
                this.log.error('error getting configuration overlay', {
                    method: 'pensieveLocationIsTransient',
                    error: err,
                });
                return cb(err);
            }
            const isTransient =
                Boolean(res?.locations[locConstraint]?.isTransient);

            return cb(null, isTransient);
        });
    }

    countItems(bucketInfoObj, callback) {
        if (!this.client.client) {
            return callback(new Error('NotConnected'));
        }
        // 'fromObj' expects that the website configuration is an instance of
        // WebsiteConfiguration as it is not used in CountItems, we nullify it.
        if (bucketInfoObj._websiteConfiguration) {
            Object.assign(bucketInfoObj, { _websiteConfiguration: null });
        }
        const bucketInfo = BucketInfo.fromObj(bucketInfoObj.bucketInfo || bucketInfoObj);
        const bucketName = bucketInfo.getName();
        this.log.info(`${process.pid} handling ${bucketName}`);
        return async.waterfall([
            next => this.getIsTransient(bucketInfo, next),
            (isTransient, next) => this.client.getObjectMDStats(bucketName, bucketInfo, isTransient, this.log, next),
        ], (err, results) => {
            monitoring.bucketsCount.inc({ status: err ? 'error' : 'success' });
            callback(err, results);
        });
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
            this.countItems(deserializeBigInts(data.bucketInfo), (err, results) => {
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
                    results: serializeBigInts(results),
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
