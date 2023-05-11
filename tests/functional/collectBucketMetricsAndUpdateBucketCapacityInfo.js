const async = require('async');
const werelogs = require('werelogs');
const assert = require('assert');
const { constants } = require('arsenal');
const S3UtilsMongoClient = require('../../utils/S3UtilsMongoClient');
const { testBucketMD, testBucketCreationDate, testUserBucketInfo } = require('../constants');
const { collectBucketMetricsAndUpdateBucketCapacityInfo } = require('../../DataReport/collectBucketMetricsAndUpdateBucketCapacityInfo');

const logger = new werelogs.Logger('collectBucketMetricsAndUpdateBucketCapacityInfo::Test::Functional');
const { MONGODB_REPLICASET } = process.env;
const dbName = 'collectBucketMetricsAndUpdateBucketCapacityInfoTest';
const USERSBUCKET = '__usersbucket';


describe('collectBucketMetricsAndUpdateBucketCapacityInfo', () => {
    const oldEnv = process.env;
    let client;
    let testBucketCapacities;

    const testBucketName = 'test-bucket';

    beforeAll(done => {
        process.env = oldEnv;
        process.env.MONGODB_DATABASE = dbName;

        const opts = {
            replicaSetHosts: MONGODB_REPLICASET,
            writeConcern: 'majority',
            replicaSet: 'rs0',
            readPreference: 'primary',
            database: dbName,
            replicationGroupId: 'RG001',
            logger,
        };
        client = new S3UtilsMongoClient(opts);
        async.series([
            next => client.setup(next),
        ], done);
    });

    afterAll(done => {
        async.series([
            next => client.db.dropDatabase(next),
            next => client.close(next),
        ], done);
    });

    beforeEach(done => {
        testBucketCapacities = {
            VeeamSOSApi: {
                SystemInfo: {
                    ProtocolCapabilities: {
                        CapacityInfo: true,
                    },
                },
                CapacityInfo: {
                    Capacity: 0,
                    Available: 0,
                    Used: 0,
                },
            },
        };
        client.updateStorageConsumptionMetrics({}, {
            bucket: {
                [`${testBucketName}_${testBucketCreationDate}`]: {
                    usedCapacity: { current: 10, nonCurrent: 10 },
                    objectCount: { current: 10, nonCurrent: 10 },
                },
            },
        }, logger, done);
    });

    afterEach(done => client.deleteBucket(testBucketName, logger, done));

    test('should not update bucket CapacityInfo if bucket doesn\'t have _capabilities attribute', done => {
        testBucketCapacities.VeeamSOSApi.SystemInfo.ProtocolCapabilities.CapacityInfo = false;
        return async.series([
            next => client.createBucket(testBucketName, {
                ...testBucketMD,
            }, logger, next),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${testBucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ), // put bucket entry in __usersbucket
            next => collectBucketMetricsAndUpdateBucketCapacityInfo(client, logger, err => {
                assert.equal(err, null);
                next();
            }),
            next => client.getBucketAttributes(testBucketName, logger, (err, bucketInfo) => {
                assert.equal(err, null);
                assert.equal(bucketInfo.getCapabilities(), null);
                next();
            }),
        ], done);
    });

    test('should not update bucket CapacityInfo if bucket is SOSAPI Capacity disabled', done => {
        testBucketCapacities.VeeamSOSApi.SystemInfo.ProtocolCapabilities.CapacityInfo = false;
        return async.series([
            next => client.createBucket(testBucketName, {
                ...testBucketMD,
                _capabilities: testBucketCapacities,
            }, logger, next),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${testBucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ), // put bucket entry in __usersbucket
            next => collectBucketMetricsAndUpdateBucketCapacityInfo(client, logger, err => {
                assert.equal(err, null);
                next();
            }),
            next => client.getBucketAttributes(testBucketName, logger, (err, bucketInfo) => {
                assert.equal(err, null);
                const { Capacity, Available, Used } = bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo;
                assert.strictEqual(Capacity, 0);
                assert.strictEqual(Available, 0);
                assert.strictEqual(Used, 0);
                next();
            }),
        ], done);
    });

    test('should successfully collect bucketMetrics and update bucket CapacityInfo', done => {
        testBucketCapacities.VeeamSOSApi.CapacityInfo.Capacity = 30;

        return async.series([
            next => client.createBucket(testBucketName, {
                ...testBucketMD,
                _capabilities: testBucketCapacities,
            }, logger, next),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${testBucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ), // put bucket entry in __usersbucket
            next => collectBucketMetricsAndUpdateBucketCapacityInfo(client, logger, err => {
                assert.equal(err, null);
                next();
            }),
            next => client.getBucketAttributes(testBucketName, logger, (err, bucketInfo) => {
                assert.equal(err, null);
                const { Capacity, Available, Used } = bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo;
                assert.strictEqual(Capacity, 30);
                assert.strictEqual(Available, 10);
                assert.strictEqual(Used, 20);
                next();
            }),
        ], done);
    });

    test('should update bucket Capacity and Available -1 if Capacity value is not valid', done => {
        testBucketCapacities.VeeamSOSApi.CapacityInfo.Capacity = 'not-a-number';

        return async.series([
            next => client.createBucket(testBucketName, {
                ...testBucketMD,
                _capabilities: testBucketCapacities,
            }, logger, next),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${testBucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ), // put bucket entry in __usersbucket
            next => collectBucketMetricsAndUpdateBucketCapacityInfo(client, logger, err => {
                assert.equal(err, null);
                next();
            }),
            next => client.getBucketAttributes(testBucketName, logger, (err, bucketInfo) => {
                assert.equal(err, null);
                const { Capacity, Available, Used } = bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo;
                assert.strictEqual(Capacity, -1);
                assert.strictEqual(Available, -1);
                assert.strictEqual(Used, 20);
                next();
            }),
        ], done);
    });

    test('should update bucket Available -1 if Capacity value is smaller than Used', done => {
        testBucketCapacities.VeeamSOSApi.CapacityInfo.Capacity = 10;

        return async.series([
            next => client.createBucket(testBucketName, {
                ...testBucketMD,
                _capabilities: testBucketCapacities,
            }, logger, next),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${testBucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ), // put bucket entry in __usersbucket
            next => collectBucketMetricsAndUpdateBucketCapacityInfo(client, logger, err => {
                assert.equal(err, null);
                next();
            }),
            next => client.getBucketAttributes(testBucketName, logger, (err, bucketInfo) => {
                assert.equal(err, null);
                const { Capacity, Available, Used } = bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo;
                assert.strictEqual(Capacity, 10);
                assert.strictEqual(Available, -1);
                assert.strictEqual(Used, 20);
                next();
            }),
        ], done);
    });

    test('should update bucket Used and Available -1 if bucket metrics are not retrievable', done => {
        testBucketCapacities.VeeamSOSApi.CapacityInfo.Capacity = 30;

        return async.series([
            next => client.updateStorageConsumptionMetrics({}, { bucket: {} }, logger, next),
            next => client.createBucket(testBucketName, {
                ...testBucketMD,
                _capabilities: testBucketCapacities,
            }, logger, next),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${testBucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ), // put bucket entry in __usersbucket
            next => collectBucketMetricsAndUpdateBucketCapacityInfo(client, logger, err => {
                assert.equal(err, null);
                next();
            }),
            next => client.getBucketAttributes(testBucketName, logger, (err, bucketInfo) => {
                assert.equal(err, null);
                const { Capacity, Available, Used } = bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo;
                assert.strictEqual(Capacity, 30);
                assert.strictEqual(Available, -1);
                assert.strictEqual(Used, -1);
                next();
            }),
        ], done);
    });
});
