const cluster = require('cluster');
const async = require('async');
const werelogs = require('werelogs');
const { BucketInfo, ObjectMD } = require('arsenal').models;
const S3UtilsMongoClient = require('../../utils/S3UtilsMongoClient');

const CountMaster = require('../../CountItems/CountMaster');
const CountManager = require('../../CountItems/CountManager');
const createMongoParams = require('../../utils/createMongoParams');
const createWorkers = require('../../CountItems/utils/createWorkers');
const { testBucketMD, testAccountCanonicalId, testBucketCreationDate } = require('../constants');

const logger = new werelogs.Logger('CountItems::Test::Functional');
const { MONGODB_REPLICASET } = process.env;
const dbName = 'countItemsTest';

const expectedCountItems = {
    objects: 90,
    versions: 60,
    buckets: 9,
    dataManaged: {
        total: { curr: 15000, prev: 12000 },
        byLocation: {
            'us-east-1': { curr: 9000, prev: 6000 },
            'secondary-location-1': { curr: 3000, prev: 3000 },
            'secondary-location-2': { curr: 3000, prev: 3000 },
        },
    },
    stalled: 0,
};
const expectedDataMetrics = {
    [`account_${testAccountCanonicalId}`]: {
        objectCount: { current: 90, deleteMarker: 0, nonCurrent: 60 },
        usedCapacity: { current: 9000, nonCurrent: 6000 },
        locations: {
            'secondary-location-1': {
                objectCount: { current: 30, deleteMarker: 0, nonCurrent: 30 },
                usedCapacity: { current: 3000, nonCurrent: 3000 },
            },
            'secondary-location-2': {
                objectCount: { current: 30, deleteMarker: 0, nonCurrent: 30 },
                usedCapacity: { current: 3000, nonCurrent: 3000 },
            },
            'us-east-1': {
                objectCount: { current: 90, deleteMarker: 0, nonCurrent: 60 },
                usedCapacity: { current: 9000, nonCurrent: 6000 },
            },
        },
    },
    [`bucket_test-bucket-0_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 0 },
        usedCapacity: { current: 1000, nonCurrent: 0 },
    },
    [`bucket_test-bucket-1_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 0 },
        usedCapacity: { current: 1000, nonCurrent: 0 },
    },
    [`bucket_test-bucket-2_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 0 },
        usedCapacity: { current: 1000, nonCurrent: 0 },
    },
    [`bucket_test-bucket-3_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
        usedCapacity: { current: 1000, nonCurrent: 1000 },
    },
    [`bucket_test-bucket-4_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
        usedCapacity: { current: 1000, nonCurrent: 1000 },
    },
    [`bucket_test-bucket-5_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
        usedCapacity: { current: 1000, nonCurrent: 1000 },
    },
    [`bucket_test-bucket-6_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
        usedCapacity: { current: 1000, nonCurrent: 1000 },
    },
    [`bucket_test-bucket-7_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
        usedCapacity: { current: 1000, nonCurrent: 1000 },
    },
    [`bucket_test-bucket-8_${testBucketCreationDate}`]: {
        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
        usedCapacity: { current: 1000, nonCurrent: 1000 },
    },
    'location_secondary-location-1': {
        objectCount: { current: 30, deleteMarker: 0, nonCurrent: 30 }, usedCapacity: { current: 3000, nonCurrent: 3000 },
    },
    'location_secondary-location-2': {
        objectCount: { current: 30, deleteMarker: 0, nonCurrent: 30 }, usedCapacity: { current: 3000, nonCurrent: 3000 },
    },
    'location_us-east-1': {
        objectCount: { current: 90, deleteMarker: 0, nonCurrent: 60 }, usedCapacity: { current: 9000, nonCurrent: 6000 },
    },
};

const expectedBucketList = Array.from(Array(9)).map((_, ind) => ({
    name: `test-bucket-${ind}`,
    location: 'us-east-1',
    isVersioned: ind >= 3,
    ownerCanonicalId: testBucketMD._owner,
    ingestion: false,
}));

const testBuckets = Array.from(Array(9)).map((_, n) => {
    const bucketName = `test-bucket-${n}`;
    const bucketMD = {
        ...testBucketMD,
        _name: bucketName,
    };
    if (n < 3) { // non versioned buckets
        return BucketInfo.fromObj(bucketMD);
    }
    if (n >= 3 && n < 6) { // versioned buckets
        return BucketInfo.fromObj({ ...bucketMD, _versioningConfiguration: { Status: 'Enabled' } });
    }
    return BucketInfo.fromObj({ ...bucketMD, _versioningConfiguration: { Status: 'Suspended' } }); // suspended buckets
});

function populateMongo(client, callback) {
    return async.eachSeries(testBuckets, (testBucket, cb) => async.series([
        next => client.createBucket(testBucket.getName(), testBucket, logger, next),
        next => async.timesSeries(10, (m, done) => {
            const objName = `test-object-${m}`;
            const objMD = new ObjectMD()
                .setKey(objName)
                .setDataStoreName('us-east-1')
                .setContentLength(100)
                .setLastModified('2020-01-01T00:00:00.000Z')
                .setOwnerId(testAccountCanonicalId);
            if (testBucket.getVersioningConfiguration()
                && testBucket.getVersioningConfiguration().Status === 'Enabled') {
                objMD.setReplicationInfo({
                    status: 'COMPLETED',
                    backends: [
                        {
                            status: 'COMPLETED',
                            site: 'secondary-location-1',
                        },
                        {
                            status: 'COMPLETED',
                            site: 'secondary-location-2',
                        },
                    ],
                });
                return async.timesSeries(2, (z, done2) => client.putObject(
                    testBucket.getName(),
                    objName,
                    objMD.getValue(),
                    {
                        versionId: null,
                        versioning: true,
                    },
                    logger,
                    done2,
                ), done);
            }
            if (testBucket.getVersioningConfiguration()
                && testBucket.getVersioningConfiguration().Status === 'Suspended') {
                return async.timesSeries(2, (z, done2) => client.putObject(
                    testBucket.getName(),
                    objName,
                    z === 0 ? objMD.getValue() : { ...objMD.getValue(), isNull: true },
                    z === 0 ? { versionId: null, versioning: true } : { versionId: null },
                    logger,
                    done2,
                ), done);
            }
            return client.putObject(testBucket.getName(), objName, objMD.getValue(), null, logger, done);
        }, next),
    ], cb), callback);
}

jest.setTimeout(120000);
describe('CountItems', () => {
    const oldEnv = process.env;
    let client;

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
            next => populateMongo(client, next),
        ], done);
    });

    afterAll(done => {
        async.series([
            next => client.db.dropDatabase(next),
            next => client.close(next),
        ], done);
    });


    test.each([1, 4])(
        'should successfully countItems with %i worker(s)',
        (cnt, done) => {
            cluster.setupMaster({
                exec: require.resolve('../../countItems.js'),
            });
            const countMaster = new CountMaster({
                log: logger,
                manager: new CountManager({
                    log: new werelogs.Logger('S3Utils::CountItems::Manager'),
                    workers: createWorkers(cnt),
                    maxConcurrent: 5,
                }),
                client: new S3UtilsMongoClient(createMongoParams(logger)),
            });

            async.series([
                next => countMaster.start(err => {
                    expect(err).toBeFalsy();
                    return next();
                }),
                next => client.readCountItems(logger, (err, res) => {
                    expect(err).toBeFalsy();
                    expect(res).toMatchObject(expectedCountItems);
                    expect(res.bucketList)
                        .toEqual(expect.arrayContaining(expectedBucketList));
                    return next();
                }),
                next => async.eachSeries(
                    Object.keys(expectedDataMetrics),
                    (entity, cb) => client.readStorageConsumptionMetrics(entity, logger, (err, res) => {
                        expect(err).toBeFalsy();
                        expect(res.usedCapacity).toMatchObject(expectedDataMetrics[entity].usedCapacity);
                        expect(res.objectCount).toMatchObject(expectedDataMetrics[entity].objectCount);
                        expect(res.measuredOn).not.toBeNull();
                        return cb();
                    }),
                    () => next(),
                ),
            ], () => countMaster.stop(null, done));
        },
    );
});
