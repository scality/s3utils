const util = require('util');
const async = require('async');
const assert = require('assert');
const werelogs = require('werelogs');
const { BucketInfo } = require('arsenal').models;
const { versioning, constants } = require('arsenal');

const { MongoMemoryReplSet } = require('mongodb-memory-server');
const { ObjectMDArchive } = require('arsenal/build/lib/models');
const S3UtilsMongoClient = require('../../../utils/S3UtilsMongoClient');
const { mongoMemoryServerParams, createMongoParamsFromMongoMemoryRepl } = require('../../utils/mongoUtils');
const {
    testBucketMD, testAccountCanonicalId, testUserBucketInfo, testBucketCreationDate,
} = require('../../constants');

const logger = new werelogs.Logger('S3UtilsMongoClient', 'debug', 'debug');

const { BucketVersioningKeyFormat } = versioning.VersioningConstants;


const variations = [
    { it: '(v0)', vFormat: BucketVersioningKeyFormat.v0 },
    { it: '(v1)', vFormat: BucketVersioningKeyFormat.v1 },
];

const bucketMD = BucketInfo.fromObj(testBucketMD);
const BUCKET_NAME = bucketMD.getName();
const USERSBUCKET = '__usersbucket';
const BUCKET_CREATE_DATE = new Date(testBucketCreationDate).getTime();


describe('S3UtilsMongoClient::getObjectMDStats', () => {
    let client;
    let repl;

    beforeAll(async () => {
        repl = await MongoMemoryReplSet.create(mongoMemoryServerParams);
        client = new S3UtilsMongoClient({
            ...createMongoParamsFromMongoMemoryRepl(repl),
            logger,
        });
        await util.promisify(client.setup.bind(client))();
    });


    afterAll(done => async.series([
        next => client.close(next),
        next => repl.stop()
            .then(() => next())
            .catch(next),
    ], done));

    const versionedBucketMD = {
        ...bucketMD,
        getOwner: () => (testAccountCanonicalId),
        _versioningConfiguration: {
            Status: 'Enabled',
        },
    };
    const suspendedBucketMD = {
        ...bucketMD,
        _versioningConfiguration: {
            Status: 'Suspended',
        },
    };

    describe('Should get correct results for versioning disabled bucket', () => {
        const versionParams = {
            versioning: false,
            versionId: null,
        };
        const object1Params = {
            'key': 'non-versioned-test-object1',
            'content-length': 10,
            'dataStoreName': 'us-east-1',
            'owner-id': testAccountCanonicalId,
            'replicationInfo': {
                backends: [],
            },
        };
        const object2Params = {
            ...object1Params,
            key: 'non-versioned-test-object2',
        };
        variations.forEach(variation => {
            describe(variation.it, () => {
                beforeEach(done => {
                    async.series([
                        next => {
                            client.defaultBucketKeyFormat = variation.vFormat;
                            return next();
                        },
                        next => client.createBucket(BUCKET_NAME, bucketMD, logger, next),
                        next => {
                            client.putObject(
                                USERSBUCKET,
                                `${bucketMD.getOwner()}${constants.splitter}${BUCKET_NAME}`,
                                testUserBucketInfo.value,
                                {
                                    versioning: false,
                                    versionId: null,
                                },
                                logger,
                                next,
                            );
                        }, // put bucket entry in __usersbuckets
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object1
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object1 again
                        next => client.putObject(
                            BUCKET_NAME,
                            object2Params.key,
                            object2Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object2
                    ], done);
                });

                afterEach(done => client.deleteBucket(BUCKET_NAME, logger, done));

                it(`Should get correct results ${variation.it}`, done => {
                    const expected = {
                        dataManaged: {
                            locations: { 'us-east-1': { curr: 20, prev: 0 } },
                            total: { curr: 20, prev: 0 },
                        },
                        objects: 2,
                        stalled: 0,
                        versions: 0,
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            location: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    };
                    return client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(BUCKET_NAME, bucketInfo, false, logger, (err, data) => {
                            assert.deepStrictEqual(err, null);
                            assert.deepStrictEqual(data, expected);
                            return done();
                        });
                    });
                });
            });
        });
    });

    describe('Should get correct results for versioning enabled bucket', () => {
        const versionParams = {
            versioning: true,
            versionId: null,
        };
        const object1Params = {
            'key': 'versioned-test-object1',
            'content-length': 10,
            'dataStoreName': 'us-east-1',
            'owner-id': testAccountCanonicalId,
            'replicationInfo': {
                backends: [],
            },
        };
        const object2Params = {
            ...object1Params,
            key: 'versioned-test-object2',
        };
        variations.forEach(variation => {
            const itOnlyInV1 = variation.vFormat === 'v1' ? it : it.skip;
            describe(variation.it, () => {
                beforeEach(done => {
                    async.series([
                        next => {
                            client.defaultBucketKeyFormat = variation.vFormat;
                            return next();
                        },
                        next => client.createBucket(BUCKET_NAME, versionedBucketMD, logger, next),
                        next => client.putObject(
                            USERSBUCKET,
                            `${versionedBucketMD.getOwner()}${constants.splitter}${BUCKET_NAME}`,
                            testUserBucketInfo.value,
                            {
                                versioning: false,
                                versionId: null,
                            },
                            logger,
                            next,
                        ),
                    ], done);
                });

                afterEach(done => client.deleteBucket(BUCKET_NAME, logger, done));

                it(`Should get correct results ${variation.it}`, done => {
                    const expected = {
                        versions: 1,
                        objects: 2,
                        dataManaged: {
                            total: { curr: 20, prev: 10 },
                            locations: { 'us-east-1': { curr: 20, prev: 10 } },
                        },
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 1n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 10n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 1n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 10n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 1n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 10n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 1n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 10n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            location: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 1n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 10n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                        stalled: 0,
                    };
                    return async.series([
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object1
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object1 again
                        next => client.putObject(
                            BUCKET_NAME,
                            object2Params.key,
                            object2Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object2
                    ], () => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(
                            BUCKET_NAME,
                            bucketInfo,
                            false,
                            logger,
                            (err, data) => {
                                assert.deepStrictEqual(err, null);
                                // Compare each section separately to avoid circular reference issues
                                assert.strictEqual(data.versions.toString(), expected.versions.toString());
                                assert.strictEqual(data.objects.toString(), expected.objects.toString());
                                assert.strictEqual(data.stalled, expected.stalled);

                                // Compare dataManaged section
                                assert.deepStrictEqual(
                                    JSON.parse(JSON.stringify(data.dataManaged)),
                                    JSON.parse(JSON.stringify(expected.dataManaged)),
                                );

                                // Compare dataMetrics sections separately
                                const sections = ['account', 'bucket', 'location'];
                                sections.forEach(section => {
                                    assert.deepStrictEqual(
                                        data.dataMetrics[section],
                                        expected.dataMetrics[section],
                                    );
                                });
                                return done();
                            },
                        );
                    }));
                });

                itOnlyInV1(`Should get correct results with deleteMarker ${variation.it}`, done => {
                    const expected = {
                        dataManaged: {
                            locations: { 'us-east-1': { curr: 0, prev: 20 } },
                            total: { curr: 0, prev: 20 },
                        },
                        objects: 0,
                        stalled: 0,
                        versions: 2,
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 20n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 0n,
                                                deleteMarker: 1n,
                                                nonCurrent: 2n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 0n,
                                                nonCurrent: 20n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 20n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 0n,
                                                deleteMarker: 1n,
                                                nonCurrent: 2n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 0n,
                                                nonCurrent: 20n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            location: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 20n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    };
                    return async.series([
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object1
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            versionParams,
                            logger,
                            next,
                        ), // put object1 again
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            {
                                ...object1Params,
                                'isDeleteMarker': true,
                                'content-length': 0,
                            },
                            versionParams,
                            logger,
                            next,
                        ), // delete object1
                    ], () => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(
                            BUCKET_NAME,
                            bucketInfo,
                            false,
                            logger,
                            (err, data) => {
                                assert.deepStrictEqual(err, null);
                                assert.deepStrictEqual(data, expected);
                                return done();
                            },
                        );
                    }));
                });

                it('should get correct results with lifecycle replication enabled '
                    + `and location transient is true ${variation.it}`, done => {
                    const expected = {
                        dataManaged: {
                            locations: {
                                'us-east-1': { curr: 10, prev: 0 },
                                'completed': { curr: 10, prev: 0 },
                            },
                            total: { curr: 20, prev: 0 },
                        },
                        objects: 2,
                        stalled: 0,
                        versions: 0,
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 1n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 10n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                        'completed': {
                                            objectCount: {
                                                current: 1n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 10n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 1n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 10n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                        'completed': {
                                            objectCount: {
                                                current: 1n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 10n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            location: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 1n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 10n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'completed': {
                                    objectCount: {
                                        current: 1n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 10n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    };
                    return async.series([
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            {
                                ...object1Params,
                                replicationInfo: {
                                    status: 'PENDING',
                                    backends: [
                                        {
                                            status: 'PENDING',
                                            site: 'not-completed',
                                        },
                                        {
                                            status: 'COMPLETED',
                                            site: 'completed',
                                        },
                                    ],
                                },
                            },
                            versionParams,
                            logger,
                            next,
                        ), // object1 with one site pending and one site complete
                        next => client.putObject(
                            BUCKET_NAME,
                            object2Params.key,
                            {
                                ...object2Params,
                                replicationInfo: {
                                    status: 'COMPLETED',
                                    backends: [
                                        {
                                            status: 'COMPLETE',
                                            site: 'completed',
                                        },
                                    ],
                                },
                            },
                            versionParams,
                            logger,
                            next,
                        ), // object2 with one site complete
                    ], () => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(
                            BUCKET_NAME,
                            bucketInfo,
                            true,
                            logger,
                            (err, data) => {
                                assert.deepStrictEqual(err, null);
                                assert.deepStrictEqual(data, expected);
                                return done();
                            },
                        );
                    }));
                });

                it('should get correct results with lifecycle replication enabled'
                    + `and location transient is false ${variation.it}`, done => {
                    const expected = {
                        dataManaged: {
                            locations: {
                                'us-east-1': { curr: 20, prev: 0 },
                                'completed': { curr: 10, prev: 0 },
                            },
                            total: { curr: 30, prev: 0 },
                        },
                        objects: 2,
                        stalled: 0,
                        versions: 0,
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                        'completed': {
                                            objectCount: {
                                                current: 1n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 10n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                        'completed': {
                                            objectCount: {
                                                current: 1n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 10n,
                                                nonCurrent: 0n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            location: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'completed': {
                                    objectCount: {
                                        current: 1n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 10n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    };
                    return async.series([
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            {
                                ...object1Params,
                                replicationInfo: {
                                    status: 'PENDING',
                                    backends: [
                                        {
                                            status: 'PENDING',
                                            site: 'not-completed',
                                        },
                                        {
                                            status: 'COMPLETED',
                                            site: 'completed',
                                        },
                                    ],
                                },
                            },
                            versionParams,
                            logger,
                            next,
                        ), // object1 with one site pending and one site complete
                        next => client.putObject(
                            BUCKET_NAME,
                            object2Params.key,
                            {
                                ...object2Params,
                                replicationInfo: {
                                    status: 'COMPLETED',
                                    backends: [
                                        {
                                            status: 'COMPLETE',
                                            site: 'completed',
                                        },
                                    ],
                                },
                            },
                            versionParams,
                            logger,
                            next,
                        ), // object2 with one site complete
                    ], () => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(
                            BUCKET_NAME,
                            bucketInfo,
                            false,
                            logger,
                            (err, data) => {
                                assert.deepStrictEqual(err, null);
                                assert.deepStrictEqual(data, expected);
                                return done();
                            },
                        );
                    }));
                });
            });
        });
    });

    describe('Should get correct results for versioning suspended bucket', () => {
        const object1Params = {
            'key': 'test-object1',
            'content-length': 10,
            'dataStoreName': 'us-east-1',
            'owner-id': testAccountCanonicalId,
            'replicationInfo': {
                backends: [],
            },
        };
        const object2Params = {
            ...object1Params,
            key: 'test-object2',
        };
        variations.forEach(variation => {
            describe(variation.it, () => {
                beforeEach(done => {
                    async.series([
                        next => {
                            client.defaultBucketKeyFormat = variation.vFormat;
                            return next();
                        },
                        next => client.createBucket(BUCKET_NAME, suspendedBucketMD, logger, next),
                    ], done);
                });

                afterEach(done => client.deleteBucket(BUCKET_NAME, logger, done));

                it(`Should get correct results ${variation.it}`, done => {
                    const expected = {
                        dataManaged: {
                            locations: { 'us-east-1': { curr: 20, prev: 10 } },
                            total: { curr: 20, prev: 10 },
                        },
                        objects: 2,
                        stalled: 0,
                        versions: 1,
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 1n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 10n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 1n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 10n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 1n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 10n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                    locations: {
                                        'us-east-1': {
                                            objectCount: {
                                                current: 2n,
                                                deleteMarker: 0n,
                                                nonCurrent: 1n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 20n,
                                                nonCurrent: 10n,
                                                _currentCold: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 0n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                },
                            },
                            location: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 1n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 20n,
                                        nonCurrent: 10n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    };
                    return async.series([
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            object1Params,
                            {
                                versionId: null,
                                versioning: true,
                            },
                            logger,
                            next,
                        ), // versioned object1 put before suspend
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Params.key,
                            {
                                ...object1Params,
                                isNull: true,
                            },
                            {
                                versionId: null,

                            },
                            logger,
                            next,
                        ), // null versioned object1
                        next => client.putObject(
                            BUCKET_NAME,
                            object2Params.key,
                            {
                                ...object2Params,
                                isNull: true,
                            },
                            {
                                versionId: null,
                            },
                            logger,
                            next,
                        ), // null versioned object2
                    ], () => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(
                            BUCKET_NAME,
                            bucketInfo,
                            false,
                            logger,
                            (err, data) => {
                                assert.deepStrictEqual(err, null);
                                assert.deepStrictEqual(data, expected);
                                return done();
                            },
                        );
                    }));
                });
            });
        });
    });

    // write a test suite for the cold objects
    describe('Should get correct results for cold objects', () => {
        const versionParams = {
            versioning: true,
            versionId: null,
        };
        const object1InCold = {
            'key': 'test-object1',
            'content-length': 10,
            'dataStoreName': 'cold-location',
            'owner-id': testAccountCanonicalId,
            'replicationInfo': {
                backends: [],
            },
            'archive': new ObjectMDArchive({}),
        };
        const object1Restoring = {
            'key': 'test-object1',
            'content-length': 10,
            'dataStoreName': 'cold-location',
            'owner-id': testAccountCanonicalId,
            'replicationInfo': {
                backends: [],
            },
            'archive': new ObjectMDArchive({}, new Date(Date.now() - 5000), 10),
        };
        const object1Restored = {
            'key': 'test-object1',
            'content-length': 10,
            'dataStoreName': 'us-east-1',
            'owner-id': testAccountCanonicalId,
            'replicationInfo': {
                backends: [],
            },
            'archive': new ObjectMDArchive(
                {},
                new Date(Date.now() - 5000),
                10,
                new Date(Date.now() - 1000),
                new Date(Date.now() + 10000),
            ),
        };
        variations.forEach(variation => {
            describe(variation.it, () => {
                beforeEach(done => {
                    async.series([
                        next => {
                            client.defaultBucketKeyFormat = variation.vFormat;
                            return next();
                        },
                        next => client.createBucket(BUCKET_NAME, versionedBucketMD, logger, next),
                    ], done);
                });

                afterEach(done => client.deleteBucket(BUCKET_NAME, logger, done));

                it(`Should get correct results ${variation.it}`, done => {
                    const expected = {
                        dataManaged: {
                            locations: {
                                'cold-location': {
                                    curr: 0,
                                    prev: 20,
                                },
                                'us-east-1': {
                                    curr: 10,
                                    prev: 10,
                                },
                            },
                            total: {
                                curr: 10,
                                prev: 30,
                            },
                        },
                        dataMetrics: {
                            account: {
                                [testAccountCanonicalId]: {
                                    locations: {
                                        'cold-location': {
                                            objectCount: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 1n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 1n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 10n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 10n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                        'us-east-1': {
                                            objectCount: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 1n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 1n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 10n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 10n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 1n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 1n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 10n,
                                        _currentRestored: 10n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 10n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                            bucket: {
                                [`${BUCKET_NAME}_${BUCKET_CREATE_DATE}`]: {
                                    locations: {
                                        'cold-location': {
                                            objectCount: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 1n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 1n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 10n,
                                                _currentRestored: 0n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 10n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                        'us-east-1': {
                                            objectCount: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                deleteMarker: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 1n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 1n,
                                                _incompleteMPUUploads: 0n,
                                            },
                                            usedCapacity: {
                                                current: 0n,
                                                _currentCold: 0n,
                                                nonCurrent: 0n,
                                                _nonCurrentCold: 0n,
                                                _currentRestored: 10n,
                                                _currentRestoring: 0n,
                                                _nonCurrentRestored: 0n,
                                                _nonCurrentRestoring: 10n,
                                                _incompleteMPUParts: 0n,
                                            },
                                        },
                                    },
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 1n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 1n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 10n,
                                        _currentRestored: 10n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 10n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                            location: {
                                'cold-location': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 1n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 1n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 10n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 10n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 1n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 10n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 10n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                        objects: 1,
                        stalled: 0,
                        versions: 2,
                    };
                    return async.series([
                        next => client.putObject(
                            BUCKET_NAME,
                            object1InCold.key,
                            object1InCold,
                            versionParams,
                            logger,
                            next,
                        ),
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Restoring.key,
                            object1Restoring,
                            versionParams,
                            logger,
                            next,
                        ),
                        next => client.putObject(
                            BUCKET_NAME,
                            object1Restored.key,
                            object1Restored,
                            versionParams,
                            logger,
                            next,
                        ), // put object2
                    ], () => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                        assert.deepStrictEqual(err, null);
                        return client.getObjectMDStats(
                            BUCKET_NAME,
                            bucketInfo,
                            false,
                            logger,
                            (err, data) => {
                                assert.deepStrictEqual(err, null);
                                assert.deepStrictEqual(data, expected);
                                return done();
                            },
                        );
                    }));
                });
            });
        });
    });
});

describe('S3UtilsMongoClient::updateBucketCapacityInfo', () => {
    let client;
    let repl;

    beforeAll(async () => {
        repl = await MongoMemoryReplSet.create(mongoMemoryServerParams);
        client = new S3UtilsMongoClient({
            ...createMongoParamsFromMongoMemoryRepl(repl),
            logger,
        });
        await util.promisify(client.setup.bind(client))();
    });


    afterAll(done => async.series([
        next => client.close(next),
        next => repl.stop()
            .then(() => next())
            .catch(next),
    ], done));

    describe('Should update correctly CapacityInfo attributes of a bucket', () => {
        variations.forEach(variation => {
            describe(variation.it, () => {
                beforeEach(done => {
                    client.defaultBucketKeyFormat = variation.vFormat;
                    done();
                });

                afterEach(done => client.deleteBucket(BUCKET_NAME, logger, done));

                it(`Should correctly update CapacityInfo attributes ${variation.it}`, done => {
                    const startTime = new Date();
                    const capacityInfo = {
                        Capacity: 30n,
                        Available: 10n,
                        Used: 10n,
                    };
                    async.series([
                        next => client.createBucket(BUCKET_NAME, bucketMD, logger, next),
                        next => client.updateBucketCapacityInfo(BUCKET_NAME, capacityInfo, logger, err => {
                            assert.equal(err, null);
                            next();
                        }),
                        next => client.getBucketAttributes(BUCKET_NAME, logger, (err, bucketInfo) => {
                            assert.equal(err, null);
                            const {
                                Capacity, Available, Used, LastModified,
                            } = bucketInfo.getCapabilities().VeeamSOSApi.CapacityInfo;
                            assert.strictEqual(Capacity, 30n);
                            assert.strictEqual(Available, 10n);
                            assert.strictEqual(Used, 10n);
                            assert(new Date(LastModified) > startTime);
                            assert(new Date(LastModified) < new Date());
                            next();
                        }),
                    ], done);
                });
            });
        });
    });
});

describe('S3UtilsMongoClient::getBucketInfos', () => {
    let client;
    let repl;

    beforeAll(async () => {
        repl = await MongoMemoryReplSet.create(mongoMemoryServerParams);
        client = new S3UtilsMongoClient({
            ...createMongoParamsFromMongoMemoryRepl(repl),
            logger,
        });
        await util.promisify(client.setup.bind(client))();
    });

    afterAll(done => async.series([
        next => client.close(next),
        next => repl.stop()
            .then(() => next())
            .catch(next),
    ], done));

    describe('Should get correct bucket infos', () => {
        const buckets = [
            {
                ...bucketMD,
                _name: 'getbucketinfos-bucket1',
            },
            {
                ...bucketMD,
                _name: 'getbucketinfos-bucket2',
                _versioningConfiguration: {
                    Status: 'Enabled',
                },
            },
            {
                ...bucketMD,
                _name: 'getbucketinfos-bucket3',
                _versioningConfiguration: {
                    Status: 'Suspended',
                },
            }];
        beforeEach(done => async.series([
            next => client.createBucket(buckets[0]._name, buckets[0], logger, next),
            next => client.createBucket(buckets[1]._name, buckets[1], logger, next),
            next => client.createBucket(buckets[2]._name, buckets[2], logger, next),
        ], done));

        afterEach(done => async.series([
            next => client.deleteBucket(buckets[0]._name, logger, next),
            next => client.deleteBucket(buckets[1]._name, logger, next),
            next => client.deleteBucket(buckets[2]._name, logger, next),
        ], done));

        it('Should get correct bucket infos', done => client.getBucketInfos(logger, (err, data) => {
            assert.equal(err, null);
            assert.strictEqual(data.bucketCount, 3);
            assert.strictEqual(
                data.bucketInfos.find(bucket => bucket._name === buckets[0]._name).serialize(),
                BucketInfo.fromObj(buckets[0]).serialize(),
            );
            assert.strictEqual(
                data.bucketInfos.find(bucket => bucket._name === buckets[1]._name).serialize(),
                BucketInfo.fromObj(buckets[1]).serialize(),
            );
            assert.strictEqual(
                data.bucketInfos.find(bucket => bucket._name === buckets[2]._name).serialize(),
                BucketInfo.fromObj(buckets[2]).serialize(),
            );
            done();
        }));

        it('Should ignore buckets in collection list but not in metastore', async () => {
            const metastoreCollection = client.getCollection('__metastore');

            // Delete a bucket from the metastore
            const deleteResult = await metastoreCollection.deleteOne({ _id: buckets[0]._name });
            assert.equal(deleteResult.deletedCount, 1);

            // Fetch bucket information
            const getBucketInfosPromise = new Promise((resolve, reject) => {
                client.getBucketInfos(logger, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(data);
                });
            });

            const data = await getBucketInfosPromise;

            // Perform the assertions
            assert.strictEqual(data.bucketCount, 2);
            assert.strictEqual(data.bucketInfos.find(bucket => bucket._name === buckets[0]._name), undefined);
            assert.strictEqual(!!data.bucketInfos.find(bucket => bucket._name === buckets[1]._name), true);
            assert.strictEqual(!!data.bucketInfos.find(bucket => bucket._name === buckets[2]._name), true);
        });
    });
});
