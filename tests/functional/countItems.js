const cluster = require('cluster');
const async = require('async');
const werelogs = require('werelogs');
const { MongoClientInterface } =
    require('arsenal').storage.metadata.mongoclient;
const { BucketInfo, ObjectMD } = require('arsenal').models;

const CountMaster = require('../../CountItems/CountMaster');
const CountManager = require('../../CountItems/CountManager');
const { createMongoParams } = require('../../CountItems/utils');
const createWorkers = require('../../CountItems/utils/createWorkers');

const logger = new werelogs.Logger('CountItems::Test::Functional');
const MONGODB_REPLICASET = process.env.MONGODB_REPLICASET;
const dbName = 'countItemsTest';

const expectedResults = {
    objects: 100,
    versions: 100,
    buckets: 10,
    dataManaged: {
        total: { curr: 30000, prev: 30000 },
        byLocation: {
            'us-east-1': { curr: 10000, prev: 10000 },
            'secondary-location-1': { curr: 10000, prev: 10000 },
            'secondary-location-2': { curr: 10000, prev: 10000 },
        },
    },
    stalled: 0,
};

const expectedBucketList = Array.from(Array(10)).map((_, ind) => ({
    name: `test-bucket-${ind}`,
    location: 'primary-location',
    isVersioned: true,
    ownerCanonicalId: 'testowner',
    ingestion: false,
}));

function populateMongo(client, callback) {
    async.series([
        next => async.timesSeries(10, (n, done) => {
            const bucketName = `test-bucket-${n}`;
            const bucketMD = BucketInfo.fromObj({
                _name: bucketName,
                _owner: 'testowner',
                _ownerDisplayName: 'testdisplayname',
                _creationDate: new Date().toJSON(),
                _acl: {
                    Canned: 'private',
                    FULL_CONTROL: [],
                    WRITE: [],
                    WRITE_ACP: [],
                    READ: [],
                    READ_ACP: [],
                },
                _mdBucketModelVersion: 10,
                _transient: false,
                _deleted: false,
                _serverSideEncryption: null,
                _versioningConfiguration: { Status: 'Enabled' },
                _locationConstraint: 'primary-location',
                _readLocationConstraint: null,
                _cors: null,
                _replicationConfiguration: null,
                _lifecycleConfiguration: null,
                _uid: '',
                _isNFS: null,
                ingestion: null,
            });
            async.series([
                next => client.createBucket(bucketName, bucketMD, logger, next),
                next => async.timesSeries(10, (m, done) => {
                    const objName = `test-object-${m}`;
                    const objMD = new ObjectMD()
                        .setKey(objName)
                        .setDataStoreName('us-east-1')
                        .setContentLength(100)
                        .setLastModified('2020-01-01T00:00:00.000Z')
                        .setReplicationInfo({
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
                            content: [],
                            destination: '',
                            storageClass: '',
                            role: '',
                            storageType: '',
                            dataStoreVersionId: '',
                            isNFS: null,
                        });
                    async.timesSeries(2, (z, done) => {
                        client.putObject(
                            bucketName,
                            objName,
                            objMD.getValue(),
                            {
                                versionId: null,
                                versioning: true,
                            }, logger, done);
                    }, done);
                }, next),
            ], done);
        }, next),
    ], callback);
}

jest.setTimeout(120000);
describe('CountItems', () => {
    const oldEnv = process.env;
    let client;

    beforeAll(done => {
        process.env = Object.assign({}, oldEnv);
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
        client = new MongoClientInterface(opts);
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
                client: new MongoClientInterface(createMongoParams(logger)),
            });

            async.series([
                next => countMaster.start(err => {
                    expect(err).toBeFalsy();
                    return next();
                }),
                next => client.readCountItems(logger, (err, res) => {
                    expect(err).toBeFalsy();
                    expect(res).toMatchObject(expectedResults);
                    expect(res.bucketList)
                        .toEqual(expect.arrayContaining(expectedBucketList));
                    return next();
                }),
            ], () => countMaster.stop(null, done));
        });
});
