const async = require('async');
const werelogs = require('werelogs');
const { BucketInfo, ObjectMD } = require('arsenal').models;

const {
    MongoClientInterfaceStalled,
} = require('../../StalledRetry/MongoClientInterfaceStalled');
const {
    RateLimitingCursor,
} = require('../../StalledRetry/CursorWrapper');

const {
    StalledRequestHandler,
} = require('../../StalledRetry/StalledRequestHandler');


const loggerConfig = {
    level: 'debug',
    dump: 'error',
};

werelogs.configure(loggerConfig);

const logger = new werelogs.Logger('StalledRetry::Test::Functional');
const { MONGODB_REPLICASET } = process.env;
const dbName = 'stalledRetryTest';

function wrapperFactory(bucketName, cmpDate, cursor, log) {
    return new RateLimitingCursor(
        cursor,
        {
            queueLimit: 10,
            log,
            cmpDate,
            bucketName,
        },
    );
}

class TrackingClient {
    constructor() {
        this.requestedBatch = [];
    }

    retryFailedObjects(params, done) {
        this.requestedBatch.push(params.Body);
        return done();
    }
}

function handlerFactory(client, log) {
    return new StalledRequestHandler(
        client,
        {
            dryRun: false,
            batchSize: 10,
            concurrentRequests: 5,
            log,
        },
    );
}

function createStalledObject(objName, lastModified) {
    return new ObjectMD()
        .setKey(objName)
        .setDataStoreName('us-east-1')
        .setLastModified(lastModified)
        .setReplicationInfo({
            status: 'PENDING',
            backends: [],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
            isNFS: null,
        })
        .setReplicationStorageClass('location-1,location-2')
        .getValue();
}

function createCompletedObject(objName) {
    return new ObjectMD()
        .setKey(objName)
        .setDataStoreName('us-east-1')
        .setLastModified('2020-01-01T00:00:00.000Z')
        .setReplicationInfo({
            status: 'COMPLETED',
            backends: [
                {
                    status: 'COMPLETED',
                    site: 'location-1',
                },
                {
                    status: 'COMPLETED',
                    site: 'location-2',
                },
            ],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
            isNFS: null,
        })
        .getValue();
}

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
                next => async.timesSeries(100, (m, done) => {
                    async.parallel([
                        done => {
                            const objName = `stalled-1hr-${m}`;
                            const cmpDate = new Date();
                            cmpDate.setHours(cmpDate.getHours() - 1);
                            const lastModified = cmpDate.toUTCString();
                            return client.putObject(
                                bucketName,
                                objName,
                                createStalledObject(objName, lastModified),
                                { versionId: null, versioning: true },
                                logger,
                                done,
                            );
                        },
                        done => {
                            const objName = `stalled-10hr-${m}`;
                            const cmpDate = new Date();
                            cmpDate.setHours(cmpDate.getHours() - 10);
                            const lastModified = cmpDate.toUTCString();
                            return client.putObject(
                                bucketName,
                                objName,
                                createStalledObject(objName, lastModified),
                                { versionId: null, versioning: true },
                                logger,
                                done,
                            );
                        },
                        done => {
                            const objName = `pending-${m}`;
                            const cmpDate = new Date();
                            const lastModified = cmpDate.toUTCString();
                            return client.putObject(
                                bucketName,
                                objName,
                                createStalledObject(objName, lastModified),
                                { versionId: null, versioning: true },
                                logger,
                                done,
                            );
                        },
                        done => {
                            const objName = `completed-${m}`;
                            return client.putObject(
                                bucketName,
                                objName,
                                createCompletedObject(objName),
                                { versionId: null, versioning: true },
                                logger,
                                done,
                            );
                        },
                    ], done);
                }, next),
            ], done);
        }, next),
    ], callback);
}


jest.setTimeout(3600000);
describe.skip('StalledRetry', () => {
    let mgoClient;
    let reqClient;

    beforeAll(done => {
        reqClient = new TrackingClient();
        const opts = {
            mongodb: {
                replicaSetHosts: MONGODB_REPLICASET,
                writeConcern: 'majority',
                replicaSet: 'rs0',
                readPreference: 'primary',
                database: dbName,
                replicationGroupId: 'RG001',
                logger,
            },
            cursorWrapperFactory: wrapperFactory,
            requestHandlerFactory: handlerFactory.bind(null, reqClient),
        };
        mgoClient = new MongoClientInterfaceStalled(opts);
        async.series([
            next => mgoClient.setup(next),
            next => populateMongo(mgoClient, next),
        ], done);
    });

    afterAll(done => {
        async.series([
            next => mgoClient.db.dropDatabase(next),
            next => mgoClient.close(next),
        ], done);
    });

    test('should correctly process stalled entries (all stalled)', done => {
        mgoClient.queueStalledObjects(1, (err, res) => {
            expect(err).toBeNull();
            expect(res).toEqual(4000);
            done();
        });
    });

    test('should correctly process stalled entries (skip 1hr exp)', done => {
        mgoClient.queueStalledObjects(5, (err, res) => {
            expect(err).toBeNull();
            expect(res).toEqual(2000);
            done();
        });
    });

    test('should correctly process stalled entries (skip 1+10hr exp)', done => {
        mgoClient.queueStalledObjects(11, (err, res) => {
            expect(err).toBeNull();
            expect(res).toEqual(0);
            done();
        });
    });
});
