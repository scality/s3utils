const async = require('async');
const werelogs = require('werelogs');
const { MongoMemoryReplSet } = require('mongodb-memory-server');
const { BucketInfo, ObjectMD } = require('arsenal').models;

const {
    MongoClientInterfaceStalled,
} = require('../../StalledRetry/MongoClientInterfaceStalled');
const {
    StalledCursorWrapper,
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
const dbName = 'metadata';

const mongoserver = new MongoMemoryReplSet({
    debug: false,
    instanceOpts: [
        { port: 27017 },
    ],
    replSet: {
        name: 'rs0',
        count: 1,
        dbName,
        storageEngine: 'ephemeralForTest',
    },
});

function wrapperFactory(bucketName, cmpDate, cursor, log) {
    return new StalledCursorWrapper(
        cursor,
        {
            queueLimit: 10,
            log,
            cmpDate,
            bucketName,
        }
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
        }
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
        }).getValue();
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
                next => async.timesSeries(1000, (m, done) => {
                    async.parallel([
                        done => {
                            const objName = `stalled-${m}`;
                            const cmpDate = new Date();
                            cmpDate.setHours(cmpDate.getHours() - 10);
                            const lastModified = cmpDate.toUTCString();
                            return client.putObject(
                                bucketName,
                                objName,
                                createStalledObject(objName, lastModified),
                                { versionId: null, versioning: true },
                                logger,
                                done
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
                                done
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
                                done
                            );
                        },
                    ], done);
                }, next),
            ], done);
        }, next),
    ], callback);
}


jest.setTimeout(3600000);
describe('StalledRetry', () => {
    let mgoClient;
    let reqClient;

    beforeAll(done => {
        reqClient = new TrackingClient();
        const opts = {
            replicaSetHosts: 'localhost:27017',
            writeConcern: 'majority',
            replicaSet: 'rs0',
            readPreference: 'primary',
            database: dbName,
            replicationGroupId: 'RG001',
            logger,
            cursorWrapperFactory: wrapperFactory,
            requestHandlerFactory: handlerFactory.bind(null, reqClient),
        };
        mgoClient = new MongoClientInterfaceStalled(opts);
        async.series([
            next => mongoserver.waitUntilRunning()
                .then(() => next())
                .catch(next),
            next => mgoClient.setup(next),
            next => populateMongo(mgoClient, next),
        ], done);
    });

    afterAll(done => {
        async.series([
            next => mgoClient.close(next),
            next => mongoserver.stop()
                .then(() => next())
                .catch(next),
        ], done);
    });

    test('should correct', done => {
        mgoClient.queueStalledObjects((err, res) => {
            expect(err).toBeNull();
            expect(res).toEqual(20000);
            done();
        });
    });
});
