const assert = require('assert');
const async = require('async');
const { BucketInfo } = require('arsenal').models;
const { exec } = require('child_process');
const { MongoClientInterface } =
    require('arsenal').storage.metadata.mongoclient;
const { MongoMemoryReplSet } = require('mongodb-memory-server');
const werelogs = require('werelogs');

const logger = new werelogs.Logger('CountItems::Test::Functional');
const dbName = 'metadata';
const mongoserver = new MongoMemoryReplSet({
    debug: false,
    instanceOpts: [
        { port: 27018 },
    ],
    replSet: {
        name: 'rs0',
        count: 1,
        dbName,
        storageEngine: 'ephemeralForTest',
    },
});

function populateMongo(client, callback) {
    async.timesSeries(3, (n, done) => {
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
            _bucketPolicy: null,
            _objectLockEnalbed: false,
            _objectLockConfiguration: null,
        });
        client.createBucket(bucketName, bucketMD, logger, done);
    }, callback);
}

jest.setTimeout(20000);
describe('ObjectLockExistingBuckets', () => {
    let client;
    beforeAll(done => {
        const opts = {
            replicaSetHosts: 'localhost:27017',
            writeConcern: 'majority',
            replicaSet: 'rs0',
            readPreference: 'primary',
            database: dbName,
            replicationGroupId: 'RG001',
            logger,
        };
        client = new MongoClientInterface(opts);
        async.series([
            next => mongoserver.waitUntilRunning()
                .then(() => next())
                .catch(next),
            next => client.setup(next),
            next => populateMongo(client, next),
        ], done);
    });

    afterAll(done => {
        async.series([
            next => client.close(next),
            next => mongoserver.stop()
                .then(() => next())
                .catch(next),
        ], done);
    });


    test('should fail if no bucket list provided', done => {
        exec('node objectLockExistingBuckets.js', err => {
            assert.strictEqual(err.message,
                'No buckets given as input, please provide a comma-separated ' +
                'list of buckets on the command line');
            done();
        });
    });

    test('should enable Object Lock on each existing bucket', done => {
        exec('node objectLockExistingBuckets.js ' +
        'test-bucket-0,test-bucket-1,test-bucket-2', (err, stderr, stdout) => {
            assert.ifError(err);
            assert.strictEqual(stdout,
                'Object Lock enabled for specified buckets');
            done();
        });
    });
});
