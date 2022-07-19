
const async = require('async');
const { Logger } = require('werelogs');
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;
const pendingObjectsList = require('./pendingObjects');

const MONGODB_REPLICASET = process.env.MONGODB_REPLICASET;
const MONGODB_DATABASE = process.env.MONGODB_DATABASE || 'metadata';
const BUCKET = process.env.BUCKET;

if (!MONGODB_REPLICASET) {
    throw new Error('MONGODB_REPLICASET not defined');
}

if (!BUCKET) {
    throw new Error('BUCKET not defined');
}

const log = new Logger('S3Utils::RetryPendingObjects');

const config = {
    replicaSetHosts: MONGODB_REPLICASET,
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: MONGODB_DATABASE,
    replicationGroupId: 'RG001',
    logger: log,
};

const filteredList = pendingObjectsList.filter(obj => obj._id.includes('\u0000'));

const mc = new MongoClientInterface(config);

mc.setup(err => {
    if (err) {
        log.error('error connecting to mongodb', err);
        return process.exit(1);
    }
    console.log(mc, mc.db)
    return mc.db.createCollection(BUCKET, err => {
        if (err) {
            log.error('unable to create bucket', { error: err.stack });
            return process.exit(1);
        }
        const c = mc.db.collection(BUCKET);
        return async.eachLimit(filteredList, 20, (key, cb) => {
            return c.insert({ _id: key._id, value: { "foo": "bar", "bar" : "baz", replicationInfo: { status: 'COMPLETED' } }}, (err, obj) => {
                if (err) {
                    return cb(err);
                }
                return cb(null, obj);
            });
        }, (err, res) => {
            if (err) {
                log.error('final cb', err);
                return process.exit(1);
            }
            return console.log('job finished', err, res);
        });
    });

});
