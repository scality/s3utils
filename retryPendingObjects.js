const async = require('async');
const { Logger } = require('werelogs');
const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;
const pendingObjectsList = require('./pendingObjects');

const { MONGODB_REPLICASET } = process.env;
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
    const c = mc.db.collection(BUCKET);
    let updated = 0;
    async.eachLimit(filteredList, 20, (key, cb) => {
        return c.findOne({ _id: key._id }, {}, (err, obj) => {
            if (err) {
                return cb(err);
            }
            return c.replaceOne({ _id: key._id }, obj, { upsert: true }, err => {
                if (err) {
                    log.error('error during replace', { error: err, stack: err.stack });
                    return cb(err);
                }
                updated++;
                return cb();

            });
        });
    }, err => {
        if (err) {
            log.error('final cb', err);
        }
        log.info('completed retrying objects', { updated });
        return process.exit(1);
    });
});
