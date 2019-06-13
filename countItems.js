const { storage } = require('arsenal');
const { Logger } = require('werelogs');

const { MongoClientInterface } = storage.metadata.mongoclient;

const INFOSTORE = '__infostore';

const log = new Logger('S3Utils::ScanItemCount');

const config = {
    replicaSetHosts: MONGODB_REPLICASET,
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: 'metadata',
    replicationGroupId: 'RG001',
    logger: log,
};

const mongoclient = new MongoClientInterface(config);
mongoclient.setup(err => {
    if (err) {
        return log.error('error connecting to mongodb', err);
    }
    return mongoclient.scanItemCount(log, (err, res) => {
        if (err) {
            return log.error('error occurred', err);
        }

        console.log(`--> ${JSON.stringify(res)}`);

        log.info('scan has completed...');
        return process.exit(0);
    });
});
