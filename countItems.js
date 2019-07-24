const { MongoClientInterface } = require('arsenal').storage.metadata.mongoclient;
const { Logger } = require('werelogs');

const log = new Logger('S3Utils::ScanItemCount');
const replicaSetHosts = process.env.MONGODB_REPLICASET;

const config = {
    replicaSetHosts,
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
        log.error('error connecting to mongodb', {
            error: err,
        });
        return process.exit(1);
    }
    return mongoclient.scanItemCount(log, error => {
        // close connection before exiting
        mongoclient.close();
        if (error) {
            log.error('error occurred during mongo scan item count', {
                error,
            });
            return process.exit(1);
        }
        log.info('mongo scan item count has completed');
        return process.exit(0);
    });
});
