const { MongoClientInterface } =
    require('arsenal').storage.metadata.mongoclient;
const { Logger } = require('werelogs');
const fs = require('fs');

const log = new Logger('S3Utils::ScanItemCount');
const replicaSetHosts = process.env.MONGODB_REPLICASET;
const database = process.env.MONGODB_DATABASE || 'metadata';

function getIsLocationTransientCb() {
    const locationConfigFile = 'conf/locationConfig.json';
    if (!fs.existsSync(locationConfigFile)) {
        log.info('location conf file missing, falling back to PENSIEVE coll',
            { filename: locationConfigFile });
        return null;
    }

    const buf = fs.readFileSync(locationConfigFile);
    const locationConfig = JSON.parse(buf.toString());

    return function locationIsTransient(locationName, log, cb) {
        if (!locationConfig[locationName]) {
            log.error('unknown location', { locationName });
            process.nextTick(cb, null, false);
            return;
        }
        const isTransient = Boolean(locationConfig[locationName].isTransient);
        process.nextTick(cb, null, isTransient);
    };
}

const params = {
    replicaSetHosts,
    database,
    isLocationTransient: getIsLocationTransientCb(),
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'secondaryPreferred',
    replicationGroupId: 'RG001',
    logger: log,
};

if (process.env.MONGODB_AUTH_USERNAME &&
    process.env.MONGODB_AUTH_PASSWORD) {
    params.authCredentials = {
        username: process.env.MONGODB_AUTH_USERNAME,
        password: process.env.MONGODB_AUTH_PASSWORD,
    };
}

const mongoclient = new MongoClientInterface(params);

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
