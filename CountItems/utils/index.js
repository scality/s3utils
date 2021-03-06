const fs = require('fs');

function getIsLocationTransientCb(log, locationConfigFile) {
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

function createMongoParams(log, customParams) {
    const replicaSetHosts = process.env.MONGODB_REPLICASET;
    const database = process.env.MONGODB_DATABASE || 'metadata';
    const locationConfigFile =
        process.env.LOCATION_CONFIG_FILE || 'conf/locationConfig.json';

    const params = {
        replicaSetHosts,
        database,
        isLocationTransient: getIsLocationTransientCb(log, locationConfigFile),
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
    return Object.assign(params, customParams || {});
}

module.exports = {
    createMongoParams,
};
