const fs = require('fs');

function getIsLocationTransientCb(log, locationConfigFile) {
    if (!fs.existsSync(locationConfigFile)) {
        log.info(
            'location conf file missing, falling back to PENSIEVE coll',
            { filename: locationConfigFile },
        );
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

function getMongoDbConfig(log, configFile) {
    let config;
    try {
        const data = fs.readFileSync(configFile, { encoding: 'utf-8' });
        config = JSON.parse(data).mongodb;
    } catch (err) {
        log.info(`could not parse config file: ${err.message}`);
        config = {
            database: 'metadata',
            replicaSet: 'rs0',
            replicationGroupId: 'RG001',
            shardCollections: false,
        };
    }
    return config;
}

function createMongoParams(log, customParams) {
    const locationConfigFile = process.env.LOCATION_CONFIG_FILE || 'conf/locationConfig.json';
    const config = getMongoDbConfig(log, process.env.CONFIG_FILE || 'conf/config.json');

    const params = {
        ...config,
        database: process.env.MONGODB_DATABASE || config.database,
        replicaSetHosts: process.env.MONGODB_REPLICASET || config.replicaSetHosts,
        isLocationTransient: getIsLocationTransientCb(log, locationConfigFile),
        writeConcern: 'majority',
        readPreference: 'secondaryPreferred',
        logger: log,
    };

    if (process.env.MONGODB_AUTH_USERNAME
        && process.env.MONGODB_AUTH_PASSWORD) {
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
