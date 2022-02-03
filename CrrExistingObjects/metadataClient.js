const { MetadataWrapper } = require('arsenal').storage.metadata;
const werelogs = require('werelogs');
const listingParser = require('./listingParser');

const loggerConfig = {
    level: 'info',
    dump: 'error',
};
werelogs.configure(loggerConfig);

const log = new werelogs.Logger('s3utils::crrExistingObjects');
const replicaSetHosts = process.env.MONGODB_REPLICASET;
const replicaSet = 'rs0';
const writeConcern = 'majority';
const readPreference = 'primary';
const database = process.env.MONGODB_DATABASE || 'metadata';
const implName = 'mongodb';
const replicationGroupId = process.env.REPLICATION_GROUP_ID;
const params = {
    customListingParser: listingParser,
    mongodb: {
        replicaSetHosts,
        writeConcern,
        replicaSet,
        readPreference,
        database,
    },
    replicationGroupId,
};
if (process.env.MONGODB_AUTH_USERNAME
    && process.env.MONGODB_AUTH_PASSWORD) {
    params.authCredentials = {
        username: process.env.MONGODB_AUTH_USERNAME,
        password: process.env.MONGODB_AUTH_PASSWORD,
    };
}
const metadata = new MetadataWrapper(implName, params, null, log);

module.exports = metadata;
