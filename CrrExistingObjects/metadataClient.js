const { MetadataWrapper } = require('arsenal').storage.metadata;
const werelogs = require('werelogs');
const createMongoParams = require('../utils/createMongoParams');
const listingParser = require('./listingParser');

const loggerConfig = {
    level: 'info',
    dump: 'error',
};
werelogs.configure(loggerConfig);

const log = new werelogs.Logger('s3utils::crrExistingObjects');
const implName = 'mongodb';
const params = {
    customListingParser: listingParser,
    mongodb: createMongoParams(log, { readPreference: 'primary' }),
};
const metadata = new MetadataWrapper(implName, params, null, log);

module.exports = metadata;
