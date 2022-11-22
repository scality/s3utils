const werelogs = require('werelogs');
const env = require('./env');

werelogs.configure({
    level: env.logLevel,
    dump: 'error',
});

const log = new werelogs.Logger('s3utils:utapi-service-level-sidecar');

module.exports = log;
