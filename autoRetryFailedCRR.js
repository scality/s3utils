const { waterfall, mapValuesLimit } = require('async');
const { Logger } = require('werelogs');
const CloudserverClient = require('./Clients/CloudserverClient');

const log = new Logger('s3utils::autoRetryFailedCRR');
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { ENDPOINT } = process.env;
if (!ENDPOINT) {
    throw new Error('ENDPOINT not defined!');
}
if (!ACCESS_KEY) {
    throw new Error('ACCESS_KEY not defined');
}
if (!SECRET_KEY) {
    throw new Error('SECRET_KEY not defined');
}

const cloudserverClient = new CloudserverClient(
    ENDPOINT,
    ACCESS_KEY,
    SECRET_KEY,
);

waterfall([
    next => cloudserverClient.getLocationsStatus(next),
    (res, next) => {
        const locations = res.status || {};
        mapValuesLimit(locations, 3, (status, location, done) => {
            cloudserverClient.listFailed({ Sitename: location }, (err, res) => {
                if (err) {
                    return done(err);
                }
                if (res.Versions && res.Versions.length > 0) {
                    return cloudserverClient.retryFailedObjects({
                        Body: Buffer.from(JSON.stringify(res.Versions)),
                    }, done);
                }
                return done();
            });
        }, next);
    },
], (err, res) => {
    if (err) {
        return log.error('error performing operation', { error: err });
    }
    return log.info('success retrying failed crr', { result: res });
});
