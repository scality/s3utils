const { waterfall, mapValuesLimit } = require('async');
const { Logger } = require('werelogs');
const ZenkoClient = require('zenkoclient');

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

const zenkoClient = new ZenkoClient({
    apiVersion: '2018-07-08-json',
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    endpoint: ENDPOINT,
    s3ForcePathStyle: true,
    signatureVersion: 'v4',
    maxRetries: 0,
    sslEnabled: false,
    httpOptions: { timeout: 0 },
});

waterfall([
    next => zenkoClient.getLocationsStatus(next),
    (locations, next) => {
        mapValuesLimit(locations, 3, (status, location, done) => {
            zenkoClient.listFailed({ Sitename: location }, (err, res) => {
                if (err) {
                    return done(err);
                }
                if (res.Versions.length > 0) {
                    return zenkoClient.retryFailedObjects({
                        Body: JSON.stringify(res.Versions),
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
