const { Logger } = require('werelogs');
const createMongoParams = require('../utils/createMongoParams');
const { collectBucketMetricsAndUpdateBucketCapacityInfo } = require('./collectBucketMetricsAndUpdateBucketCapacityInfo');
const S3UtilsMongoClient = require('../utils/S3UtilsMongoClient');

const log = new Logger('s3utils:DataReport');

function stop() {
    log.info('stopping execution');
    process.exit(0);
}

function main() {
    const mongoClient = new S3UtilsMongoClient(createMongoParams(log));
    mongoClient.setup(err => {
        if (err) {
            log.error('error connecting to mongodb', {
                error: err,
            });
            process.exit(1);
        }
        collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, log, err => {
            if (err) {
                log.error('error collecting bucket metrics and updating bucket capacity info', {
                    error: err,
                });
                mongoClient.close(() => process.exit(1));
            }
            mongoClient.close(() => stop());
        });
    });
}

main();

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
