const { Logger } = require('werelogs');
const { listObjectsByReplicationStatus } = require('./listObjectsByReplicationStatus');

/**
 * Backwards compatibility wrapper for listFailedObjects
 * This script specifically lists objects with FAILED replication status
 * For more flexible replication status filtering, use listObjectsByReplicationStatus.js
 */

if (require.main === module) {
    const log = new Logger('s3utils:listFailedObjects');

    const BUCKETS = process.argv[2] || null;
    const { ACCESS_KEY, SECRET_KEY, ENDPOINT } = process.env;

    // Call the main function with FAILED status hardcoded
    listObjectsByReplicationStatus({
        buckets: BUCKETS,
        accessKey: ACCESS_KEY,
        secretKey: SECRET_KEY,
        endpoint: ENDPOINT,
        replicationStatus: 'FAILED',
        logger: log,
    }).then(() => {
        log.info('Completed successfully');
        process.exit(0);
    }).catch(err => {
        log.error('Failed with error', { error: err.message });
        process.exit(1);
    });
}
