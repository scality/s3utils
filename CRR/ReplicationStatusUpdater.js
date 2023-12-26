const {
    doWhilst, eachSeries, eachLimit, waterfall, series,
} = require('async');
const werelogs = require('werelogs');
const { ObjectMD } = require('arsenal').models;

const { setupClients } = require('./clients');

const LOG_PROGRESS_INTERVAL_MS = 10000;

class ReplicationStatusUpdater {
    /**
     * @param {Object} params - An object containing the configuration parameters for the instance.
     * @param {Array<string>} params.buckets - An array of bucket names to process.
     * @param {Array<string>} params.replicationStatusToProcess - Replication status to be processed.
     * @param {number} params.workers - Number of worker threads for processing.
     * @param {string} params.accessKey - Access key for AWS SDK authentication.
     * @param {string} params.secretKey - Secret key for AWS SDK authentication.
     * @param {string} params.endpoint - Endpoint URL for the S3 service.
     * @param {Object} log - The logging object used for logging purposes within the instance.
     *
     * @param {string} [params.siteName] - (Optional) Name of the destination site.
     * @param {string} [params.storageType] - (Optional) Type of the destination site (aws_s3, azure...).
     * @param {string} [params.targetPrefix] - (Optional) Prefix to target for replication.
     * @param {number} [params.listingLimit] - (Optional) Limit for listing objects.
     * @param {number} [params.maxUpdates] - (Optional) Maximum number of updates to perform.
     * @param {number} [params.maxScanned] - (Optional) Maximum number of items to scan.
     * @param {string} [params.keyMarker] - (Optional) Key marker for resuming object listing.
     * @param {string} [params.versionIdMarker] - (Optional) Version ID marker for resuming object listing.
     */
    constructor(params, log) {
        const {
            buckets,
            replicationStatusToProcess,
            workers,
            accessKey,
            secretKey,
            endpoint,
            siteName,
            storageType,
            targetPrefix,
            listingLimit,
            maxUpdates,
            maxScanned,
            keyMarker,
            versionIdMarker,
        } = params;

        // inputs
        this.buckets = buckets;
        this.replicationStatusToProcess = replicationStatusToProcess;
        this.workers = workers;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.siteName = siteName;
        this.storageType = storageType;
        this.targetPrefix = targetPrefix;
        this.listingLimit = listingLimit;
        this.maxUpdates = maxUpdates;
        this.maxScanned = maxScanned;
        this.inputKeyMarker = keyMarker;
        this.inputVersionIdMarker = versionIdMarker;
        this.log = log;

        this._setupClients();

        this.logProgressInterval = setInterval(this._logProgress.bind(this), LOG_PROGRESS_INTERVAL_MS);

        // intenal state
        this._nProcessed = 0;
        this._nSkipped = 0;
        this._nUpdated = 0;
        this._nErrors = 0;
        this._bucketInProgress = null;
        this._VersionIdMarker = null;
        this._KeyMarker = null;
    }

    /**
     * Sets up and initializes the S3 and Backbeat client instances.
     *
     * @returns {void} This method does not return a value; instead, it sets the S3 and Backbeat clients.
     */
    _setupClients() {
        const { s3, bb } = setupClients({
            accessKey: this.accessKey,
            secretKey: this.secretKey,
            endpoint: this.endpoint,
        }, this.log);

        this.s3 = s3;
        this.bb = bb;
    }

    /**
     * Logs the progress of the CRR process at regular intervals.
     * @private
     * @returns {void}
     */
    _logProgress() {
        this.log.info('progress update', {
            updated: this._nUpdated,
            skipped: this._nSkipped,
            errors: this._nErrors,
            bucket: this._bucketInProgress || null,
            keyMarker: this._KeyMarker || null,
            versionIdMarker: this._VersionIdMarker || null,
        });
    }


    /**
     * Determines if an object should be updated based on its replication metadata properties.
     * @private
     * @param {ObjectMD} objMD - The metadata of the object.
     * @returns {boolean} True if the object should be updated.
     */
    _objectShouldBeUpdated(objMD) {
        return this.replicationStatusToProcess.some(filter => {
            if (filter === 'NEW') {
                return (!objMD.getReplicationInfo()
                    || objMD.getReplicationInfo().status === '');
            }
            return (objMD.getReplicationInfo()
                && objMD.getReplicationInfo().status === filter);
        });
    }

    /**
     * Marks an object as pending for replication.
     * @private
     * @param {string} bucket - The bucket name.
     * @param {string} key - The object key.
     * @param {string} versionId - The object version ID.
     * @param {string} storageClass - The storage class for replication.
     * @param {Object} repConfig - The replication configuration.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _markObjectPending(
        bucket,
        key,
        versionId,
        storageClass,
        repConfig,
        cb,
    ) {
        let objMD;
        let skip = false;
        return waterfall([
            // get object blob
            next => this.bb.getMetadata({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }, next),
            (mdRes, next) => {
                // NOTE: The Arsenal Object Metadata schema version 8.1 is being used for both Ring S3C and Artesca,
                // it is acceptable because the 8.1 schema only adds extra properties to the 7.10 schema.
                // This is beneficial because:
                // - Forward compatibility: Having the 8.1 properties in place now ensures that
                //   S3C is compatible with the 8.1 schema, which could be useful if we plan to upgrade
                //   from 7.10 to 8.1 in the future.
                // - No impact on current functionality: The extra properties from the 8.1
                //   schema do not interfere with the current functionalities of the 7.10 environment,
                //   so there is no harm in keeping them. S3C should ignore them without causing any issues.
                // - Simple codebase: Not having to remove these properties simplifies the codebase of s3utils.
                //   Less complexity and potential errors linked with conditionally removing metadata properties
                //   based on the version.
                // - Single schema approach: Keeping a single, unified schema approach in s3utils can make the
                //   codebase easier to maintain and upgrade, as opposed to having multiple branches or versions of
                //   the code for different schema versions.
                objMD = new ObjectMD(JSON.parse(mdRes.Body));
                if (!this._objectShouldBeUpdated(objMD)) {
                    skip = true;
                    return process.nextTick(next);
                }
                // Initialize replication info, if missing
                // This is particularly important if the object was created before
                // enabling replication on the bucket.
                if (!objMD.getReplicationInfo()
                    || !objMD.getReplicationSiteStatus(storageClass)) {
                    const { Rules, Role } = repConfig;
                    const destination = Rules[0].Destination.Bucket;
                    // set replication properties
                    const ops = objMD.getContentLength() === 0 ? ['METADATA']
                        : ['METADATA', 'DATA'];
                    const backends = [{
                        site: storageClass,
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    }];
                    const replicationInfo = {
                        status: 'PENDING',
                        backends,
                        content: ops,
                        destination,
                        storageClass,
                        role: Role,
                        storageType: this.storageType,
                    };
                    objMD.setReplicationInfo(replicationInfo);
                }

                objMD.setReplicationSiteStatus(storageClass, 'PENDING');
                objMD.setReplicationStatus('PENDING');
                objMD.updateMicroVersionId();
                const md = objMD.getSerialized();
                return this.bb.putMetadata({
                    Bucket: bucket,
                    Key: key,
                    VersionId: versionId,
                    ContentLength: Buffer.byteLength(md),
                    Body: md,
                }, next);
            },
        ], err => {
            ++this._nProcessed;
            if (err) {
                ++this._nErrors;
                this.log.error('error updating object', {
                    bucket, key, versionId, error: err.message,
                });
                cb();
                return;
            }
            if (skip) {
                ++this._nSkipped;
            } else {
                ++this._nUpdated;
            }
            cb();
        });
    }

    /**
     * Lists object versions for a bucket.
     * @private
     * @param {string} bucket - The bucket name.
     * @param {string} VersionIdMarker - The version ID marker for pagination.
     * @param {string} KeyMarker - The key marker for pagination.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _listObjectVersions(bucket, VersionIdMarker, KeyMarker, cb) {
        return this.s3.listObjectVersions({
            Bucket: bucket,
            MaxKeys: this.listingLimit,
            Prefix: this.targetPrefix,
            VersionIdMarker,
            KeyMarker,
        }, cb);
    }

    /**
     * Marks pending replication for listed object versions.
     * @private
     * @param {string} bucket - The bucket name.
     * @param {Array} versions - Array of object versions.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _markPending(bucket, versions, cb) {
        const options = { Bucket: bucket };
        waterfall([
            next => this.s3.getBucketReplication(options, (err, res) => {
                if (err) {
                    this.log.error('error getting bucket replication', { error: err });
                    return next(err);
                }
                return next(null, res.ReplicationConfiguration);
            }),
            (repConfig, next) => {
                const { Rules } = repConfig;
                const storageClass = Rules[0].Destination.StorageClass || this.siteName;
                if (!storageClass) {
                    const errMsg = 'missing SITE_NAME environment variable, must be set to'
                        + ' the value of "site" property in the CRR configuration';
                    this.log.error(errMsg);
                    return next(new Error(errMsg));
                }
                return eachLimit(versions, this.workers, (i, apply) => {
                    const { Key, VersionId } = i;
                    this._markObjectPending(bucket, Key, VersionId, storageClass, repConfig, apply);
                }, next);
            },
        ], cb);
    }

    /**
     * Triggers CRR process on a specific bucket.
     * @private
     * @param {string} bucketName - The name of the bucket.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _triggerCRROnBucket(bucketName, cb) {
        const bucket = bucketName.trim();
        this._bucketInProgress = bucket;
        this.log.info(`starting task for bucket: ${bucket}`);
        if (this.inputKeyMarker || this.inputVersionIdMarker) {
            // resume from where we left off in previous script launch
            this._KeyMarker = this.inputKeyMarker;
            this._VersionIdMarker = this.inputVersionIdMarker;
            this.inputKeyMarker = undefined;
            this.inputVersionIdMarker = undefined;
            this.log.info(`resuming bucket: ${bucket} at: KeyMarker=${this._KeyMarker} `
                + `VersionIdMarker=${this._VersionIdMarker}`);
        }
        return doWhilst(
            done => this._listObjectVersions(
                bucket,
                this._VersionIdMarker,
                this._KeyMarker,
                (err, data) => {
                    if (err) {
                        this.log.error('error listing object versions', { error: err });
                        return done(err);
                    }
                    return this._markPending(bucket, data.Versions.concat(data.DeleteMarkers), err => {
                        if (err) {
                            return done(err);
                        }
                        this._VersionIdMarker = data.NextVersionIdMarker;
                        this._KeyMarker = data.NextKeyMarker;
                        return done();
                    });
                },
            ),
            () => {
                if (this._nUpdated >= this.maxUpdates || this._nProcessed >= this.maxScanned) {
                    this._logProgress();
                    let remainingBuckets;
                    if (this._VersionIdMarker || this._KeyMarker) {
                        // next bucket to process is still the current one
                        remainingBuckets = this.buckets.slice(
                            this.buckets.findIndex(bucket => bucket === bucketName),
                        );
                    } else {
                        // next bucket to process is the next in bucket list
                        remainingBuckets = this.buckets.slice(
                            this.buckets.findIndex(bucket => bucket === bucketName) + 1,
                        );
                    }
                    let message = 'reached '
                        + `${this._nUpdated >= this.maxUpdates ? 'update' : 'scanned'} `
                        + 'count limit, resuming from this '
                        + 'point can be achieved by re-running the script with '
                        + `the bucket list "${remainingBuckets.join(',')}"`;
                    if (this._VersionIdMarker || this._KeyMarker) {
                        message += ' and the following environment variables set: '
                            + `KEY_MARKER=${this._KeyMarker} `
                            + `VERSION_ID_MARKER=${this._VersionIdMarker}`;
                    }
                    this.log.info(message);
                    return false;
                }
                if (this._VersionIdMarker || this._KeyMarker) {
                    return true;
                }
                return false;
            },
            err => {
                this._bucketInProgress = null;
                if (err) {
                    this.log.error('error marking objects for crr', { bucket });
                    cb(err);
                    return;
                }
                this._logProgress();
                this.log.info(`completed task for bucket: ${bucket}`);
                cb();
            },
        );
    }

    /**
     * Runs the CRR process on all configured buckets.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    run(cb) {
        return eachSeries(this.buckets, this._triggerCRROnBucket.bind(this), err => {
            clearInterval(this.logProgressInterval);
            if (err) {
                cb(err);
                return;
            }
            cb();
        });
    }

    /**
     * Stops the execution of the CRR process.
     * NOTE: This method terminates the node.js process, and hence it does not return a value.
     * @returns {void}
     */
    stop() {
        this.log.warn('stopping execution');
        this._logProgress();
        clearInterval(this.logProgressInterval);
        process.exit(1);
    }
}

module.exports = ReplicationStatusUpdater;
