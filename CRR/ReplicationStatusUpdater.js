const {
    doWhilst, eachSeries, eachLimit, waterfall,
} = require('async');
const { ObjectMD } = require('arsenal').models;
const { 
    ListObjectVersionsCommand, 
    GetBucketReplicationCommand 
} = require('@aws-sdk/client-s3');
const CloudserverClient = require('../Clients/CloudserverClient');
const createS3Client = require('../Clients/s3Client');

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
     * @param {boolean} [params.currentVersionOnly] - (Optional) Whether to process only the current version of objects.
     * @param {boolean} [params.forceUsingConfiguration] - (Optional) Force reset replication target to bucket's configuration.
     * @param {boolean} [params.allowNewSite] - (Optional) Accept a site that is not in the bucket
     * replication rules and not in the object replication info.
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
            currentVersionOnly,
            forceUsingConfiguration,
            allowNewSite,
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
        this.currentVersionOnly = currentVersionOnly;
        this.forceUsingConfiguration = forceUsingConfiguration;
        this.allowNewSite = allowNewSite;
        this.log = log;

        this._setupClients();

        this.logProgressInterval = setInterval(this._logProgress.bind(this), LOG_PROGRESS_INTERVAL_MS);

        // intenal state
        this._nProcessed = 0;
        this._nSkipped = 0;
        this._nUpdated = 0;
        this._nErrors = 0;
        this._bucketInProgress = null;
        this._bucketStopped = false;
        this._stoppedBuckets = [];
        this._VersionIdMarker = null;
        this._KeyMarker = null;
    }

    /**
     * Sets up and initializes the S3 and Cloudserver client instances.
     *
     * @returns {void} This method does not return a value; instead, it sets the S3 and Cloudserver clients.
     */
    _setupClients() {
        this.s3 = createS3Client({
            accessKey: this.accessKey,
            secretKey: this.secretKey,
            endpoint: this.endpoint,
        }, this.log);
        this.cloudserverclient = new CloudserverClient(this.endpoint, this.accessKey, this.secretKey);
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
     * @param {string} site - The destination site name.
     * @returns {boolean} True if the object should be updated.
     */
    _objectShouldBeUpdated(objMD, site) {
        return this.replicationStatusToProcess.some(filter => {
            if (filter === 'NEW') {
                // Either site specific replication info is missing
                // or are initialized with empty fields.
                return (!objMD.getReplicationInfo()
                    || !objMD.getReplicationSiteStatus(site));
            }
            return (objMD.getReplicationInfo()
                && objMD.getReplicationSiteStatus(site) === filter);
        });
    }

    /**
     * Returns the sites set in the StorageClass of the bucket replication rules.
     * Empty when the rules use the default replication endpoint (no StorageClass).
     * @private
     * @param {Array} rules - Bucket replication rules.
     * @returns {Array<string>} Site names.
     */
    _getConfiguredSites(rules) {
        return (rules || [])
            .map(rule => rule.Destination && rule.Destination.StorageClass)
            .filter(Boolean)
            .flatMap(storageClass => storageClass.split(','))
            .map(site => site.split(':preferred_read')[0]);
    }

    /**
     * Returns the sites in the object replication info (storageClass).
     * Empty when the object was never set up for replication.
     * @private
     * @param {ObjectMD} objMD - Object metadata.
     * @returns {Array<string>} Site names.
     */
    _getObjectSites(objMD) {
        const storageClass = objMD.getReplicationInfo()
            && objMD.getReplicationStorageClass();
        if (!storageClass) {
            return [];
        }
        return storageClass.split(',')
            .map(site => site.split(':preferred_read')[0]);
    }

    /**
     * Checks that the site is a known replication destination: in the bucket rules
     * (StorageClass) or in the object replication info (storageClass).
     * This make sure we don't create a backend no replication processor handles to avoid the
     * object staying PENDING forever.
     *
     * | Bucket rule           | Object sites | SITE_NAME | Result                                     |
     * |-----------------------|--------------|-----------|--------------------------------------------|
     * | StorageClass=lab-9512 | none         | lab-9512  | processed                                  |
     * | StorageClass=lab-9512 | none         | foo       | skipped (known sites = [lab-9512])         |
     * | no StorageClass       | lab-9512     | lab-9512  | processed                                  |
     * | no StorageClass       | lab-9512     | foo       | skipped (known sites = [lab-9512])         |
     * | no StorageClass       | none         | lab-9512  | processed                                  |
     * | no StorageClass       | none         | foo       | processed: nothing to compare, not caught  |
     * | no StorageClass       | none         | unset     | error "missing SITE_NAME" (_markPending)   |
     *
     * NOTE: if no StorageClass, S3C will use the default replication endpoint from 
     * the federation config (env_replication_endpoints).
     * @private
     * @param {ObjectMD} objMD - Object metadata.
     * @param {string} site - Destination site name.
     * @param {Array<string>} configuredSites - Sites set in the bucket replication rules.
     * @returns {boolean} True if the site is known, or nothing to compare against.
     */
    _isKnownSite(objMD, site, configuredSites) {
        const knownSites = configuredSites.concat(this._getObjectSites(objMD));
        return knownSites.length === 0 || knownSites.includes(site);
    }

    /**
     * Marks an object as pending for replication.
     * @private
     * @param {string} bucket - The bucket name.
     * @param {string} key - The object key.
     * @param {string} versionId - The object version ID.
     * @param {string} storageClass - The storage class for replication.
     * @param {Object} repConfig - The replication configuration.
     * @param {Array<string>} configuredSites - Sites set in the bucket replication rules.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _markObjectPending(
        bucket,
        key,
        versionId,
        storageClass,
        repConfig,
        configuredSites,
        cb,
    ) {
        let objMD;
        let skip = false;
        return waterfall([
            // get object blob
            next => this.cloudserverclient.getMetadata({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }, next),
            (mdRes, next) => {
                const originalMD = JSON.parse(mdRes.Body);
                const originalMDVersion = originalMD['md-model-version'];
                objMD = new ObjectMD(originalMD);
                const newMDVersion = objMD.getModelVersion();

                // Prevent schema downgrade: do not write metadata if this model version
                // is older than the object's original version, to avoid losing newer fields.
                if (newMDVersion < originalMDVersion) {
                    this.log.error('model version regression: newMDVersion < originalMDVersion', {
                        bucket,
                        key,
                        versionId,
                        newMDVersion,
                        originalMDVersion,
                    });
                    return next(new Error('model version regression: refusing to overwrite newer metadata'));
                }

                if (!this._objectShouldBeUpdated(objMD, storageClass)) {
                    skip = true;
                    return process.nextTick(next);
                }
                // The site must be in the bucket rules (StorageClass) or already in the
                // object replication info. Without StorageClass (S3C default endpoint),
                // only the object replication info knows the site.
                if (!this.allowNewSite
                    && !this._isKnownSite(objMD, storageClass, configuredSites)) {
                    // one unknown site is enough: SITE_NAME is wrong for this bucket,
                    // stop it instead of scanning (and logging) every object
                    if (!this._bucketStopped) {
                        this._bucketStopped = true;
                        this.log.error('unknown replication site, stopping bucket. '
                            + 'Check SITE_NAME, or set ALLOW_NEW_SITE=true to add a new destination', {
                            bucket,
                            key,
                            versionId,
                            site: storageClass,
                            bucketSites: configuredSites,
                            objectSites: this._getObjectSites(objMD),
                        });
                    }
                    skip = true;
                    return process.nextTick(next);
                }
                // Initialize replication info, if missing
                // This is particularly important if the object was created before
                // enabling replication on the bucket.
                let replicationInfo = objMD.getReplicationInfo();
                const { Rules, Role } = repConfig;
                const destination = Rules[0].Destination.Bucket;
                
                if (!replicationInfo || !replicationInfo.status) {
                    // set replication properties
                    const ops = objMD.getContentLength() === 0 ? ['METADATA']
                        : ['METADATA', 'DATA'];
                    replicationInfo = {
                        status: 'PENDING',
                        content: ops,
                        backends: [],
                        destination,
                        storageClass: '',
                        role: Role,
                        storageType: '',
                    };
                    objMD.setReplicationInfo(replicationInfo);
                }

                // Force reset object's replication configuration to match bucket's configuration
                if (this.forceUsingConfiguration) {
                    objMD.setReplicationTargetBucket(destination);
                    objMD.setReplicationRoles(Role);
                }
                // Update replication info with site specific info
                if (!objMD.getReplicationSiteStatus(storageClass)) {
                    // When replicating to multiple destinations,
                    // the storageClass and storageType properties
                    // become comma-separated lists of the storage
                    // classes and types of the replication destinations.
                    const storageClasses = objMD.getReplicationStorageClass()
                        ? `${objMD.getReplicationStorageClass()},${storageClass}` : storageClass;
                    objMD.setReplicationStorageClass(storageClasses);
                    if (this.storageType) {
                        const storageTypes = objMD.getReplicationStorageType()
                            ? `${objMD.getReplicationStorageType()},${this.storageType}` : this.storageType;
                        objMD.setReplicationStorageType(storageTypes);
                    }
                    // Add site to the list of replication backends
                    const backends = objMD.getReplicationBackends();
                    backends.push({
                        site: storageClass,
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    });
                    objMD.setReplicationBackends(backends);
                }

                objMD.setReplicationSiteStatus(storageClass, 'PENDING');
                objMD.setReplicationStatus('PENDING');
                objMD.updateMicroVersionId();
                const md = objMD.getSerialized();
                return this.cloudserverclient.putMetadata({
                    Bucket: bucket,
                    Key: key,
                    VersionId: versionId,
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
        this.s3.send(new ListObjectVersionsCommand({
            Bucket: bucket,
            MaxKeys: this.listingLimit,
            Prefix: this.targetPrefix,
            VersionIdMarker,
            KeyMarker,
        }))
            .then(data => cb(null, data))
            .catch(cb);
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
        waterfall([
            async () => {
                try {
                    const res = await this.s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
                    return res.ReplicationConfiguration;
                } catch (err) {
                    this.log.error('error getting bucket replication', { error: err });
                    throw err;
                }
            },
            (repConfig, next) => {
                const { Rules } = repConfig;
                const storageClass = this.siteName || Rules[0].Destination.StorageClass;
                if (!storageClass) {
                    const errMsg = 'missing SITE_NAME environment variable, must be set to'
                        + ' the value of "site" property in the CRR configuration';
                    this.log.error(errMsg);
                    return next(new Error(errMsg));
                }
                if (!this.siteName) {
                    this.log.warn(`missing SITE_NAME environment variable, triggering replication to the ${storageClass} storage class`);
                }
                const configuredSites = this._getConfiguredSites(Rules);
                return eachLimit(versions, this.workers, (i, apply) => {
                    const { Key, VersionId, IsLatest } = i;
                    if (this._bucketStopped) {
                        // bucket stopped on an unknown site: don't start new objects
                        apply();
                        return;
                    }
                    if (this.currentVersionOnly && !IsLatest) {
                        ++this._nSkipped;
                        apply();
                        return;
                    }
                    this._markObjectPending(bucket, Key, VersionId, storageClass, repConfig, configuredSites, apply);
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
        this._bucketStopped = false;
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
                    const versions = (data.Versions || []).concat(data.DeleteMarkers || []);
                    return this._markPending(bucket, versions, err => {
                        if (err) {
                            return done(err);
                        }
                        this._VersionIdMarker = data.NextVersionIdMarker;
                        this._KeyMarker = data.NextKeyMarker;
                        return done();
                    });
                },
            ),
            async () => {
                if (this._bucketStopped) {
                    return false;
                }
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
                if (this._bucketStopped) {
                    this._stoppedBuckets.push(bucket);
                    this.log.error(`stopped task for bucket: ${bucket}, unknown replication site`);
                } else {
                    this.log.info(`completed task for bucket: ${bucket}`);
                }
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
            if (this._stoppedBuckets.length > 0) {
                this.log.error('buckets stopped on an unknown replication site, check SITE_NAME '
                    + 'or set ALLOW_NEW_SITE=true', {
                    site: this.siteName,
                    buckets: this._stoppedBuckets,
                });
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
