const {
    doWhilst, eachSeries, eachLimit, waterfall,
} = require('async');
const { ObjectMD, ReplicationConfiguration } = require('arsenal').models;
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
     * Returns true if the replication config uses the V1 format.
     * V2 rules carry a Filter element; V1 rules do not.
     * @private
     * @param {Object} repConfig - The replication configuration from GetBucketReplicationCommand.
     * @returns {boolean}
     */
    _isV1Format(repConfig) {
        return repConfig.Rules.every(r => !r.Filter);
    }

    /**
     * Returns the subset of rules whose prefix matches the given key, deduplicated by site
     * (highest Priority wins when two rules target the same StorageClass).
     * @private
     * @param {string} key - The object key.
     * @param {Array} rules - The replication rules from GetBucketReplicationCommand.
     * @returns {Array}
     */
    _getMatchingRules(key, rules) {
        const siteMap = new Map();
        for (const rule of rules) {
            if (rule.Status !== 'Enabled') { continue; }
            const prefix = rule.Prefix || (rule.Filter && rule.Filter.Prefix) || '';
            if (!key.startsWith(prefix)) { continue; }
            const site = rule.Destination.StorageClass;
            const existing = siteMap.get(site);
            const priority = rule.Priority || 0;
            if (!existing || priority > (existing.Priority || 0)) {
                siteMap.set(site, rule);
            }
        }
        return [...siteMap.values()];
    }

    /**
     * Removes V1-only top-level fields from replicationInfo that have no meaning in V2 format.
     * @private
     * @param {Object} replicationInfo - The replication info object to mutate.
     * @returns {void}
     */
    _removeV1Fields(replicationInfo) {
        // eslint-disable-next-line no-param-reassign
        delete replicationInfo.destination;
        // eslint-disable-next-line no-param-reassign
        delete replicationInfo.storageClass;
        // eslint-disable-next-line no-param-reassign
        delete replicationInfo.storageType;
        // eslint-disable-next-line no-param-reassign
        delete replicationInfo.dataStoreVersionId;
    }

    /**
     * Initializes V2 replication info on objMD if it is missing or has no status.
     * @private
     * @param {ObjectMD} objMD - The object metadata.
     * @param {string} sourceRole - The source-side IAM role ARN.
     * @returns {void}
     */
    _initV2ReplicationInfo(objMD, sourceRole) {
        const replicationInfo = objMD.getReplicationInfo();
        if (!replicationInfo || !replicationInfo.status) {
            const ops = objMD.getContentLength() === 0 ? ['METADATA'] : ['METADATA', 'DATA'];
            objMD.setReplicationInfo({
                status: 'PENDING',
                role: sourceRole,
                backends: [],
                content: ops,
                isNFS: null,
            });
        }
    }

    /**
     * Computes the aggregate top-level replication status from all backends per the V2 rules:
     *   any FAILED → FAILED; else any PENDING → PROCESSING; else COMPLETED.
     * @private
     * @param {Array} backends - The backends array from replicationInfo.
     * @returns {string}
     */
    _computeTopLevelStatus(backends) {
        if (backends.some(b => b.status === 'FAILED')) { return 'FAILED'; }
        if (backends.some(b => b.status === 'PENDING')) { return 'PROCESSING'; }
        return 'COMPLETED';
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
                    || !objMD.getReplicationSiteStatus({ site }));
            }
            return (objMD.getReplicationInfo()
                && objMD.getReplicationSiteStatus({ site }) === filter);
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
                        backends: [],
                        content: ops,
                        destination,
                        storageClass: '',
                        role: Role,
                        storageType: '',
                        dataStoreVersionId: '',
                    };
                    objMD.setReplicationInfo(replicationInfo);
                }

                // Force reset object's replication configuration to match bucket's configuration
                if (this.forceUsingConfiguration) {
                    objMD.getReplicationInfo().destination = destination;
                    objMD.setReplicationRoles(Role);
                }
                // Update replication info with site specific info
                if (!objMD.getReplicationSiteStatus({ site: storageClass })) {
                    // When replicating to multiple destinations,
                    // the storageClass and storageType properties
                    // become comma-separated lists of the storage
                    // classes and types of the replication destinations.
                    const ri = objMD.getReplicationInfo();
                    ri.storageClass = ri.storageClass
                        ? `${ri.storageClass},${storageClass}` : storageClass;
                    if (this.storageType) {
                        ri.storageType = ri.storageType
                            ? `${ri.storageType},${this.storageType}` : this.storageType;
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

                objMD.setReplicationSiteStatus({ site: storageClass }, 'PENDING');
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
     * Adapts matched AWS SDK rules to arsenal's ReplicationConfigurationMetadata shape.
     * @private
     * @param {Array} matchingRules - Rules already matched to this key.
     * @param {Object} repConfig - The full replication configuration.
     * @returns {Object}
     */
    _buildArsenalConfig(matchingRules, repConfig) {
        return {
            role: repConfig.Role,
            destination: matchingRules[0]?.Destination.Bucket ?? '',
            rules: matchingRules.map(r => ({
                enabled: r.Status === 'Enabled',
                prefix: r.Filter?.Prefix ?? r.Prefix ?? '',
                storageClass: r.Destination.StorageClass,
                destination: r.Destination.Bucket,
                account: r.Destination.Account,
                priority: r.Priority,
                id: r.ID ?? '',
            })),
        };
    }

    /**
     * Marks an object as pending for replication using the V2 multi-destination format.
     * Writes per-backend destination and role; strips V1-only top-level storageClass/storageType/destination.
     * @private
     * @param {string} bucket - The bucket name.
     * @param {string} key - The object key.
     * @param {string} versionId - The object version ID.
     * @param {Array} matchingRules - Rules already matched to this key (prefix-filtered, deduped by site).
     * @param {Object} repConfig - The full replication configuration.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _markObjectPendingV2(bucket, key, versionId, matchingRules, repConfig, cb) {
        let objMD;
        let skip = false;
        return waterfall([
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

                if (newMDVersion < originalMDVersion) {
                    this.log.error('model version regression: newMDVersion < originalMDVersion', {
                        bucket, key, versionId, newMDVersion, originalMDVersion,
                    });
                    return next(new Error('model version regression: refusing to overwrite newer metadata'));
                }

                const arsenalConfig = this._buildArsenalConfig(matchingRules, repConfig);

                // Capture existing backends before any initialization (to carry forward dataStoreVersionId)
                const existingBackends = objMD.getReplicationInfo()?.backends;

                const candidateBackends = ReplicationConfiguration.resolveBackends(
                    arsenalConfig, key, () => false, existingBackends,
                );

                const backendsToUpdate = candidateBackends.filter(b =>
                    this._objectShouldBeUpdated(objMD, b.site)
                );

                if (backendsToUpdate.length === 0) {
                    skip = true;
                    return process.nextTick(next);
                }

                const sourceRole = ReplicationConfiguration.resolveSourceRole(repConfig.Role);
                this._initV2ReplicationInfo(objMD, sourceRole);
                const replicationInfo = objMD.getReplicationInfo();
                this._removeV1Fields(replicationInfo);
                if (this.forceUsingConfiguration) {
                    replicationInfo.role = sourceRole;
                }
                objMD.setReplicationInfo(replicationInfo);

                const updatedSites = new Set(backendsToUpdate.map(b => b.site));
                // Use candidateBackends (always V2-shaped) for skipped sites, but restore
                // original status and dataStoreVersionId: resolveBackends forces PENDING on every
                // entry, and can't match V1-format existing backends (missing destination/role),
                // so it resets dataStoreVersionId to ''.
                const skippedBackends = candidateBackends
                    .filter(b => !updatedSites.has(b.site))
                    .map(b => {
                        const orig = (existingBackends ?? []).find(e => e.site === b.site);
                        if (!orig) {
                            return b;
                        }

                        return {
                            ...b,
                            status: orig.status,
                            dataStoreVersionId: orig.dataStoreVersionId ?? b.dataStoreVersionId,
                        };
                    });
                const finalBackends = [...skippedBackends, ...backendsToUpdate];
                objMD.setReplicationBackends(finalBackends);

                objMD.setReplicationStatus(this._computeTopLevelStatus(finalBackends));
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

                if (this._isV1Format(repConfig)) {
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
                    return eachLimit(versions, this.workers, (i, apply) => {
                        const { Key, VersionId, IsLatest } = i;
                        if (this.currentVersionOnly && !IsLatest) {
                            ++this._nSkipped;
                            apply();
                            return;
                        }
                        this._markObjectPending(bucket, Key, VersionId, storageClass, repConfig, apply);
                    }, next);
                }

                return eachLimit(versions, this.workers, (i, apply) => {
                    const { Key, VersionId, IsLatest } = i;
                    if (this.currentVersionOnly && !IsLatest) {
                        ++this._nSkipped;
                        apply();
                        return;
                    }
                    let matchingRules = this._getMatchingRules(Key, Rules);
                    if (this.siteName) {
                        matchingRules = matchingRules.filter(r => r.Destination.StorageClass === this.siteName);
                    }
                    if (matchingRules.length === 0) {
                        ++this._nSkipped;
                        apply();
                        return;
                    }
                    this._markObjectPendingV2(bucket, Key, VersionId, matchingRules, repConfig, apply);
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
