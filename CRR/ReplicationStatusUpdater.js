const crypto = require('crypto');
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

        // Random replicationGroupId (7 chars) and instanceId (6 chars) for
        // microVersionId generation: this tool bypasses CloudServer's
        // configured values, so per-instance random tokens prevent
        // collisions with concurrent writers. Sizes match LENGTH_RG and
        // LENGTH_ID in arsenal's VersionID module.
        this.replicationGroupId = crypto.randomBytes(4).toString('hex').slice(0, 7);
        this.instanceId = crypto.randomBytes(3).toString('hex').slice(0, 6);

        this._setupClients();

        this.logProgressInterval = setInterval(this._logProgress.bind(this), LOG_PROGRESS_INTERVAL_MS);

        // intenal state
        this._nProcessed = 0;
        this._nSkipped = 0;
        this._nUpdated = 0;
        this._nErrors = 0;
        this._bucketInProgress = null;
        // object that stopped the run on an unknown site, if any
        this._unknownSite = null;
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
        const info = objMD.getReplicationInfo();
        const siteStatus = info && objMD.getReplicationSiteStatus({ site });
        return this.replicationStatusToProcess.some(filter =>
            filter === 'NEW' ? (!info || !siteStatus) : (info && siteStatus === filter));
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
     * Returns the sites in the object replication info: backend sites plus
     * the top-level storageClass.
     * Newer arsenal (multi-destination CRR) deprecates storageClass, so new objects
     * may not have it. Objects written before 2018 have no backends. Reading both
     * covers every format.
     * Empty when the object was never set up for replication.
     * @private
     * @param {ObjectMD} objMD - Object metadata.
     * @returns {Array<string>} Site names.
     */
    _getObjectSites(objMD) {
        const repInfo = objMD.getReplicationInfo();
        if (!repInfo) {
            return [];
        }
        const backendSites = (repInfo.backends || []).map(backend => backend.site);
        const storageClassSites = repInfo.storageClass
            ? repInfo.storageClass.split(',').map(site => site.split(':preferred_read')[0])
            : [];
        return backendSites.concat(storageClassSites);
    }

    /**
     * Checks that the site is a known replication destination.
     * An unknown site creates a backend no replication processor handles -> the
     * object stays PENDING forever.
     * If there is something reliable to compare against, the site must be in it:
     * - the object already replicates: its sites plus the rule sites
     * - every rule names its site (StorageClass): the rule sites
     * Otherwise accept. A rule without StorageClass uses the S3C default site from
     * the federation config (env_replication_endpoints). This script can't read it.
     *
     * "names X": a rule has StorageClass=X.
     *
     * | Rules                  | Object sites     | SITE_NAME | Result                                 |
     * |------------------------|------------------|-----------|----------------------------------------|
     * | no StorageClass        | none             | any value | processed: can't check                 |
     * | all name lab-9512      | none             | lab-9512  | processed                              |
     * | all name lab-9512      | none             | foo       | run stopped                            |
     * | mixed (one without)    | none             | any value | processed: can't check                 |
     * | no StorageClass        | lab-9512         | lab-9512  | processed: requeue to its own site     |
     * | no StorageClass        | lab-9512         | foo       | run stopped                            |
     * | one names lab-94       | lab-9512         | lab-94    | processed: site from the rule          |
     * | one names lab-94       | lab-9512         | lab-9512  | processed: requeue to its own site     |
     * | one names lab-94       | lab-9512         | foo       | run stopped                            |
     * | one names foo          | lab-9512         | foo       | processed: site from the rule          |
     * | one names lab-9512     | none             | unset     | processed with lab-9512 (_markPending) |
     * | no StorageClass        | none             | unset     | error "missing SITE_NAME" (_markPending) |
     * | no StorageClass        | 9.5 replica (dr) | primary   | run stopped: needs ALLOW_NEW_SITE=true |
     *
     * ALLOW_NEW_SITE=true skips the check. It's needed when the object must get a
     * site it doesn't know and that is the S3C default site: DR failback with
     * replicas written by S3C 9.5, or a renamed default DR site.
     * @private
     * @param {ObjectMD} objMD - Object metadata.
     * @param {string} site - Destination site name.
     * @param {Array<Object>} rules - Bucket replication rules.
     * @returns {boolean} True if the site is known, or nothing to compare against.
     */
    _isKnownSite(objMD, site, rules) {
        const ruleSites = this._getConfiguredSites(rules);
        const objectSites = this._getObjectSites(objMD);
        const canCheck = objectSites.length > 0
            || rules.every(rule => rule.Destination && rule.Destination.StorageClass);
        return !canCheck || ruleSites.concat(objectSites).includes(site);
    }

    /**
     * Fetches, validates, mutates, and writes back object metadata.
     * @private
     * @param {string} bucket
     * @param {string} key
     * @param {string} versionId
     * @param {Function} mutate - Receives objMD; return { skip: true } to skip the write.
     * @param {Function} cb
     * @returns {void}
     */
    _updateObjectMD(bucket, key, versionId, mutate, cb) {
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

                const result = mutate(objMD);
                if (result?.skip) {
                    skip = true;
                    return process.nextTick(next);
                }

                objMD.updateMicroVersionId(this.instanceId, this.replicationGroupId);
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
    _markObjectPending(bucket, key, versionId, storageClass, repConfig, cb) {
        this._updateObjectMD(bucket, key, versionId, objMD => {
            if (!this._objectShouldBeUpdated(objMD, storageClass)) {
                return { skip: true };
            }
            // unknown site -> write nothing and stop the run: SITE_NAME is likely
            // wrong for every bucket (see _isKnownSite)
            if (!this.allowNewSite
                && !this._isKnownSite(objMD, storageClass, repConfig.Rules)) {
                if (!this._unknownSite) {
                    this._unknownSite = { bucket, key, versionId };
                    this.log.error('unknown replication site, run stopped. SITE_NAME is not a site '
                        + 'of this object or of the bucket rules. Expected for the S3C default site '
                        + '(rules without StorageClass), e.g. DR failback or renamed DR site. '
                        + 'Objects processed before this point may already have this SITE_NAME: '
                        + 'if SITE_NAME is wrong, run removeReplicationSite (dry run) with '
                        + `SITE_TO_REMOVE=${storageClass} on these buckets to find and remove it. `
                        + 'If it reset objects, re-run with the right SITE_NAME from the start of '
                        + 'this run (same bucket list and markers): the reset objects are before '
                        + 'the resume point. Otherwise re-run from the resume point with the right '
                        + 'SITE_NAME, or with ALLOW_NEW_SITE=true', {
                        bucket,
                        key,
                        versionId,
                        site: storageClass,
                        ruleSites: this._getConfiguredSites(repConfig.Rules),
                        objectSites: this._getObjectSites(objMD),
                    });
                }
                return { skip: true };
            }

            let replicationInfo = objMD.getReplicationInfo();
            const { Rules, Role } = repConfig;
            const destination = Rules[0].Destination.Bucket;

            if (!replicationInfo || !replicationInfo.status) {
                const ops = objMD.getContentLength() === 0 ? ['METADATA'] : ['METADATA', 'DATA'];
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

            if (this.forceUsingConfiguration) {
                const ri = objMD.getReplicationInfo();
                ri.destination = destination;
                objMD.setReplicationRoles(Role);
            }

            if (!objMD.getReplicationSiteStatus({ site: storageClass })) {
                const ri = objMD.getReplicationInfo();
                ri.storageClass = ri.storageClass
                    ? `${ri.storageClass},${storageClass}` : storageClass;
                if (this.storageType) {
                    ri.storageType = ri.storageType
                        ? `${ri.storageType},${this.storageType}` : this.storageType;
                }
                const backends = objMD.getReplicationBackends();
                backends.push({ site: storageClass, status: 'PENDING', dataStoreVersionId: '' });
                objMD.setReplicationBackends(backends);
            }

            objMD.setReplicationSiteStatus({ site: storageClass }, 'PENDING');
            objMD.setReplicationStatus('PENDING');
            return { skip: false };
        }, cb);
    }

    /**
     * Adapts AWS SDK replication config to arsenal's ReplicationConfigurationMetadata shape.
     * @private
     * @param {Object} repConfig - The full replication configuration.
     * @returns {Object}
     */
    _buildArsenalConfig(repConfig) {
        const { Role, Rules } = repConfig;
        return {
            role: Role,
            destination: Rules[0]?.Destination.Bucket ?? '',
            rules: Rules.map(r => ({
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
     * @param {Object} repConfig - The full replication configuration.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _markObjectPendingV2(bucket, key, versionId, repConfig, cb) {
        this._updateObjectMD(bucket, key, versionId, objMD => {
            const arsenalConfig = this._buildArsenalConfig(repConfig);
            const prev = objMD.getReplicationInfo();
            const existingBackends = prev?.backends;

            let candidateBackends = ReplicationConfiguration.resolveBackends(
                arsenalConfig, key, () => false, existingBackends,
            );

            if (this.siteName) {
                candidateBackends = candidateBackends.filter(b => b.site === this.siteName);
            }

            const backendsToUpdate = candidateBackends.filter(b =>
                this._objectShouldBeUpdated(objMD, b.site)
            );

            if (backendsToUpdate.length === 0) {
                return { skip: true };
            }

            const sourceRole = ReplicationConfiguration.resolveSourceRole(repConfig.Role);
            const updatedSites = new Set(backendsToUpdate.map(b => b.site));
            // resolveBackends can't match V1-format existing backends (no destination/role),
            // so it resets dataStoreVersionId to '' for those — restore it from the original.
            const candidateSites = new Set(candidateBackends.map(b => b.site));
            const updatedBackends = candidateBackends.map(c => {
                if (updatedSites.has(c.site)) { return c; }
                const orig = existingBackends?.find(e => e.site === c.site);
                return orig
                    ? { ...c, status: orig.status, dataStoreVersionId: orig.dataStoreVersionId ?? c.dataStoreVersionId }
                    : c;
            });
            // Preserve backends for sites not targeted by this run (e.g. when SITE_NAME is set),
            // so they are not silently dropped from the metadata.
            const preservedBackends = (existingBackends || []).filter(b => !candidateSites.has(b.site));
            const allBackends = [...updatedBackends, ...preservedBackends];

            objMD.setReplicationInfo({
                status: this._computeTopLevelStatus(allBackends),
                role: this.forceUsingConfiguration ? sourceRole : (prev?.role || sourceRole),
                backends: allBackends,
                content: prev?.content ?? (objMD.getContentLength() === 0 ? ['METADATA'] : ['METADATA', 'DATA']),
                isNFS: prev?.isNFS ?? null,
            });
            return { skip: false };
        }, cb);
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
                        if (this._unknownSite) {
                            // run stopped on an unknown site: don't start new objects
                            apply();
                            return;
                        }
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
                    const hasMatchingRule = Rules.some(r =>
                        r.Status === 'Enabled' &&
                        (!this.siteName || r.Destination.StorageClass === this.siteName) &&
                        Key.startsWith(r.Filter?.Prefix ?? r.Prefix ?? '')
                    );
                    if (!hasMatchingRule) {
                        ++this._nSkipped;
                        apply();
                        return;
                    }
                    this._markObjectPendingV2(bucket, Key, VersionId, repConfig, apply);
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
                        if (this._unknownSite) {
                            // keep the markers of this page: the resume point
                            return done();
                        }
                        this._VersionIdMarker = data.NextVersionIdMarker;
                        this._KeyMarker = data.NextKeyMarker;
                        return done();
                    });
                },
            ),
            async () => {
                if (this._unknownSite) {
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
                if (this._unknownSite) {
                    let message = 'stopped on an unknown replication site, resume by re-running '
                        + 'the script with the bucket list '
                        + `"${this.buckets.slice(this.buckets.indexOf(bucketName)).join(',')}"`;
                    if (this._VersionIdMarker || this._KeyMarker) {
                        message += ' and the following environment variables set: '
                            + `KEY_MARKER=${this._KeyMarker} `
                            + `VERSION_ID_MARKER=${this._VersionIdMarker}`;
                    }
                    this.log.error(message);
                    cb(new Error('stopped on an unknown replication site'));
                    return;
                }
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
