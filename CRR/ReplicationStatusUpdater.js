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
     * @param {string} [params.targetPrefix] - (Optional) Prefix to target for replication.
     * @param {number} [params.listingLimit] - (Optional) Limit for listing objects.
     * @param {number} [params.maxUpdates] - (Optional) Maximum number of updates to perform.
     * @param {number} [params.maxScanned] - (Optional) Maximum number of items to scan.
     * @param {string} [params.keyMarker] - (Optional) Key marker for resuming object listing.
     * @param {string} [params.versionIdMarker] - (Optional) Version ID marker for resuming object listing.
     * @param {boolean} [params.currentVersionOnly] - (Optional) Whether to process only the current version of objects.
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
            targetPrefix,
            listingLimit,
            maxUpdates,
            maxScanned,
            keyMarker,
            versionIdMarker,
            currentVersionOnly,
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
        this.targetPrefix = targetPrefix;
        this.listingLimit = listingLimit;
        this.maxUpdates = maxUpdates;
        this.maxScanned = maxScanned;
        this.inputKeyMarker = keyMarker;
        this.inputVersionIdMarker = versionIdMarker;
        this.currentVersionOnly = currentVersionOnly;
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
     * Tells if TARGET_REPLICATION_STATUS picks a backend with this status.
     * @private
     * @param {string} [status] - Status of the object backend. Undefined when the object
     * was never copied to this site and bucket, and that's what NEW picks.
     * @returns {boolean} True if we must copy the object to this site and bucket again.
     */
    _statusShouldBeProcessed(status) {
        return this.replicationStatusToProcess.some(filter =>
            (filter === 'NEW' ? !status : status === filter));
    }

    /**
     * Tells if an object backend and a candidate are the same: same site and same bucket.
     * Since S3C 10.1, one site can have two buckets. A backend with no bucket is a cloud
     * site written by S3C 10.1: the site is enough (backends written by S3C 9.5 got their
     * bucket in _upgradeBackends).
     * We don't compare the role: a bucket has one owner on a site, and a backend must
     * still match after the bucket Role changed, so we send it again with the new role.
     * Example: the object backend is site sf, bucket d1, role typo-role. The rule now gives
     * role replication-role. We still find the backend.
     * @private
     * @param {Object} existing - Backend of the object.
     * @param {Object} candidate - Backend built from the bucket rules.
     * @returns {boolean} True if both are the same.
     */
    _isSameBackend(existing, candidate) {
        return existing.site === candidate.site
            && (!existing.destination || existing.destination === candidate.destination);
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
     * A site that no backbeat process handles leaves the object PENDING forever.
     * When the object already replicates, the site must be one of its sites or a rule
     * site. Otherwise we accept it: a rule without StorageClass uses the S3C default
     * site, and we can't read it.
     * We only check the objects we write. The table is for TARGET_REPLICATION_STATUS=NEW:
     * with the other statuses, we only write objects that already have a backend for
     * SITE_NAME. "names X" means a rule has StorageClass=X.
     *
     * | Rules                  | Object sites     | SITE_NAME | Result                                 |
     * |------------------------|------------------|-----------|----------------------------------------|
     * | no StorageClass        | none             | any value | processed: can't check                 |
     * | all name lab-9512      | none             | lab-9512  | processed                              |
     * | all name lab-9512      | none             | foo       | skipped: no rule gives site foo        |
     * | mixed (one without)    | none             | any value | processed: can't check                 |
     * | no StorageClass        | lab-9512         | lab-9512  | processed: requeue to its own site     |
     * | no StorageClass        | lab-9512         | foo       | run stopped                            |
     * | one names lab-94       | lab-9512         | lab-94    | processed: site from the rule          |
     * | one names lab-94       | lab-9512         | lab-9512  | processed: requeue to its own site     |
     * | one names lab-94       | lab-9512         | foo       | skipped: no rule gives site foo        |
     * | one names foo          | lab-9512         | foo       | processed: site from the rule          |
     * | all name a site        | none             | unset     | processed: every rule site (_markPending) |
     * | no StorageClass        | none             | unset     | error "missing SITE_NAME" (_markPending) |
     * | no StorageClass        | 9.5 replica (dr) | primary   | run stopped: needs ALLOW_NEW_SITE=true |
     *
     * ALLOW_NEW_SITE=true skips the check, for a DR failback of replicas written by
     * S3C 9.5, or a renamed default DR site.
     * @private
     * @param {ObjectMD} objMD - Object metadata.
     * @param {string} site - Destination site name.
     * @param {Array<Object>} rules - Bucket replication rules.
     * @returns {boolean} True if the site is known, or nothing to compare against.
     */
    _isKnownSite(objMD, site, rules) {
        const ruleSites = this._getConfiguredSites(rules);
        const objectSites = this._getObjectSites(objMD);
        return objectSites.length === 0 || ruleSites.concat(objectSites).includes(site);
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
     * Adapts the AWS SDK replication config to the shape arsenal expects.
     * A rule without StorageClass goes to the S3C default site, set in the federation
     * config (env_replication_endpoints), and we can't read it. So we give it SITE_NAME as
     * site. Without a site, resolveBackends ignores the rule.
     * @private
     * @param {Object} repConfig - The full replication configuration.
     * @returns {Object}
     */
    _buildArsenalConfig(repConfig) {
        const { Role, Rules } = repConfig;
        // When another rule names SITE_NAME, we assume the default site is another site, and
        // the rules without StorageClass get no site. If the default site is SITE_NAME too,
        // they are left out (workaround in the README, SITE_NAME).
        // Example: r1 has no StorageClass, r2 names sf. With SITE_NAME=sf, r1 is left out.
        // With SITE_NAME=dr, r1 gets site dr.
        const isRuleSite = this._getConfiguredSites(Rules).includes(this.siteName);
        const defaultSite = isRuleSite ? undefined : this.siteName;
        return {
            role: Role,
            destination: Rules[0]?.Destination.Bucket ?? '',
            rules: Rules.map(r => ({
                enabled: r.Status === 'Enabled',
                prefix: r.Filter?.Prefix ?? r.Prefix ?? '',
                storageClass: r.Destination.StorageClass || defaultSite,
                destination: r.Destination.Bucket,
                account: r.Destination.Account,
                priority: r.Priority,
                id: r.ID ?? '',
            })),
        };
    }

    /**
     * Stops the run when we don't know SITE_NAME (see _isKnownSite), unless
     * ALLOW_NEW_SITE is set. We stop the whole run, not only this object: if SITE_NAME is
     * wrong here, it's probably wrong for every bucket.
     * This function only decides for one object: it skips it, and sets _unknownSite. The
     * callers stop the run. _markPending doesn't start new objects once _unknownSite is
     * set, and the listing stops at this page and keeps its markers as the resume point.
     * Objects already started still finish: we write the ones with a known site, and we
     * skip the ones with an unknown site here.
     * @private
     * @param {ObjectMD} objMD - Object metadata.
     * @param {string} bucket - The bucket name.
     * @param {string} key - The object key.
     * @param {string} versionId - The object version ID.
     * @param {Array<Object>} rules - Bucket replication rules.
     * @returns {boolean} True if the run is stopped and the object must be skipped.
     */
    _stopOnUnknownSite(objMD, bucket, key, versionId, rules) {
        if (!this.siteName || this.allowNewSite || this._isKnownSite(objMD, this.siteName, rules)) {
            return false;
        }
        if (!this._unknownSite) {
            this._unknownSite = { bucket, key, versionId };
            this.log.error('unknown replication site, run stopped. SITE_NAME is not a site '
                + 'of this object or of the bucket rules. Expected for the S3C default site '
                + '(rules without StorageClass), e.g. DR failback or renamed DR site. '
                + 'Objects processed before this point may already have this SITE_NAME: '
                + 'if SITE_NAME is wrong, run removeReplicationSite (dry run) with '
                + `SITE_TO_REMOVE=${this.siteName} on these buckets to find and remove it. `
                + 'If it reset objects, re-run with the right SITE_NAME from the start of '
                + 'this run (same bucket list and markers): the reset objects are before '
                + 'the resume point. Otherwise re-run from the resume point with the right '
                + 'SITE_NAME, or with ALLOW_NEW_SITE=true', {
                bucket,
                key,
                versionId,
                site: this.siteName,
                ruleSites: this._getConfiguredSites(rules),
                objectSites: this._getObjectSites(objMD),
            });
        }
        return true;
    }

    /**
     * Tells backbeat what to send: the metadata, and the data if the object has some.
     * An object put before the bucket had a replication rule has an empty list (content []).
     * Then we compute it from the object size: with an empty list, backbeat would first
     * send the metadata only.
     * @private
     * @param {Object} [replicationInfo] - The current object replication info.
     * @param {ObjectMD} objMD - Object metadata.
     * @returns {Array<string>} 'METADATA', and 'DATA' for an object with data.
     */
    _getReplicationContent(replicationInfo, objMD) {
        if (replicationInfo?.content?.length > 0) {
            return replicationInfo.content;
        }
        if (objMD.getContentLength() === 0) {
            return ['METADATA'];
        }
        return ['METADATA', 'DATA'];
    }

    /**
     * Rewrites the backends written by S3C 9.5 in the S3C 10.1 format, so the object has
     * one format. A backend written by S3C 9.5 has no bucket and no role: backbeat takes
     * them from the top-level destination and role of the object (getReplicationTargetBucket
     * and getReplicationRoles in arsenal). We copy them on the backend, so backbeat uses
     * the same bucket and role, and we never write the top-level destination again.
     * When the object role has no destination role (a public cloud site in S3C 9.5), we
     * copy only the bucket: backbeat then uses the top-level role, as before.
     * A cloud site written by S3C 10.1 has no bucket, and the object has no top-level
     * destination: we leave it as it is.
     * Example: { destination: 'arn:aws:s3:::backup-sf', role: '...old-src,...old-dst',
     *            backends: [{ site: 'sf', status: 'COMPLETED', dataStoreVersionId: '' }] }
     *   gives [{ site: 'sf', status: 'COMPLETED', dataStoreVersionId: '',
     *            destination: 'arn:aws:s3:::backup-sf', role: '...old-dst' }]
     * @private
     * @param {Object} [prev] - The current replication info of the object.
     * @returns {Array<Object>} The object backends, in their order.
     */
    _upgradeBackends(prev) {
        const backends = prev?.backends || [];
        if (!prev?.destination) {
            return backends;
        }
        const destinationRole = prev.role?.split(',')[1];
        return backends.map(b => {
            if (b.destination) {
                return b;
            }
            const upgraded = { ...b, destination: prev.destination };
            if (!b.role && destinationRole) {
                upgraded.role = destinationRole;
            }
            return upgraded;
        });
    }

    /**
     * Builds the plan for one object: where the bucket replication rules send this key
     * today, which object backend matches each destination, and which ones
     * TARGET_REPLICATION_STATUS picks.
     * Example (see _markObjectPending): for 'docs/a.pdf', we get
     *   [site sf,  bucket backup-sf,  object backend COMPLETED, not picked by NEW]
     *   [site dr2, bucket backup-dr2, no object backend,        picked by NEW]
     * @private
     * @param {string} key - The object key.
     * @param {Array<Object>} existingBackends - The object backends, from _upgradeBackends.
     * @param {Object} repConfig - The bucket replication configuration.
     * @returns {Array<Object>} One object target per destination: { candidate, existing,
     * selected }. existing is the matching object backend, if any.
     */
    _buildPlan(key, existingBackends, repConfig) {

        // 1. We take the destinations from the rules, like cloudserver on a PUT: site,
        //    bucket and role. We use them for every status, not only NEW: a requeue must
        //    use the current bucket and role (S3UTILS-208). resolveBackends sets them all
        //    PENDING: step 4 decides which ones we really send.
        let candidates = ReplicationConfiguration.resolveBackends(
            this._buildArsenalConfig(repConfig), key, () => false, existingBackends,
        );

        // 2. When SITE_NAME is set, we keep only that site, to requeue it without touching
        //    the others. When no rule sends the key to SITE_NAME anymore, we take the object
        //    backend for it as it is, to retry an old replication (S3UTILS-250).
        //    Example: SITE_NAME=dr2 keeps [site dr2, bucket backup-dr2].
        if (this.siteName) {
            candidates = candidates.filter(b => b.site === this.siteName);
            if (candidates.length === 0) {
                candidates = existingBackends.filter(b => b.site === this.siteName);
            }
        }

        // 3. For each destination, we find the object backend for the same site and bucket
        //    (see _isSameBackend). No backend means the object was never sent there.
        // 4. TARGET_REPLICATION_STATUS picks on the status of that backend.
        //    Example: NEW picks dr2, which has no backend. COMPLETED would pick sf.
        return candidates.map(candidate => {
            const existing = existingBackends.find(e => this._isSameBackend(e, candidate));
            const selected = this._statusShouldBeProcessed(existing?.status);
            return { candidate, existing, selected };
        });
    }

    /**
     * Builds the replication info to write from the plan. Only what the plan picks
     * changes: this run must not touch the rest of the object (see the table in
     * _markObjectPending).
     * @private
     * @param {Object} [prev] - The current replication info of the object.
     * @param {Array<Object>} existingBackends - The object backends, from _upgradeBackends.
     * @param {Array<Object>} plan - The object targets from _buildPlan.
     * @param {Object} repConfig - The bucket replication configuration.
     * @param {ObjectMD} objMD - Object metadata.
     * @returns {Object} The replication info to write.
     */
    _buildReplicationInfo(prev, existingBackends, plan, repConfig, objMD) {
        // We keep the object backends in their order, and add the new ones at the end.
        // Example: [site sf, bucket backup-sf, COMPLETED (as it was);
        //           site dr2, bucket backup-dr2, PENDING]
        const backends = existingBackends.map(existing => {
            const target = plan.find(t => t.existing === existing);
            return target?.selected ? { ...target.candidate, status: 'PENDING' } : existing;
        });
        for (const { candidate, existing, selected } of plan) {
            if (selected && !existing) {
                backends.push({ ...candidate, status: 'PENDING' });
            }
        }

        // Backbeat sends a backend with '<source role>,<backend role>', so we write only the
        // source role, from the bucket config, like cloudserver on a PUT.
        const replicationInfo = {
            // Backbeat only queues objects that are PENDING. It sets PROCESSING itself.
            status: 'PENDING',
            role: ReplicationConfiguration.resolveSourceRole(repConfig.Role),
            backends,
            content: this._getReplicationContent(prev, objMD),
            isNFS: prev?.isNFS ?? null,
        };
        // A replica that we send further (cascading) is still a replica.
        if (prev?.isReplica) {
            replicationInfo.isReplica = true;
        }
        return replicationInfo;
    }

    /**
     * Marks an object as pending for replication.
     *
     * We write the format of S3C 10.1 (and Zenko 2.16), like cloudserver on a PUT: each
     * backend has its own destination bucket and role, whatever the rule format (V1 Prefix
     * or V2 Filter). _markPending only calls this for keys that match a rule prefix.
     *
     * 1. _upgradeBackends: the object backends, in the S3C 10.1 format when we can.
     * 2. _buildPlan: where the rules send the key, and what TARGET_REPLICATION_STATUS picks.
     * 3. Nothing picked: we write nothing.
     * 4. _stopOnUnknownSite: we stop the run on a SITE_NAME we don't know.
     * 5. _buildReplicationInfo: what we write.
     *
     * What happens to each object backend:
     *
     * | Object backend               | Matched on              | Picked               | Not picked          |
     * |------------------------------|-------------------------|----------------------|---------------------|
     * | none for this destination    | -                       | added at the end     | -                   |
     * | S3C 10.1: site, bucket, role | site + bucket (the role | rewritten in place   | kept as it is       |
     * |                              | may have changed)       | with the bucket and  |                     |
     * |                              |                         | role of the rule,    |                     |
     * |                              |                         | PENDING              |                     |
     * | S3C 9.5: site only           | site + bucket, copied   | same                 | rewritten with the  |
     * |                              | from the top level      |                      | top-level bucket    |
     * |                              | (_upgradeBackends)      |                      | and role            |
     * | cloud, S3C 10.1: site only   | site                    | same                 | kept as it is       |
     * | no rule for it anymore       | itself (SITE_NAME only) | PENDING in place     | kept as it is       |
     *
     * Top level: status PENDING, source role from the bucket config, content computed when
     * empty (object put before the rule), isReplica kept.
     *
     * Example used in the comments:
     *   bucket Role: '...:role/crr-src,...:role/crr-dst'
     *   bucket rules (V1 or V2, same result):
     *     r1  Prefix ''       Destination { Bucket: 'arn:aws:s3:::backup-sf',  StorageClass: 'sf' }
     *     r2  Prefix 'docs/'  Destination { Bucket: 'arn:aws:s3:::backup-dr2', StorageClass: 'dr2' }
     *   TARGET_REPLICATION_STATUS=NEW, SITE_NAME not set
     *   object 'docs/a.pdf', replicated to site sf, bucket backup-sf, COMPLETED
     *   result: we leave sf as it is and add site dr2, bucket backup-dr2, PENDING.
     *
     * @private
     * @param {string} bucket - The bucket name.
     * @param {string} key - The object key.
     * @param {string} versionId - The object version ID.
     * @param {Object} repConfig - The full replication configuration.
     * @param {Function} cb - Callback function.
     * @returns {void}
     */
    _markObjectPending(bucket, key, versionId, repConfig, cb) {
        this._updateObjectMD(bucket, key, versionId, objMD => {
            const prev = objMD.getReplicationInfo();
            const existingBackends = this._upgradeBackends(prev);
            const plan = this._buildPlan(key, existingBackends, repConfig);
            if (!plan.some(t => t.selected)) {
                return { skip: true };
            }
            // We check only the objects we write: an object we skip gets no backend, so a
            // wrong SITE_NAME can't hurt it.
            if (this._stopOnUnknownSite(objMD, bucket, key, versionId, repConfig.Rules)) {
                return { skip: true };
            }
            // _updateObjectMD then gives the object a new microVersionId, so the destination
            // accepts this update as newer than what it has.
            objMD.setReplicationInfo(
                this._buildReplicationInfo(prev, existingBackends, plan, repConfig, objMD));
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

                // A rule without StorageClass needs SITE_NAME as its site (see
                // _buildArsenalConfig).
                const hasRuleWithoutSite = Rules.some(r =>
                    r.Status === 'Enabled' && !r.Destination.StorageClass
                );
                if (!this.siteName && hasRuleWithoutSite) {
                    const errMsg = 'missing SITE_NAME environment variable, must be set to'
                        + ' the value of "site" property in the CRR configuration';
                    this.log.error(errMsg);
                    return next(new Error(errMsg));
                }
                // When no rule sends objects to SITE_NAME, we only requeue the objects that
                // already have a backend for it. We warn once per bucket, so the user knows why
                // few objects are updated.
                // Example: the rules name sf and dr2, and SITE_NAME=dr3.
                const ruleSites = this._getConfiguredSites(Rules);
                if (this.siteName && !hasRuleWithoutSite && !ruleSites.includes(this.siteName)
                    && this._siteWarnedBucket !== bucket) {
                    this._siteWarnedBucket = bucket;
                    this.log.warn('SITE_NAME is not a site of the bucket rules: only objects that '
                        + 'already replicate to it are requeued, the other objects are skipped', {
                        bucket,
                        site: this.siteName,
                        ruleSites,
                    });
                }
                return eachLimit(versions, this.workers, (i, apply) => {
                    const { Key, VersionId, IsLatest } = i;
                    if (this._unknownSite) {
                        // The run stopped on an unknown site: we don't start new objects.
                        apply();
                        return;
                    }
                    if (this.currentVersionOnly && !IsLatest) {
                        ++this._nSkipped;
                        apply();
                        return;
                    }
                    // We skip a key that no enabled rule covers: backbeat refuses it anyway
                    // ("replication disabled for object").
                    const hasMatchingRule = Rules.some(r =>
                        r.Status === 'Enabled' &&
                        Key.startsWith(r.Filter?.Prefix ?? r.Prefix ?? '')
                    );
                    if (!hasMatchingRule) {
                        ++this._nSkipped;
                        apply();
                        return;
                    }
                    this._markObjectPending(bucket, Key, VersionId, repConfig, apply);
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
