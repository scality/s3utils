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
     * The site is not enough: since S3C 10.1, two bucket replication rules can send to the
     * same site with two buckets. Example: site sf, bucket d1, and site sf, bucket d2.
     * A backend written by S3C 9.5 has no bucket: its bucket is the top-level destination
     * of the object. A backend of a public cloud site has no bucket, and the object has no
     * top-level destination: there, the site is enough.
     * We don't compare the role. A bucket has one owner on a site, so we don't need the
     * role to tell two backends apart. And we need a backend to match after the role
     * changed: then we send the object again with the new role, because backbeat keeps
     * refusing the old one (BadRole).
     * Example: the bucket Role had a typo, and the customer fixed it. The object backend is
     * site sf, bucket d1, role typo-role. The rule now gives role replication-role. We
     * still find the backend.
     * @private
     * @param {Object} existing - Backend of the object.
     * @param {Object} candidate - Backend built from the bucket rules.
     * @param {Object} [replicationInfo] - Replication info of the object.
     * @returns {boolean} True if both are the same.
     */
    _isSameBackend(existing, candidate, replicationInfo) {
        // With the SITE_NAME fallback (step 3 of _markObjectPending), the candidate is the
        // object backend itself. So it is the same, even with no bucket to compare (S3C 9.5).
        if (existing === candidate) {
            return true;
        }
        if (existing.site !== candidate.site) {
            return false;
        }
        if (!existing.destination) {
            return !replicationInfo?.destination
                || candidate.destination === replicationInfo.destination;
        }
        return existing.destination === candidate.destination;
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
     * When the object already replicates, the site must be one of its sites or one of
     * the rule sites. Otherwise we accept it: a rule without StorageClass uses the S3C
     * default site from the federation config (env_replication_endpoints), and this
     * script can't read it.
     * We don't need a check for "every rule names its site": then we only write a site
     * that a rule names (_markObjectPending, step 2), or a site the object already has
     * (step 3). Both are known.
     *
     * "names X": a rule has StorageClass=X.
     *
     * We only check the objects we write (_markObjectPending, step 6). The table below
     * is for TARGET_REPLICATION_STATUS=NEW. With FAILED, COMPLETED or PENDING, we only
     * write an object that already has a backend for SITE_NAME, so the check passes.
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
     * Adapts AWS SDK replication config to arsenal's ReplicationConfigurationMetadata shape.
     * A bucket replication rule without StorageClass sends the objects to the S3C default
     * site. That site is in the federation config (env_replication_endpoints), and we
     * can't read it from here. So we use SITE_NAME as the site of these rules. Without a
     * site, resolveBackends ignores the rule.
     * @private
     * @param {Object} repConfig - The full replication configuration.
     * @returns {Object}
     */
    _buildArsenalConfig(repConfig) {
        const { Role, Rules } = repConfig;
        // When a rule names SITE_NAME in its StorageClass, we assume the default site is
        // another site, and the rules without StorageClass get no site. Otherwise we would
        // send their objects to the wrong site.
        // Example: r1 has no StorageClass (bucket dr-bucket, default site dr), r2 uses site
        // sf, bucket sf-bucket. With SITE_NAME=sf, r1 is left out: we only requeue site sf,
        // bucket sf-bucket. With SITE_NAME=dr, r1 gets site dr.
        // If the default site is SITE_NAME too, we get it wrong: r1 is left out without a
        // word. Workaround: set StorageClass sf on r1 for the run, then put the bucket
        // replication config back.
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
     * Marks an object as pending for replication.
     *
     * We write the same format as cloudserver since S3C 10.1: each backend has its own
     * destination bucket and role. Cloudserver does this for every bucket, V1 rules
     * (Prefix) or V2 rules (Filter), so we do the same.
     *
     * "The rules" in the comments below means the replication rules of the bucket
     * (GetBucketReplication).
     *
     * _markPending only calls this for keys that match a rule prefix. If a rule has no
     * StorageClass, it also checks that SITE_NAME is set.
     *
     * Example used in the comments below:
     *   bucket Role: '...:role/crr-src,...:role/crr-dst'
     *   bucket rules (V1 or V2, same result):
     *     r1  Prefix ''       Destination { Bucket: 'arn:aws:s3:::backup-sf',  StorageClass: 'sf' }
     *     r2  Prefix 'docs/'  Destination { Bucket: 'arn:aws:s3:::backup-dr2', StorageClass: 'dr2' }
     *   TARGET_REPLICATION_STATUS=NEW, SITE_NAME not set
     *   object 'docs/a.pdf', already replicated to sf:
     *     replicationInfo: { status: 'COMPLETED', role: '...:role/crr-src',
     *       content: ['DATA', 'METADATA'], isNFS: null,
     *       backends: [{ site: 'sf', status: 'COMPLETED', dataStoreVersionId: '',
     *                    destination: 'arn:aws:s3:::backup-sf', role: '...:role/crr-dst' }] }
     *   result: we leave sf as it is and add dr2 as PENDING. Backbeat copies the object
     *   to site dr2, bucket backup-dr2, and nowhere else.
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
            // 1. We read the object backends: where the object was already sent, and how it
            //    went. We need them because TARGET_REPLICATION_STATUS is about the object: what
            //    already happened to it for each site and bucket. The bucket replication rules
            //    don't know that.
            //    Example: [site sf, bucket backup-sf, COMPLETED]
            const prev = objMD.getReplicationInfo();
            const existingBackends = prev?.backends || [];

            // 2. We build the candidates: where the bucket replication rules send this key
            //    today (site, bucket and role). We use resolveBackends, like cloudserver on a
            //    PUT. We need them for every status, not only NEW: a requeue must also use the
            //    current bucket and role, not the ones the object was sent with (S3UTILS-208).
            //    The role of a candidate is the destination role of the bucket Role, with the
            //    account of Destination.Account when the rule has one.
            //    resolveBackends sets every candidate to PENDING: that's not a decision yet,
            //    step 5 picks the ones we really send again.
            //    Example: two candidates, one per rule that matches the key:
            //      site sf,  bucket backup-sf,  role ...crr-dst
            //      site dr2, bucket backup-dr2, role ...crr-dst
            const arsenalConfig = this._buildArsenalConfig(repConfig);
            let candidateBackends = ReplicationConfiguration.resolveBackends(
                arsenalConfig, key, () => false, existingBackends,
            );

            // 3. When SITE_NAME is set, we keep only the candidates for that site. We need it
            //    to requeue one site without touching the others. Example: resend everything
            //    to dr2 after dr2 lost its data.
            //    Example: SITE_NAME=dr2 keeps [site dr2, bucket backup-dr2].
            if (this.siteName) {
                candidateBackends = candidateBackends.filter(b => b.site === this.siteName);
                if (candidateBackends.length === 0) {
                    // No rule sends this key to SITE_NAME anymore, but the object may still
                    // have a backend for it. We need this to retry old replications to a site
                    // the rules moved away from: we take that backend as it is, with its own
                    // bucket and role.
                    // Example: the rules now only name dr3. With SITE_NAME=sf, we take the
                    // object backend for site sf.
                    candidateBackends = existingBackends.filter(b => b.site === this.siteName);
                }
            }

            // 4. For each candidate, we find the object backend for the same site and bucket
            //    (see _isSameBackend), and so its status. We need it to apply
            //    TARGET_REPLICATION_STATUS.
            //    No backend means the object was never sent to this site and bucket: NEW picks
            //    it. That's also the case when a rule moved to another bucket on the same site.
            //    We match on the site and the bucket, not on the role. A bucket has one owner
            //    on a site, so we don't need the role to tell two backends apart. And the role
            //    can change after the object was sent: we still need to find its backend, so
            //    we can send the object again with the new role.
            //    Example: site sf, bucket backup-sf has an object backend, COMPLETED. Site dr2,
            //    bucket backup-dr2 has no backend.
            //    Example: say the backend for site sf, bucket backup-sf has role ...crr-typo,
            //    because the bucket Role had a typo back then. The rule now gives role
            //    ...crr-dst. We still find the backend, and PENDING picks it.
            //
            // 5. TARGET_REPLICATION_STATUS picks the candidates to send again.
            //    Example: NEW picks dr2. COMPLETED would pick sf.
            const pairs = candidateBackends.map(candidate => {
                const existing = existingBackends.find(e => this._isSameBackend(e, candidate, prev));
                const selected = this._statusShouldBeProcessed(existing?.status);
                return { candidate, existing, selected };
            });

            // Nothing picked: we write nothing, and the object counts as skipped.
            if (!pairs.some(p => p.selected)) {
                return { skip: true };
            }

            // 6. A SITE_NAME that is in neither the bucket replication rules nor the object is
            //    probably a typo. We stop before writing anything: a backend for a site that
            //    no backbeat process handles stays PENDING forever (S3UTILS-250).
            //    We check only the objects we write: an object we skip gets no backend, so a
            //    wrong SITE_NAME can't hurt it. Otherwise a FAILED retry run would stop on an
            //    object that has nothing to retry.
            //    Example: SITE_NAME is not set, so there is nothing to check.
            //    Example: say r1 has no StorageClass, and SITE_NAME=sff. r1 gets site sff
            //    (see _buildArsenalConfig), and NEW picks site sff, bucket backup-sf. The
            //    object only knows site sf: we stop the run.
            if (this._stopOnUnknownSite(objMD, bucket, key, versionId, repConfig.Rules)) {
                return { skip: true };
            }

            // 7. We build the backends to write. Only what we picked changes: this run must not
            //    touch the rest of the object.
            //    We keep the object backends in their order and add the new ones at the end.
            //    Backbeat finds a backend written by S3C 9.5 by its site only, so it takes the
            //    first backend of that site: it must stay before a new one for the same site.
            //    Example: [site sf, bucket backup-sf, COMPLETED (as it was);
            //              site dr2, bucket backup-dr2, PENDING]
            const allBackends = existingBackends.map(existing => {
                const pair = pairs.find(p => p.existing === existing);
                // Picked: send it again, to the site and bucket of the rule, with the role
                // of the rule. Not picked, or no rule for it: leave it as it is.
                return pair?.selected ? { ...pair.candidate, status: 'PENDING' } : existing;
            });
            for (const { candidate, existing, selected } of pairs) {
                // Picked and not on the object yet: add it at the end.
                if (selected && !existing) {
                    allBackends.push({ ...candidate, status: 'PENDING' });
                }
            }

            // 8. We build the replication info to write.
            //    Role: backbeat sends a backend with '<source role of this role>,<backend role>'.
            //    So we only write the source role, from the bucket config, like cloudserver on
            //    a PUT.
            //    But a backend written by S3C 9.5 has no bucket and no role: backbeat takes
            //    them from the top-level destination and role. While we keep such a backend, we
            //    keep the destination and the destination role of the object. S3C 9.5 allowed
            //    one bucket and one Role per bucket config, so all these backends share them.
            //    The source role still comes from the bucket config: backbeat uses it for the
            //    new backends too, and an old one would make them fail (BadRole).
            //    Example: { status: 'PENDING', role: '...crr-src', backends: [sf, dr2],
            //               content: ['DATA', 'METADATA'], isNFS: null }
            //    Example: say sf is a backend written by S3C 9.5, and the object role is
            //    '...old-src,...old-dst'. We write role '...crr-src,...old-dst'.
            const keepsOldBackend = allBackends.some(b => !b.role);
            const sourceRole = ReplicationConfiguration.resolveSourceRole(repConfig.Role);
            // Undefined when the object role has no destination role. Example: a public
            // cloud site in S3C 9.5 has a single role. Then we write the source role alone.
            const oldDestinationRole = keepsOldBackend ? prev?.role?.split(',')[1] : undefined;
            const replicationInfo = {
                // PENDING: backbeat only picks up objects that are PENDING. Backbeat sets
                // PROCESSING itself later, when one site is done and others aren't.
                status: 'PENDING',
                role: oldDestinationRole ? `${sourceRole},${oldDestinationRole}` : sourceRole,
                backends: allBackends,
                content: this._getReplicationContent(prev, objMD),
                isNFS: prev?.isNFS ?? null,
            };
            if (keepsOldBackend && prev?.destination) {
                replicationInfo.destination = prev.destination;
            }
            // A replica that we send further (cascading) is still a replica.
            if (prev?.isReplica) {
                replicationInfo.isReplica = true;
            }

            // 9. We save it. _updateObjectMD gives the object a new microVersionId: we need it so
            //    the destination accepts this update as newer than what it has.
            objMD.setReplicationInfo(replicationInfo);
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

                // A bucket replication rule without StorageClass sends the objects to the S3C
                // default site, which we can't read from here: SITE_NAME must give it (see
                // _buildArsenalConfig). Without it, we stop with "missing SITE_NAME".
                const hasRuleWithoutSite = Rules.some(r =>
                    r.Status === 'Enabled' && !r.Destination.StorageClass
                );
                if (!this.siteName && hasRuleWithoutSite) {
                    const errMsg = 'missing SITE_NAME environment variable, must be set to'
                        + ' the value of "site" property in the CRR configuration';
                    this.log.error(errMsg);
                    return next(new Error(errMsg));
                }
                // We warn when no rule sends objects to SITE_NAME: no rule names it, and no rule
                // uses the default site. Then we only requeue the objects that already have a
                // backend for SITE_NAME, and skip the others. The warning tells the user why few
                // or no objects are updated. We say it once per bucket: _markPending runs for
                // each listing page.
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
