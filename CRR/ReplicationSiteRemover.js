const crypto = require('crypto');
const { promisify } = require('util');
const { eachLimit } = require('async');
const { ObjectMD } = require('arsenal').models;
const { ListObjectVersionsCommand, GetBucketReplicationCommand } = require('@aws-sdk/client-s3');
const CloudserverClient = require('../Clients/CloudserverClient');
const createS3Client = require('../Clients/s3Client');

const LOG_PROGRESS_INTERVAL_MS = 10000;

/**
 * Returns the replication info of an object never set up for replication.
 * Taken from arsenal ObjectMD defaults, so it follows the installed arsenal format.
 * @returns {Object} Empty replication info (fresh copy).
 */
function getEmptyReplicationInfo() {
    return new ObjectMD().getReplicationInfo();
}

/**
 * Computes the global replication status from the remaining sites.
 * A REPLICA backend means the object is a replica on this site. Its other
 * backends are a frozen copy of the source (BB-875), so it stays REPLICA.
 * On S3C 9.5 the REPLICA status is the only thing that keeps backbeat from
 * replicating it again.
 * Otherwise same order as backbeat: FAILED, then PENDING, then COMPLETED.
 * One difference: a PENDING site gives PENDING, not PROCESSING, so the object
 * is requeued for that site.
 * @param {Array<Object>} backends - Remaining replication backends.
 * @returns {string} Global replication status.
 */
function getGlobalStatus(backends) {
    const statuses = backends.map(b => b.status);
    if (statuses.includes('REPLICA')) {
        return 'REPLICA';
    }
    if (statuses.includes('FAILED')) {
        return 'FAILED';
    }
    if (statuses.includes('PENDING')) {
        return 'PENDING';
    }
    return 'COMPLETED';
}

/**
 * Removes the site from the comma-separated site list.
 * Also removes the item at the same position in the aligned list (storageType)
 * when both lists have the same length.
 * @param {string} storageClass - Comma-separated site list.
 * @param {string} storageType - Comma-separated list aligned with the site list.
 * @param {string} site - Site to remove.
 * @returns {Object} Updated storageClass and storageType.
 */
function removeSiteFromLists(storageClass, storageType, site) {
    const sites = storageClass ? storageClass.split(',') : [];
    const types = storageType ? storageType.split(',') : [];
    const idx = sites.findIndex(s => s.split(':preferred_read')[0] === site);
    if (idx === -1) {
        return { storageClass, storageType };
    }
    sites.splice(idx, 1);
    if (types.length === sites.length + 1) {
        types.splice(idx, 1);
    }
    return { storageClass: sites.join(','), storageType: types.join(',') };
}

/**
 * Returns the sites set in the StorageClass of the bucket replication rules.
 * @param {Array<Object>} rules - Bucket replication rules.
 * @returns {Array<string>} Site names.
 */
function getRuleSites(rules) {
    return (rules || [])
        .map(rule => rule.Destination && rule.Destination.StorageClass)
        .filter(Boolean)
        .flatMap(storageClass => storageClass.split(','))
        .map(site => site.split(':preferred_read')[0]);
}

class ReplicationSiteRemover {
    /**
     * @param {Object} params - Configuration.
     * @param {Array<string>} params.buckets - Buckets to process.
     * @param {string} params.siteToRemove - Replication site to remove from object metadata.
     * @param {string} params.accessKey - Access key.
     * @param {string} params.secretKey - Secret key.
     * @param {string} params.endpoint - S3 endpoint.
     * @param {boolean} [params.dryRun=true] - Log what would change, write nothing.
     * @param {number} [params.workers=10] - Parallel metadata updates.
     * @param {number} [params.listingLimit=1000] - Listing page size.
     * @param {string} [params.targetPrefix] - Only process keys with this prefix.
     * @param {number} [params.maxUpdates] - Stop after this many updates (resume with the markers).
     * @param {number} [params.maxScanned] - Stop after this many scanned versions (resume with the markers).
     * @param {string} [params.keyMarker] - Resume listing from this key (first bucket only).
     * @param {string} [params.versionIdMarker] - Resume listing from this version (first bucket only).
     * @param {boolean} [params.forceRemoveConfiguredSite=false] - Remove the site even if a bucket
     * replication rule names it in StorageClass.
     * @param {Object} log - Logger.
     */
    constructor(params, log) {
        this.buckets = params.buckets;
        this.siteToRemove = params.siteToRemove;
        this.dryRun = params.dryRun !== false;
        this.workers = params.workers || 10;
        this.listingLimit = params.listingLimit || 1000;
        this.targetPrefix = params.targetPrefix;
        this.maxUpdates = params.maxUpdates;
        this.maxScanned = params.maxScanned;
        this.inputKeyMarker = params.keyMarker;
        this.inputVersionIdMarker = params.versionIdMarker;
        this.forceRemoveConfiguredSite = params.forceRemoveConfiguredSite === true;
        this.log = log;

        // Random replicationGroupId (7 chars) and instanceId (6 chars) for
        // microVersionId generation, as in ReplicationStatusUpdater: this tool
        // bypasses CloudServer's configured values.
        this.replicationGroupId = crypto.randomBytes(4).toString('hex').slice(0, 7);
        this.instanceId = crypto.randomBytes(3).toString('hex').slice(0, 6);

        this.s3 = createS3Client({
            accessKey: params.accessKey,
            secretKey: params.secretKey,
            endpoint: params.endpoint,
        }, log);
        this.cloudserverClient = new CloudserverClient(params.endpoint, params.accessKey, params.secretKey);

        this.stats = {
            scanned: 0, updated: 0, reset: 0, skipped: 0, errors: 0,
        };
        this._bucketInProgress = null;
        this._keyMarker = null;
        this._versionIdMarker = null;
    }

    _logProgress(message = 'progress update') {
        this.log.info(message, {
            dryRun: this.dryRun,
            site: this.siteToRemove,
            bucket: this._bucketInProgress,
            keyMarker: this._keyMarker,
            versionIdMarker: this._versionIdMarker,
            ...this.stats,
        });
    }

    /**
     * Returns the limit reached (MAX_UPDATES or MAX_SCANNED), if any.
     * Checked at the end of each listing page.
     * @returns {string|null} Limit name and value, or null.
     */
    _limitReached() {
        if (this.maxUpdates !== undefined && this.stats.updated >= this.maxUpdates) {
            return `MAX_UPDATES (${this.maxUpdates})`;
        }
        if (this.maxScanned !== undefined && this.stats.scanned >= this.maxScanned) {
            return `MAX_SCANNED (${this.maxScanned})`;
        }
        return null;
    }

    async _getMetadata(params) {
        return promisify(this.cloudserverClient.getMetadata.bind(this.cloudserverClient))(params);
    }

    async _putMetadata(params) {
        return promisify(this.cloudserverClient.putMetadata.bind(this.cloudserverClient))(params);
    }

    /**
     * Removes the site from one object version. Never throws: errors are
     * counted and logged, so one bad version does not stop the bucket.
     * @param {string} bucket - Bucket name.
     * @param {string} key - Object key.
     * @param {string} versionId - Version ID.
     * @returns {Promise<undefined>} Resolves when done.
     */
    async _processVersion(bucket, key, versionId) {
        const params = { Bucket: bucket, Key: key, VersionId: versionId };
        const logFields = { bucket, key, versionId };
        this.stats.scanned += 1;
        try {
            const mdRes = await this._getMetadata(params);
            const originalMD = JSON.parse(mdRes.Body);
            const objMD = new ObjectMD(originalMD);
            if (objMD.getModelVersion() < originalMD['md-model-version']) {
                this.stats.errors += 1;
                this.log.error('model version regression, refusing to overwrite newer metadata', logFields);
                return;
            }
            const repInfo = objMD.getReplicationInfo();
            const backends = (repInfo && repInfo.backends) || [];
            if (!backends.some(b => b.site === this.siteToRemove)) {
                this.stats.skipped += 1;
                return;
            }
            const remaining = backends.filter(b => b.site !== this.siteToRemove);
            const { storageClass, storageType } = removeSiteFromLists(
                objMD.getReplicationStorageClass(), objMD.getReplicationStorageType(), this.siteToRemove);
            const reset = remaining.length === 0;
            const newStatus = reset ? '' : getGlobalStatus(remaining);
            this.log.info(this.dryRun ? 'would update object (dry run)' : 'updating object', {
                ...logFields,
                action: reset ? 'reset' : 'remove-site',
                isDeleteMarker: objMD.getIsDeleteMarker(),
                oldStatus: repInfo.status,
                newStatus,
                oldStorageClass: objMD.getReplicationStorageClass(),
                newStorageClass: storageClass,
            });
            if (!this.dryRun) {
                if (reset) {
                    // the site was the only destination: back to the never-replicated state,
                    // so a crrExistingObjects run with the right SITE_NAME picks the object up
                    objMD.setReplicationInfo({
                        ...getEmptyReplicationInfo(),
                        isNFS: repInfo.isNFS,
                    });
                    // a replica stays a replica (S3C 10.1: isReplica). setReplicationInfo
                    // only keeps known fields, so set it back after.
                    if (repInfo.isReplica !== undefined) {
                        objMD.getReplicationInfo().isReplica = repInfo.isReplica;
                    }
                } else {
                    objMD.setReplicationBackends(remaining)
                        .setReplicationStatus(newStatus);
                    // deprecated fields, still present on metadata in the old single-destination
                    // format (S3C 9.5): keep them in line with the backends
                    const ri = objMD.getReplicationInfo();
                    ri.storageClass = storageClass;
                    ri.storageType = storageType;
                }
                objMD.updateMicroVersionId(this.instanceId, this.replicationGroupId);
                await this._putMetadata({ ...params, Body: objMD.getSerialized() });
            }
            this.stats.updated += 1;
            if (reset) {
                this.stats.reset += 1;
            }
        } catch (err) {
            this.stats.errors += 1;
            this.log.error('error updating object', { ...logFields, error: err.message });
        }
    }

    /**
     * Refuses to remove a site that a bucket replication rule names in StorageClass:
     * that site is a real destination. Removing it would reset objects that replicate
     * to it, and a real run can't be undone.
     * A bucket without replication rules has nothing to protect.
     * @param {string} bucket - Bucket name.
     * @returns {Promise<undefined>} Resolves if the site can be removed, rejects otherwise.
     */
    async _checkSiteNotInRules(bucket) {
        if (this.forceRemoveConfiguredSite) {
            return;
        }
        let rules;
        try {
            const res = await this.s3.send(new GetBucketReplicationCommand({ Bucket: bucket }));
            rules = res.ReplicationConfiguration.Rules;
        } catch (err) {
            if (err.name === 'ReplicationConfigurationNotFoundError') {
                return;
            }
            throw err;
        }
        const ruleSites = getRuleSites(rules);
        if (ruleSites.includes(this.siteToRemove)) {
            const message = `SITE_TO_REMOVE "${this.siteToRemove}" is a destination of bucket ${bucket} `
                + '(StorageClass of its replication rule). Removing it would reset objects that '
                + 'replicate to it. Check SITE_TO_REMOVE, or set FORCE_REMOVE_CONFIGURED_SITE=true';
            this.log.error(message, { bucket, site: this.siteToRemove, ruleSites });
            throw new Error(message);
        }
    }

    /**
     * Processes one bucket, page by page.
     * @param {string} bucket - Bucket name.
     * @returns {Promise<boolean>} True if the bucket was fully processed,
     * false if stopped by MAX_UPDATES or MAX_SCANNED.
     */
    async _processBucket(bucket) {
        this._bucketInProgress = bucket;
        await this._checkSiteNotInRules(bucket);
        this._keyMarker = this.inputKeyMarker || null;
        this._versionIdMarker = this.inputVersionIdMarker || null;
        // markers only apply to the first bucket processed
        this.inputKeyMarker = undefined;
        this.inputVersionIdMarker = undefined;
        this.log.info('starting bucket', {
            bucket, site: this.siteToRemove, dryRun: this.dryRun,
            keyMarker: this._keyMarker, versionIdMarker: this._versionIdMarker,
        });
        do {
            const data = await this.s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
                MaxKeys: this.listingLimit,
                Prefix: this.targetPrefix,
                KeyMarker: this._keyMarker || undefined,
                VersionIdMarker: this._versionIdMarker || undefined,
            }));
            const versions = (data.Versions || []).concat(data.DeleteMarkers || []);
            await eachLimit(versions, this.workers,
                async v => this._processVersion(bucket, v.Key, v.VersionId));
            this._keyMarker = data.NextKeyMarker || null;
            this._versionIdMarker = data.NextVersionIdMarker || null;
            if (this._limitReached()) {
                return !(this._keyMarker || this._versionIdMarker);
            }
        } while (this._keyMarker || this._versionIdMarker);
        this._logProgress('completed bucket');
        return true;
    }

    /**
     * Runs on all buckets, in order.
     * Stops on a listing error, or when MAX_UPDATES or MAX_SCANNED is reached: then
     * it logs the bucket list and markers to resume.
     * @returns {Promise<Object>} Final stats.
     */
    async run() {
        const progressInterval = setInterval(() => this._logProgress(), LOG_PROGRESS_INTERVAL_MS);
        try {
            for (let i = 0; i < this.buckets.length; i++) {
                const bucket = this.buckets[i].trim();
                const bucketDone = await this._processBucket(bucket);
                const limit = this._limitReached();
                if (limit) {
                    const remaining = this.buckets.slice(bucketDone ? i + 1 : i);
                    if (remaining.length > 0) {
                        let message = `reached ${limit}, resume by re-running `
                            + `with the bucket list "${remaining.join(',')}"`;
                        if (!bucketDone) {
                            message += ` and KEY_MARKER=${this._keyMarker} VERSION_ID_MARKER=${this._versionIdMarker}`;
                        }
                        this.log.info(message);
                    }
                    break;
                }
            }
            this._bucketInProgress = null;
            this._logProgress('completed task');
            return this.stats;
        } catch (err) {
            // log what was already changed before the stop
            this._logProgress('stopped on error');
            throw err;
        } finally {
            clearInterval(progressInterval);
        }
    }
}

module.exports = ReplicationSiteRemover;
module.exports.getGlobalStatus = getGlobalStatus;
module.exports.removeSiteFromLists = removeSiteFromLists;
module.exports.getEmptyReplicationInfo = getEmptyReplicationInfo;
module.exports.getRuleSites = getRuleSites;
