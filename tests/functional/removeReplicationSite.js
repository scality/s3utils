const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');
const {
    DeleteObjectCommand,
    GetBucketReplicationCommand,
    PutBucketReplicationCommand,
} = require('@aws-sdk/client-s3');
const ReplicationSiteRemover = require('../../CRR/ReplicationSiteRemover');

const {
    iamHost,
    iamPort,
    s3Host,
    s3Port,
    adminAccessKeyId,
    adminSecretAccessKey,
    createTestAccount,
    deleteTestAccount,
    configureCrr,
    removeCrrConfiguration,
    putObject,
    getReplicationInfo,
    setReplicationInfo,
} = require('../utils/S3Setup');

const log = new Logger('removeReplicationSite:test');
// default replication site of the cloudserver under test (workbench: "sf")
const DEFAULT_SITE = process.env.CRR_DEFAULT_SITE || 'sf';
const WRONG_SITE = 'wrong-site';

/**
 * Same metadata as a crrExistingObjects run with SITE_NAME=wrong-site:
 * a PENDING backend appended for a site nobody processes.
 * @param {Object} replicationInfo - current replication info
 * @returns {Object} broken replication info
 */
function withWrongSite(replicationInfo) {
    return {
        ...replicationInfo,
        status: 'PENDING',
        backends: replicationInfo.backends.concat({ site: WRONG_SITE, status: 'PENDING', dataStoreVersionId: '' }),
        storageClass: [replicationInfo.storageClass, WRONG_SITE].filter(Boolean).join(','),
    };
}

function sitesOf(replicationInfo) {
    return replicationInfo.backends.map(b => b.site);
}

function runRemover(account, siteToRemove = WRONG_SITE) {
    const remover = new ReplicationSiteRemover({
        buckets: [account.bucketName],
        siteToRemove,
        accessKey: account.accountAccessKey,
        secretKey: account.accountSecretKey,
        endpoint: `http://${s3Host}:${s3Port}`,
        dryRun: false,
    }, log);
    return remover.run();
}

describe('removeReplicationSite', () => {
    let vaultClient;
    let accountSource;
    let accountDest;

    beforeEach(async () => {
        vaultClient = new vaultclient.Client(
            iamHost, iamPort, false, undefined, undefined, undefined, undefined,
            adminAccessKeyId, adminSecretAccessKey,
        );
        accountSource = await createTestAccount(vaultClient);
        accountDest = await createTestAccount(vaultClient);
        // no StorageClass: objects get the default replication site
        await configureCrr(accountSource, accountDest);
    });

    afterEach(async () => {
        await removeCrrConfiguration(accountSource);
        await removeCrrConfiguration(accountDest);
        await deleteTestAccount(vaultClient, accountSource);
        await deleteTestAccount(vaultClient, accountDest);
    });

    it('should remove the wrong site and recompute the status from the remaining site', async () => {
        const versionId = await putObject(accountSource, 'obj-1');
        const repInfo = await getReplicationInfo(accountSource, 'obj-1', versionId);
        await setReplicationInfo(accountSource, 'obj-1', versionId, withWrongSite(repInfo));

        const stats = await runRemover(accountSource);

        expect(stats).toEqual(expect.objectContaining({ updated: 1, reset: 0, errors: 0 }));
        const cleaned = await getReplicationInfo(accountSource, 'obj-1', versionId);
        expect(sitesOf(cleaned)).toEqual([DEFAULT_SITE]);
        expect(cleaned.storageClass).toBe(DEFAULT_SITE);
        expect(cleaned.status).toBe(ReplicationSiteRemover.getGlobalStatus(cleaned.backends));
    }, 60000);

    it('should clean delete markers', async () => {
        const versionId = await putObject(accountSource, 'obj-1');
        const { VersionId: markerVersionId } = await accountSource.s3Client.send(new DeleteObjectCommand({
            Bucket: accountSource.bucketName, Key: 'obj-1',
        }));
        for (const vId of [versionId, markerVersionId]) {
            const repInfo = await getReplicationInfo(accountSource, 'obj-1', vId);
            await setReplicationInfo(accountSource, 'obj-1', vId, withWrongSite(repInfo));
        }

        const stats = await runRemover(accountSource);

        expect(stats).toEqual(expect.objectContaining({ updated: 2, errors: 0 }));
        expect(sitesOf(await getReplicationInfo(accountSource, 'obj-1', markerVersionId))).toEqual([DEFAULT_SITE]);
    }, 60000);

    it('should keep a replica as REPLICA', async () => {
        const versionId = await putObject(accountSource, 'obj-1');
        const repInfo = await getReplicationInfo(accountSource, 'obj-1', versionId);
        // S3C 9.5 replica of a multi-destination source (frozen PENDING site),
        // then a run with SITE_NAME=wrong-site
        await setReplicationInfo(accountSource, 'obj-1', versionId, {
            ...repInfo,
            status: 'PENDING',
            backends: [
                { site: DEFAULT_SITE, status: 'PENDING', dataStoreVersionId: '' },
                { site: 'primary', status: 'REPLICA', dataStoreVersionId: '' },
                { site: WRONG_SITE, status: 'PENDING', dataStoreVersionId: '' },
            ],
            storageClass: `${DEFAULT_SITE},primary,${WRONG_SITE}`,
        });

        const stats = await runRemover(accountSource);

        expect(stats).toEqual(expect.objectContaining({ updated: 1, errors: 0 }));
        const cleaned = await getReplicationInfo(accountSource, 'obj-1', versionId);
        expect(cleaned.status).toBe('REPLICA');
        expect(sitesOf(cleaned)).toEqual([DEFAULT_SITE, 'primary']);
    }, 60000);

    it('should reset the replication info when the wrong site is the only one', async () => {
        const versionId = await putObject(accountSource, 'obj-1');
        const repInfo = await getReplicationInfo(accountSource, 'obj-1', versionId);
        // object never replicated, then crrExistingObjects with SITE_NAME=wrong-site
        await setReplicationInfo(accountSource, 'obj-1', versionId, {
            ...repInfo,
            status: 'PENDING',
            backends: [{ site: WRONG_SITE, status: 'PENDING', dataStoreVersionId: '' }],
            storageClass: WRONG_SITE,
        });

        const stats = await runRemover(accountSource);

        expect(stats).toEqual(expect.objectContaining({ updated: 1, reset: 1, errors: 0 }));
        const cleaned = await getReplicationInfo(accountSource, 'obj-1', versionId);
        expect(cleaned.status).toBe('');
        expect(cleaned.backends).toEqual([]);
        // empty: '' with arsenal 8.2, absent with arsenal 8.5 (new format)
        expect(cleaned.storageClass || '').toBe('');
    }, 60000);
    it('should refuse to remove a site named in the bucket rule', async () => {
        const versionId = await putObject(accountSource, 'obj-1');
        // name the real site in the rule StorageClass
        const { ReplicationConfiguration } = await accountSource.s3Client.send(
            new GetBucketReplicationCommand({ Bucket: accountSource.bucketName }));
        ReplicationConfiguration.Rules[0].Destination.StorageClass = DEFAULT_SITE;
        await accountSource.s3Client.send(new PutBucketReplicationCommand({
            Bucket: accountSource.bucketName, ReplicationConfiguration,
        }));

        await expect(runRemover(accountSource, DEFAULT_SITE))
            .rejects.toThrow(`SITE_TO_REMOVE "${DEFAULT_SITE}" is a destination`);

        expect(sitesOf(await getReplicationInfo(accountSource, 'obj-1', versionId))).toEqual([DEFAULT_SITE]);
    }, 60000);
});
