const { promisify } = require('util');
const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');
const {
    PutObjectCommand,
    DeleteObjectCommand,
    GetBucketReplicationCommand,
    DeleteBucketReplicationCommand,
    PutBucketReplicationCommand,
} = require('@aws-sdk/client-s3');
const ReplicationStatusUpdater = require('../../CRR/ReplicationStatusUpdater');
const ReplicationSiteRemover = require('../../CRR/ReplicationSiteRemover');
const CloudserverClient = require('../../Clients/CloudserverClient');

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
} = require('../utils/S3Setup');

const log = new Logger('crrSiteValidation:test');
// default replication site of the cloudserver under test (workbench: "sf")
const DEFAULT_SITE = process.env.CRR_DEFAULT_SITE || 'sf';
const WRONG_SITE = 'wrong-site';
// account setup/cleanup creates and deletes IAM roles and policies: slow on a remote cluster
const HOOK_TIMEOUT_MS = 30000;

describe('crrExistingObjects site validation and removeReplicationSite', () => {
    let vaultClient;
    let accountSource;
    let accountDest;
    let endpoint;
    let cloudserverClient;

    beforeEach(async () => {
        endpoint = `http://${s3Host}:${s3Port}`;
        vaultClient = new vaultclient.Client(
            iamHost, iamPort, false, undefined, undefined, undefined, undefined,
            adminAccessKeyId, adminSecretAccessKey,
        );
        accountSource = await createTestAccount(vaultClient);
        accountDest = await createTestAccount(vaultClient);
        // no StorageClass: objects get the default replication site
        await configureCrr(accountSource, accountDest);
        cloudserverClient = new CloudserverClient(
            endpoint, accountSource.accountAccessKey, accountSource.accountSecretKey);
    }, HOOK_TIMEOUT_MS);

    afterEach(async () => {
        await removeCrrConfiguration(accountSource);
        await removeCrrConfiguration(accountDest);
        await deleteTestAccount(vaultClient, accountSource);
        await deleteTestAccount(vaultClient, accountDest);
    }, HOOK_TIMEOUT_MS);

    async function putObject(key) {
        const res = await accountSource.s3Client.send(new PutObjectCommand({
            Bucket: accountSource.bucketName, Key: key, Body: `data of ${key}`,
        }));
        return res.VersionId;
    }

    async function getReplicationInfo(key, versionId) {
        const res = await promisify(cloudserverClient.getMetadata.bind(cloudserverClient))({
            Bucket: accountSource.bucketName, Key: key, VersionId: versionId,
        });
        return JSON.parse(res.Body).replicationInfo;
    }

    function sitesOf(replicationInfo) {
        return replicationInfo.backends.map(b => b.site);
    }

    async function runCrrExistingObjects(params) {
        const updater = new ReplicationStatusUpdater({
            buckets: [accountSource.bucketName],
            // all statuses: objects may already be processed by backbeat
            replicationStatusToProcess: ['NEW', 'PENDING', 'COMPLETED', 'FAILED'],
            workers: 10,
            accessKey: accountSource.accountAccessKey,
            secretKey: accountSource.accountSecretKey,
            endpoint,
            storageType: '',
            listingLimit: 1000,
            ...params,
        }, log);
        await promisify(updater.run.bind(updater))();
        return updater;
    }

    async function runRemover(params) {
        const remover = new ReplicationSiteRemover({
            buckets: [accountSource.bucketName],
            siteToRemove: WRONG_SITE,
            accessKey: accountSource.accountAccessKey,
            secretKey: accountSource.accountSecretKey,
            endpoint,
            ...params,
        }, log);
        return remover.run();
    }

    it('should skip objects when SITE_NAME is not a known site', async () => {
        const versionId = await putObject('obj-1');
        expect(sitesOf(await getReplicationInfo('obj-1', versionId))).toEqual([DEFAULT_SITE]);

        const updater = await runCrrExistingObjects({ siteName: WRONG_SITE });

        expect(updater._nSkipped).toBe(1);
        expect(updater._nUpdated).toBe(0);
        expect(updater._stoppedBuckets).toEqual([accountSource.bucketName]);
        expect(sitesOf(await getReplicationInfo('obj-1', versionId))).toEqual([DEFAULT_SITE]);
    }, 60000);

    it('should process objects when SITE_NAME is the object site', async () => {
        const versionId = await putObject('obj-1');

        const updater = await runCrrExistingObjects({ siteName: DEFAULT_SITE });

        expect(updater._nUpdated).toBe(1);
        const repInfo = await getReplicationInfo('obj-1', versionId);
        expect(sitesOf(repInfo)).toEqual([DEFAULT_SITE]);
        expect(repInfo.status).toBe('PENDING');
    }, 60000);

    it('should add an unknown site with ALLOW_NEW_SITE, then removeReplicationSite cleans it', async () => {
        const versionId = await putObject('obj-1');

        // reproduce the bug: object now has a backend nobody processes
        await runCrrExistingObjects({ siteName: WRONG_SITE, allowNewSite: true });
        let repInfo = await getReplicationInfo('obj-1', versionId);
        expect(sitesOf(repInfo)).toEqual([DEFAULT_SITE, WRONG_SITE]);
        expect(repInfo.storageClass).toBe(`${DEFAULT_SITE},${WRONG_SITE}`);

        const stats = await runRemover({ dryRun: false });

        expect(stats).toEqual(expect.objectContaining({ updated: 1, errors: 0 }));
        repInfo = await getReplicationInfo('obj-1', versionId);
        expect(sitesOf(repInfo)).toEqual([DEFAULT_SITE]);
        expect(repInfo.storageClass).toBe(DEFAULT_SITE);
        // status comes from the remaining site only
        expect(repInfo.status).toBe(ReplicationSiteRemover.getGlobalStatus(repInfo.backends));
    }, 60000);

    it('should clean delete markers', async () => {
        await putObject('obj-1');
        const res = await accountSource.s3Client.send(new DeleteObjectCommand({
            Bucket: accountSource.bucketName, Key: 'obj-1',
        }));
        const markerVersionId = res.VersionId;

        await runCrrExistingObjects({ siteName: WRONG_SITE, allowNewSite: true });
        expect(sitesOf(await getReplicationInfo('obj-1', markerVersionId))).toContain(WRONG_SITE);

        const stats = await runRemover({ dryRun: false });

        // object version + delete marker
        expect(stats).toEqual(expect.objectContaining({ updated: 2, errors: 0 }));
        expect(sitesOf(await getReplicationInfo('obj-1', markerVersionId))).toEqual([DEFAULT_SITE]);
    }, 60000);

    it('should reset an object whose only site is the wrong one, then process it with the right site', async () => {
        // object written before replication was enabled -> no replication info
        const { ReplicationConfiguration } = await accountSource.s3Client.send(
            new GetBucketReplicationCommand({ Bucket: accountSource.bucketName }));
        await accountSource.s3Client.send(
            new DeleteBucketReplicationCommand({ Bucket: accountSource.bucketName }));
        const versionId = await putObject('obj-1');
        await accountSource.s3Client.send(new PutBucketReplicationCommand({
            Bucket: accountSource.bucketName, ReplicationConfiguration,
        }));
        expect(sitesOf(await getReplicationInfo('obj-1', versionId))).toEqual([]);

        // rule without StorageClass + object never replicated: nothing to compare,
        // the wrong site is accepted and becomes the only site
        await runCrrExistingObjects({ siteName: WRONG_SITE });
        expect(sitesOf(await getReplicationInfo('obj-1', versionId))).toEqual([WRONG_SITE]);

        const stats = await runRemover({ dryRun: false });

        expect(stats).toEqual(expect.objectContaining({ updated: 1, reset: 1, errors: 0 }));
        let repInfo = await getReplicationInfo('obj-1', versionId);
        expect(repInfo.status).toBe('');
        expect(repInfo.backends).toEqual([]);
        expect(repInfo.storageClass).toBe('');

        // back to the first-time CRR case: the right site is processed
        const updater = await runCrrExistingObjects({ siteName: DEFAULT_SITE });
        expect(updater._nUpdated).toBe(1);
        repInfo = await getReplicationInfo('obj-1', versionId);
        expect(sitesOf(repInfo)).toEqual([DEFAULT_SITE]);
    }, 60000);

    it('should not write anything in dry run', async () => {
        const versionId = await putObject('obj-1');
        await runCrrExistingObjects({ siteName: WRONG_SITE, allowNewSite: true });

        const stats = await runRemover({});

        expect(stats).toEqual(expect.objectContaining({ updated: 1, errors: 0 }));
        expect(sitesOf(await getReplicationInfo('obj-1', versionId))).toEqual([DEFAULT_SITE, WRONG_SITE]);
    }, 60000);
});
