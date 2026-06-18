const { promisify } = require('util');
const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');
const { PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const ReplicationStatusUpdater = require('../../CRR/ReplicationStatusUpdater');

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

const log = new Logger('crrExistingObjects:test');
const SITE_NAME = 'test-site';

function makeCaptureLogger() {
    const captured = [];
    const inner = new Logger('crrExistingObjects:test:capture');
    const wrap = level => (
        (msg, data) => {
            captured.push({ level, message: msg, ...(data || {}) });
            return inner[level](msg, data);
        }
    );
    return {
        captured,
        logger: {
            info: wrap('info'),
            warn: wrap('warn'),
            error: wrap('error'),
            fatal: wrap('fatal'),
            debug: wrap('debug'),
            trace: wrap('trace'),
        },
    };
}

describe('crrExistingObjects', () => {
    let vaultClient;
    let accountSource;
    let accountDest;
    let endpoint;

    beforeEach(async () => {
        endpoint = `http://${s3Host}:${s3Port}`;
        vaultClient = new vaultclient.Client(
            iamHost,
            iamPort,
            false,
            undefined,
            undefined,
            undefined,
            undefined,
            adminAccessKeyId,
            adminSecretAccessKey,
        );
        log.info('Setting up source and destination test accounts');
        accountSource = await createTestAccount(vaultClient);
        accountDest = await createTestAccount(vaultClient);
        log.info(`Source: ${accountSource.accountName}, Dest: ${accountDest.accountName}`);
        await configureCrr(accountSource, accountDest);
    });

    afterEach(async () => {
        log.info('Cleaning up test accounts');
        await removeCrrConfiguration(accountSource);
        await removeCrrConfiguration(accountDest);
        await deleteTestAccount(vaultClient, accountSource);
        await deleteTestAccount(vaultClient, accountDest);
        log.info('Test accounts deleted');
    });

    async function runTest(testObjects) {
        for (const obj of testObjects) {
            await accountSource.s3Client.send(new PutObjectCommand({
                Bucket: accountSource.bucketName,
                Key: obj.Key,
                Body: obj.Body,
            }));
            log.info('Uploaded object', { key: obj.Key });
        }

        const { captured, logger } = makeCaptureLogger();
        const updater = new ReplicationStatusUpdater({
            buckets: [accountSource.bucketName],
            replicationStatusToProcess: ['NEW'],
            workers: 10,
            accessKey: accountSource.accountAccessKey,
            secretKey: accountSource.accountSecretKey,
            endpoint,
            siteName: SITE_NAME,
            storageType: '',
            listingLimit: 1000,
            currentVersionOnly: false,
            forceUsingConfiguration: true,
        }, logger);
        await promisify(updater.run.bind(updater))();

        const objectUpdateErrors = captured.filter(
            entry => entry.level === 'error' && entry.message === 'error updating object',
        );
        expect(objectUpdateErrors).toEqual([]);
        expect(updater._nErrors).toBe(0);
        expect(updater._nUpdated).toBe(testObjects.length);

        for (const obj of testObjects) {
            const head = await accountSource.s3Client.send(new HeadObjectCommand({
                Bucket: accountSource.bucketName,
                Key: obj.Key,
            }));
            expect(head.ReplicationStatus).toBe('PENDING');
        }
    }

    it('should replicate existing objects whose key contains Polish diacritics', async () => {
        await runTest([
            { Key: 'BŚ-test.txt', Body: 'data with Ś' },
            { Key: 'ąęóćśźżł-all-diacritics.txt', Body: 'data with all polish diacritics' },
            { Key: 'mixed/żółć/Łódź.dat', Body: 'mixed path segments' },
        ]);
    }, 60000);

    it('should replicate existing objects whose key contains 3-byte UTF-8 characters', async () => {
        await runTest([
            { Key: '日本語-test.txt', Body: 'data with Japanese characters' },
            { Key: '中文/文件.dat', Body: 'data with Chinese path segments' },
            { Key: 'مرحبا-arabic.txt', Body: 'data with Arabic characters' },
        ]);
    }, 60000);

    it('should replicate existing objects whose key contains 4-byte UTF-8 characters', async () => {
        await runTest([
            { Key: '🌍-planet-key.txt', Body: 'data with emoji key' },
            { Key: '📁/📄-nested.txt', Body: 'data with emoji path segments' },
            { Key: '𝔹𝕆𝕃𝔻-math.txt', Body: 'data with mathematical alphanumeric key' },
        ]);
    }, 60000);

    it('should replicate existing objects with ASCII-only keys', async () => {
        await runTest([
            { Key: 'ascii-test-1.txt', Body: 'ascii data 1' },
            { Key: 'ascii-test-2.txt', Body: 'ascii data 2' },
        ]);
    }, 60000);
});
