const crypto = require('crypto');
const { PassThrough, Readable } = require('stream');

const { metadataScan, UsageError } = require('../../metadataScan');
const {
    bucketdHostport,
    uniqueBucketName,
    createBucket,
    createBucketWithObjects,
    deleteBucket,
    getBucketRaftSession,
} = require('../utils/BucketdSetup');
const FakeSproxyd = require('../utils/FakeSproxyd');

jest.setTimeout(60000);

// minimum size of a listing entry value for bucketd to trim the
// "location" field from it, see arsenal's Extension.trimMetadata()
const TRIM_METADATA_MIN_BLOB_SIZE = 10000;

/**
 * Generate random sproxyd keys
 *
 * @param {number} count - number of keys to generate
 * @return {string[]} - sproxyd keys, made of 38 random hex digits
 * followed by a fixed class of service byte
 */
function newSproxydKeys(count) {
    const keys = [];
    for (let i = 0; i < count; ++i) {
        keys.push(`${crypto.randomBytes(19).toString('hex').toUpperCase()}20`);
    }
    return keys;
}

function locations(sproxydKeys) {
    return sproxydKeys.map((key, i) => ({
        key,
        size: 1,
        start: i,
        dataStoreName: 'us-east-1',
        dataStoreETag: `${i + 1}:0123456789abcdef0123456789abcdef`,
    }));
}

/**
 * Metadata of a regular object (not MPU) stored with the given
 * sproxyd keys
 */
function objectMD(sproxydKeys, extra = {}) {
    return {
        'content-length': sproxydKeys.length,
        'content-md5': '0123456789abcdef0123456789abcdef',
        'location': locations(sproxydKeys),
        ...extra,
    };
}

/**
 * Metadata of an MPU object stored with the given sproxyd keys, one
 * per part
 */
function mpuObjectMD(sproxydKeys, extra = {}) {
    return objectMD(sproxydKeys, {
        'content-md5': `0123456789abcdef0123456789abcdef-${sproxydKeys.length}`,
        ...extra,
    });
}

/**
 * Create a logger capturing all log entries
 *
 * @param {function} [onEntry] - called with each new log entry
 * @return {object} - { logger, entries } where entries is an array
 * of { level, message, data } objects
 */
function createCaptureLogger(onEntry = () => {}) {
    const entries = [];
    const logger = {};
    for (const level of ['trace', 'debug', 'info', 'warn', 'error', 'fatal']) {
        logger[level] = (message, data) => {
            const entry = { level, message, data };
            entries.push(entry);
            onEntry(entry);
        };
    }
    return { logger, entries };
}

function findLogs(entries, message) {
    return entries.filter(entry => entry.message === message);
}

/**
 * Get the list of buckets that a scan started scanning, from its logs
 */
function getScannedBuckets(entries) {
    return findLogs(entries, 'start scanning bucket')
        .map(entry => entry.data.url.slice('s3://'.length).split('/')[0]);
}

describe('metadataScan', () => {
    const fakeSproxyd = new FakeSproxyd();
    let sproxydHostport;
    let createdBuckets;

    /**
     * Create a bucket with objects, deleted after the test
     */
    async function setupBucket(objects) {
        const bucket = uniqueBucketName('metadatascan');
        createdBuckets.push(bucket);
        await createBucketWithObjects(bucket, objects);
        return bucket;
    }

    function getScanEnv() {
        return {
            BUCKETD_HOSTPORT: bucketdHostport,
            SPROXYD_HOSTPORT: sproxydHostport,
        };
    }

    /**
     * Run a scan with the given command-line options, against bucketd
     * and the fake sproxyd
     *
     * @param {string[]} args - command-line options
     * @param {object} [params] - extra metadataScan() parameters
     * @return {Promise<object>} - { status, exitCode, logs }
     */
    async function runScan(args, params = {}) {
        const { logger, entries } = createCaptureLogger();
        const { status, exitCode } = await metadataScan({
            argv: ['node', 'metadataScan.js', ...args],
            env: getScanEnv(),
            logger,
            ...params,
        });
        return { status, exitCode, logs: entries };
    }

    beforeAll(async () => {
        sproxydHostport = await fakeSproxyd.start();
    });

    afterAll(async () => {
        await fakeSproxyd.stop();
    });

    beforeEach(() => {
        createdBuckets = [];
        fakeSproxyd.reset();
    });

    afterEach(async () => {
        for (const bucket of createdBuckets) {
            await deleteBucket(bucket);
        }
    });

    describe('options', () => {
        it('should reject with a UsageError when no source of objects is given', async () => {
            await expect(runScan([])).rejects.toThrow(UsageError);
        });

        it('should reject with a UsageError when --from-url does not start with s3://', async () => {
            const promise = runScan(['--buckets', 'bucket1', '--from-url', 'http://bucket1/key']);
            await expect(promise).rejects.toThrow(UsageError);
            await expect(promise).rejects.toThrow('FROM_URL must start with "s3://"');
        });

        it('should reject with a UsageError when the --from-url bucket is not in --buckets', async () => {
            const promise = runScan(['--buckets', 'bucket1,bucket2', '--from-url', 's3://bucket3/key']);
            await expect(promise).rejects.toThrow(UsageError);
            await expect(promise).rejects.toThrow('FROM_URL bucket bucket3 is not in BUCKETS');
        });
    });

    describe('abort signal', () => {
        it('should reject with the abort reason when the signal is already aborted', async () => {
            const abortController = new AbortController();
            abortController.abort(new Error('scan aborted by test'));
            await expect(runScan(['--buckets', 'bucket1'], {
                signal: abortController.signal,
            })).rejects.toThrow('scan aborted by test');
        });

        it('should stop a running scan and log its last status', async () => {
            const bucket = await setupBucket({
                object: objectMD(newSproxydKeys(1)),
            });
            // input stream that is never ended, so that the scan
            // keeps running until aborted
            const stdin = new PassThrough();
            stdin.write(`${JSON.stringify({ bucket, key: 'object' })}\n`);
            const abortController = new AbortController();
            // abort on the first progress update, to make sure the
            // scan is running
            const { logger, entries } = createCaptureLogger(entry => {
                if (entry.message === 'progress update') {
                    abortController.abort(new Error('scan aborted by test'));
                }
            });

            const promise = metadataScan({
                argv: ['node', 'metadataScan.js', '--keys-from-stdin', '--log-progress-interval', '1'],
                env: getScanEnv(),
                stdin,
                logger,
                signal: abortController.signal,
            });

            await expect(promise).rejects.toThrow('scan aborted by test');
            stdin.destroy();
            const messages = entries.map(entry => entry.message);
            expect(messages).not.toContain('completed scan');
            const stoppingIndex = messages.indexOf('stopping execution');
            expect(stoppingIndex).not.toBe(-1);
            expect(messages.indexOf('last status')).toBeGreaterThan(stoppingIndex);
            expect(findLogs(entries, 'last status')[0].data.scanned).toBe(1);
        });
    });

    describe('bucket scan', () => {
        it('should scan a bucket without issue', async () => {
            const [key1, key2, key3, key4] = newSproxydKeys(4);
            const bucket = await setupBucket({
                'regular': objectMD([key1]),
                'empty': objectMD([], { 'content-length': 0, 'location': null }),
                'phd': objectMD([key2], { isPHD: true }),
                // master key of a versioned object, along with its
                // versioned key, which must not be counted twice
                'versioned': objectMD([key3], { versionId: 'v2' }),
                'versioned\0v1': objectMD([key4], { versionId: 'v1' }),
                'versioned\0v2': objectMD([key3], { versionId: 'v2' }),
            });

            const { status, exitCode, logs } = await runScan(['--buckets', bucket]);

            expect(exitCode).toBe(0);
            expect(status).toMatchObject({
                // regular, empty, versioned (master) and versioned\0v1
                objectsScanned: 4,
                objectsSkipped: 0,
                objectsWithDupKeys: 0,
                objectsWithEmptyMetadata: 0,
                objectsWithBrokenMetadata: 0,
                objectsErrors: 0,
            });
            expect(logs.filter(entry => entry.level === 'error')).toEqual([]);
            expect(findLogs(logs, 'completed scan')).toHaveLength(1);
        });

        it('should detect sproxyd keys shared by multiple objects', async () => {
            const [sharedKey, otherKey] = newSproxydKeys(2);
            const bucket = await setupBucket({
                object1: objectMD([sharedKey]),
                object2: objectMD([otherKey, sharedKey]),
            });

            const { status, exitCode, logs } = await runScan(['--buckets', bucket]);

            expect(exitCode).toBe(102);
            expect(status).toMatchObject({
                objectsScanned: 2,
                objectsWithDupKeys: 1,
            });
            const dupLogs = findLogs(logs, 'duplicate sproxyd key found');
            expect(dupLogs).toHaveLength(1);
            expect(dupLogs[0].data.sproxydKey).toBe(sharedKey);
            expect([dupLogs[0].data.objectUrl, dupLogs[0].data.objectUrl2].sort()).toEqual([
                `s3://${bucket}/object1`,
                `s3://${bucket}/object2`,
            ]);
        });

        it('should detect objects with empty metadata', async () => {
            const bucket = await setupBucket({
                'empty-md': '{}',
                'regular': objectMD(newSproxydKeys(1)),
            });

            const { status, exitCode, logs } = await runScan(['--buckets', bucket]);

            expect(exitCode).toBe(104);
            expect(status).toMatchObject({
                objectsScanned: 2,
                objectsWithEmptyMetadata: 1,
            });
            const emptyLogs = findLogs(logs, 'object with empty metadata found');
            expect(emptyLogs).toHaveLength(1);
            expect(emptyLogs[0].data.objectUrl).toBe(`s3://${bucket}/empty-md`);
        });

        it('should detect objects with broken locations', async () => {
            const [key1, key2] = newSproxydKeys(2);
            const bucket = await setupBucket({
                'empty-location': objectMD([key1], { location: [] }),
                'non-array-location': objectMD([key1], { location: { key: key1 } }),
                'regular': objectMD([key2]),
            });

            const { status, exitCode } = await runScan(['--buckets', bucket]);

            expect(exitCode).toBe(105);
            expect(status).toMatchObject({
                objectsScanned: 3,
                objectsWithBrokenMetadata: 2,
            });
        });

        it('should only scan MPU objects with --mpu-only', async () => {
            const [key1, key2, key3] = newSproxydKeys(3);
            const bucket = await setupBucket({
                mpu: mpuObjectMD([key1, key2]),
                regular: objectMD([key3]),
            });

            const { status, exitCode } = await runScan(['--buckets', bucket, '--mpu-only']);

            expect(exitCode).toBe(0);
            expect(status).toMatchObject({
                objectsScanned: 1,
                objectsSkipped: 1,
            });
        });

        it('should give the same results when listing one key at a time', async () => {
            const [key1, key2, key3, key4] = newSproxydKeys(4);
            const bucket = await setupBucket({
                'object1': objectMD([key1]),
                'object2': objectMD([key2]),
                // duplicate key of object1, found in a later listing page
                'object3': objectMD([key3, key1]),
                // the versioned key matching the master key is listed
                // in the next page, and must still be skipped
                'versioned': objectMD([key4], { versionId: 'v1' }),
                'versioned\0v1': objectMD([key4], { versionId: 'v1' }),
            });

            const defaultScan = await runScan(['--buckets', bucket]);
            const pagedScan = await runScan(['--buckets', bucket, '--listing-limit', '1']);

            expect(pagedScan.exitCode).toBe(102);
            expect(pagedScan.status).toMatchObject({
                objectsScanned: 4,
                objectsWithDupKeys: 1,
            });
            expect(pagedScan.exitCode).toBe(defaultScan.exitCode);
            expect(pagedScan.status).toEqual(defaultScan.status);
        });

        it('should fetch the locations of objects trimmed from the listing', async () => {
            // enough parts for bucketd to trim the "location" field
            // from the listing entry
            const bigObjectKeys = newSproxydKeys(200);
            const bigObjectMD = mpuObjectMD(bigObjectKeys);
            expect(JSON.stringify(bigObjectMD).length).toBeGreaterThanOrEqual(TRIM_METADATA_MIN_BLOB_SIZE);
            const bucket = await setupBucket({
                'big-mpu': bigObjectMD,
                // shares a key with the big MPU, only found if the
                // big MPU locations are fetched
                'small': objectMD([bigObjectKeys[100]]),
            });

            const { status, exitCode, logs } = await runScan(['--buckets', bucket]);

            expect(exitCode).toBe(102);
            expect(status).toMatchObject({
                objectsScanned: 2,
                objectsWithDupKeys: 1,
                objectsErrors: 0,
            });
            const dupLogs = findLogs(logs, 'duplicate sproxyd key found');
            expect(dupLogs).toHaveLength(1);
            expect(dupLogs[0].data.sproxydKey).toBe(bigObjectKeys[100]);
            expect([dupLogs[0].data.objectUrl, dupLogs[0].data.objectUrl2].sort()).toEqual([
                `s3://${bucket}/big-mpu`,
                `s3://${bucket}/small`,
            ]);
        });
    });

    describe('bucket selection', () => {
        it('should resume from --from-url', async () => {
            const objects = {
                a: objectMD(newSproxydKeys(1)),
                b: objectMD(newSproxydKeys(1)),
                c: objectMD(newSproxydKeys(1)),
            };
            const bucket1 = await setupBucket(objects);
            const bucket2 = await setupBucket(objects);
            const bucket3 = await setupBucket(objects);

            const { status, exitCode, logs } = await runScan([
                '--buckets', `${bucket1},${bucket2},${bucket3}`,
                '--from-url', `s3://${bucket2}/b`,
            ]);

            expect(exitCode).toBe(0);
            // "c" in bucket2, then all 3 objects of bucket3
            expect(status.objectsScanned).toBe(4);
            expect(getScannedBuckets(logs)).toEqual([bucket2, bucket3]);
        });

        it('should skip buckets that do not exist', async () => {
            const missingBucket = uniqueBucketName('metadatascan-missing');
            const bucket = await setupBucket({
                object: objectMD(newSproxydKeys(1)),
            });

            const { status, exitCode } = await runScan(['--buckets', `${missingBucket},${bucket}`]);

            expect(exitCode).toBe(0);
            expect(status).toMatchObject({
                bucketsDeleted: 1,
                objectsScanned: 1,
            });
        });

        it('should scan the buckets of --raft-sessions except MPU shadow buckets', async () => {
            const bucket = await setupBucket({
                object: objectMD(newSproxydKeys(1)),
            });
            const shadowBucket = `mpuShadowBucket${bucket}`;
            createdBuckets.push(shadowBucket);
            await createBucket(shadowBucket);
            const raftSession = await getBucketRaftSession(bucket);
            expect(await getBucketRaftSession(shadowBucket)).toBe(raftSession);

            // other buckets of the raft session may be scanned as
            // well, so only check for the test buckets
            const { logs } = await runScan(['--raft-sessions', raftSession]);

            const scannedBuckets = getScannedBuckets(logs);
            expect(scannedBuckets).toContain(bucket);
            expect(scannedBuckets).not.toContain(shadowBucket);
        });
    });

    describe('missing sproxyd keys check', () => {
        it('should detect missing sproxyd keys', async () => {
            const [key1, key2, missingKey] = newSproxydKeys(3);
            fakeSproxyd.missingKeys.add(missingKey);
            const bucket = await setupBucket({
                object1: objectMD([key1]),
                object2: objectMD([key2, missingKey]),
            });

            const { status, exitCode, logs } = await runScan([
                '--buckets', bucket, '--check-missing-sproxyd-keys',
            ]);

            expect(exitCode).toBe(101);
            expect(status).toMatchObject({
                objectsScanned: 2,
                objectsWithMissingKeys: 1,
                objectsErrors: 0,
            });
            const missingLogs = findLogs(logs, 'sproxyd check reported missing key');
            expect(missingLogs).toHaveLength(1);
            expect(missingLogs[0].data).toMatchObject({
                objectUrl: `s3://${bucket}/object2`,
                sproxydKey: missingKey,
            });
        });

        it('should count sproxyd errors', async () => {
            const [key1, failingKey] = newSproxydKeys(2);
            fakeSproxyd.failingKeys.add(failingKey);
            const bucket = await setupBucket({
                object1: objectMD([key1]),
                object2: objectMD([failingKey]),
            });

            const { status, exitCode, logs } = await runScan([
                '--buckets', bucket, '--check-missing-sproxyd-keys',
            ]);

            expect(exitCode).toBe(103);
            expect(status).toMatchObject({
                objectsScanned: 2,
                objectsWithMissingKeys: 0,
                objectsErrors: 1,
            });
            const errorLogs = findLogs(logs, 'sproxyd check returned HTTP error');
            expect(errorLogs).toHaveLength(1);
            expect(errorLogs[0].data).toMatchObject({
                objectUrl: `s3://${bucket}/object2`,
                sproxydKey: failingKey,
                httpCode: 500,
            });
        });

        it('should check the fetched locations of objects trimmed from the listing', async () => {
            const bigObjectKeys = newSproxydKeys(200);
            fakeSproxyd.missingKeys.add(bigObjectKeys[123]);
            const bucket = await setupBucket({
                'big-mpu': mpuObjectMD(bigObjectKeys),
            });

            const { status, exitCode, logs } = await runScan([
                '--buckets', bucket, '--check-missing-sproxyd-keys',
            ]);

            expect(exitCode).toBe(101);
            expect(status).toMatchObject({
                objectsScanned: 1,
                objectsWithMissingKeys: 1,
            });
            const missingLogs = findLogs(logs, 'sproxyd check reported missing key');
            expect(missingLogs).toHaveLength(1);
            expect(missingLogs[0].data).toMatchObject({
                objectUrl: `s3://${bucket}/big-mpu`,
                sproxydKey: bigObjectKeys[123],
            });
        });

        it('should fail when the sproxyd configuration cannot be fetched', async () => {
            fakeSproxyd.confStatus = 500;
            const bucket = await setupBucket({
                object: objectMD(newSproxydKeys(1)),
            });

            await expect(runScan([
                '--buckets', bucket, '--check-missing-sproxyd-keys',
            ])).rejects.toThrow(/\/\.conf returned status 500/);
        });
    });

    describe('--keys-from-stdin', () => {
        it('should scan the objects given as input', async () => {
            const [key1, key2] = newSproxydKeys(2);
            const bucket = await setupBucket({
                'object': objectMD([key1]),
                'versioned\0v1': objectMD([key2], { versionId: 'v1' }),
            });
            const stdin = Readable.from([
                { bucket, key: 'object' },
                { bucket, key: 'versioned\0v1' },
                { bucket, key: 'does-not-exist' },
                { key: 'object' },
            ].map(item => `${JSON.stringify(item)}\n`));

            const { status, exitCode, logs } = await runScan(['--keys-from-stdin'], { stdin });

            expect(exitCode).toBe(103);
            expect(status).toMatchObject({
                // object, versioned\0v1 and does-not-exist
                objectsScanned: 3,
                objectsErrors: 1,
            });
            const fetchErrorLogs = findLogs(logs, 'error fetching object locations array');
            expect(fetchErrorLogs).toHaveLength(1);
            expect(fetchErrorLogs[0].data.objectUrl).toBe(`s3://${bucket}/does-not-exist`);
            expect(findLogs(logs, 'missing "bucket" attribute in JSON stream item')).toHaveLength(1);
        });
    });
});
