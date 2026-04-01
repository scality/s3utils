jest.mock('../../utils/async/httpRequest');
jest.mock('../../utils/async/bucketd/listVersions');

const httpRequest = require('../../utils/async/httpRequest');
const listVersions = require('../../utils/async/bucketd/listVersions');
const {
    getSproxydAlias,
    getUploadIdsWithOverview,
    collectOrphanParts,
    buildOrphanMap,
    cleanupOrphanEntry,
    cleanupOrphans,
} = require('../../cleanupMpuOrphans');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BUCKETD_HOSTPORT = 'localhost:9000';
const SHADOW_BUCKET = 'mpuShadowBuckettest-bucket';
const RETRY_PARAMS = { times: 5, interval: 100 };

function overviewUrl({ maxKeys = 1000, marker = '' } = {}) {
    return `http://${BUCKETD_HOSTPORT}/default/bucket/${SHADOW_BUCKET}`
        + `?prefix=${encodeURIComponent('overview..|..')}&maxKeys=${maxKeys}`
        + `&marker=${encodeURIComponent(marker)}`;
}

// Build an overview key in the format: overview..|..<objectKey>..|..<uploadId>
function overviewKey(objectKey, uploadId) {
    return `overview..|..${objectKey}..|..${uploadId}`;
}

function fakeResponse(statusCode, body) {
    return { statusCode, body: body !== undefined ? JSON.stringify(body) : '' };
}

// Build a listing page body with the given overview key strings
function listingPage(keys, isTruncated = false) {
    return {
        Contents: keys.map(key => ({ key })),
        IsTruncated: isTruncated,
    };
}

beforeEach(() => {
    // Reset only httpRequest so each test starts with no queued responses,
    // while preserving async.retry's mock implementation.
    httpRequest.mockReset();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('getUploadIdsWithOverview', () => {
    test('adds nothing when shadow bucket returns 404', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(404));

        const uploadIds = new Set();
        await getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds);

        expect(uploadIds.size).toBe(0);
        expect(httpRequest).toHaveBeenCalledTimes(1);
        expect(httpRequest).toHaveBeenCalledWith('GET', overviewUrl(), undefined);
    });

    test('populates uploadIds from a single page of overview keys', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([
            overviewKey('object1', 'uploadId1'),
            overviewKey('object2', 'uploadId2'),
        ])));

        const uploadIds = new Set();
        await getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds);

        expect(uploadIds).toEqual(new Set(['uploadId1', 'uploadId2']));
        expect(httpRequest).toHaveBeenCalledTimes(1);
        expect(httpRequest).toHaveBeenCalledWith('GET', overviewUrl(), undefined);
    });

    test('follows pagination and uses last key of each page as the next marker', async () => {
        const lastKeyPage1 = overviewKey('object1', 'uploadId1');
        httpRequest
            .mockResolvedValueOnce(fakeResponse(200, {
                Contents: [{ key: lastKeyPage1 }],
                IsTruncated: true,
            }))
            .mockResolvedValueOnce(fakeResponse(200, listingPage([
                overviewKey('object2', 'uploadId2'),
            ])));

        const uploadIds = new Set();
        await getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds);

        expect(uploadIds).toEqual(new Set(['uploadId1', 'uploadId2']));
        expect(httpRequest).toHaveBeenCalledTimes(2);
        expect(httpRequest).toHaveBeenNthCalledWith(2, 'GET', overviewUrl({ marker: lastKeyPage1 }), undefined);
    });

    test('pageSize option controls maxKeys in the URL', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));

        const uploadIds = new Set();
        await getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds, { pageSize: 42 });

        expect(httpRequest).toHaveBeenCalledWith('GET', overviewUrl({ maxKeys: 42 }), undefined);
    });

    test('retry option is forwarded to httpRequest', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));
        const retry = { times: 5, interval: 1000 };

        const uploadIds = new Set();
        await getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds, { retry });

        expect(httpRequest).toHaveBeenCalledWith('GET', overviewUrl(), retry);
    });

    test('overview key with object key containing the separator', async () => {
        // overview key format: overview..|..<objectKey>..|..<uploadId>
        // split('..|..') takes the LAST part as uploadId, so embedded
        // separators in the object key are handled correctly
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([
            overviewKey('dir..|..subdir/object', 'uploadId1'),
        ])));

        const uploadIds = new Set();
        await getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds);

        expect(uploadIds).toEqual(new Set(['uploadId1']));
    });

    test('throws when the listing returns a non-200 non-404 status', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(500));

        const uploadIds = new Set();
        await expect(getUploadIdsWithOverview(BUCKETD_HOSTPORT, SHADOW_BUCKET, uploadIds))
            .rejects.toThrow('returned status 500');
    });
});

// ---------------------------------------------------------------------------
// collectOrphanParts helpers
// ---------------------------------------------------------------------------

function partsUrl({ maxKeys = 1000, marker = '' } = {}) {
    return `http://${BUCKETD_HOSTPORT}/default/bucket/${SHADOW_BUCKET}`
        + `?maxKeys=${maxKeys}&marker=${encodeURIComponent(marker)}`;
}

// Build a part listing entry: key = "<uploadId>..|..<5-digit-index>",
// value = JSON with partLocations containing the given sproxyd keys.
function partEntry(uploadId, partIndex, sproxydKeys = []) {
    return {
        key: `${uploadId}..|..${String(partIndex).padStart(5, '0')}`,
        value: JSON.stringify({ partLocations: sproxydKeys.map(key => ({ key })) }),
    };
}

// Build a listing page body with the given entry objects ({ key, value })
function partsPage(entries, isTruncated = false) {
    return {
        Contents: entries,
        IsTruncated: isTruncated,
    };
}

// ---------------------------------------------------------------------------
// collectOrphanParts tests
// ---------------------------------------------------------------------------

describe('collectOrphanParts', () => {
    test('returns empty map when shadow bucket returns 404', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(404));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result).toEqual({});
        expect(httpRequest).toHaveBeenCalledTimes(1);
        expect(httpRequest).toHaveBeenCalledWith('GET', partsUrl(), undefined);
    });

    test('returns empty map when bucket has no parts', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result).toEqual({});
    });

    test('collects partKeys and sproxydKeys for orphaned upload IDs', async () => {
        const entry = partEntry('uploadId1', 1, ['sproxyd-key-A', 'sproxyd-key-B']);
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([entry])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result).toEqual({
            uploadId1: {
                partKeys: [entry.key],
                sproxydKeys: new Set(['sproxyd-key-A', 'sproxyd-key-B']),
            },
        });
    });

    test('accumulates multiple parts under the same upload ID', async () => {
        const e1 = partEntry('uploadId1', 1, ['key-A']);
        const e2 = partEntry('uploadId1', 2, ['key-B']);
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([e1, e2])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result.uploadId1.partKeys).toEqual([e1.key, e2.key]);
        expect(result.uploadId1.sproxydKeys).toEqual(new Set(['key-A', 'key-B']));
    });

    test('skips overview keys (keys starting with "overview..|..")', async () => {
        const overviewEntry = { key: overviewKey('object1', 'uploadId1'), value: '{}' };
        const partEnt = partEntry('uploadId2', 1, ['key-X']);
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([overviewEntry, partEnt])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(Object.keys(result)).toEqual(['uploadId2']);
    });

    test('skips upload IDs present in uploadIdsWithOverview', async () => {
        const e1 = partEntry('uploadIdLive', 1, ['key-live']);
        const e2 = partEntry('uploadIdOrphan', 1, ['key-orphan']);
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([e1, e2])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set(['uploadIdLive']));

        expect(Object.keys(result)).toEqual(['uploadIdOrphan']);
    });

    test('skips parts with malformed key (no ..|.. separator)', async () => {
        const badEntry = { key: 'no-separator-here', value: '{}' };
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([badEntry])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result).toEqual({});
    });

    test('records partKey but skips sproxydKeys when part metadata JSON is malformed', async () => {
        const entry = { key: 'uploadId1..|..00001', value: 'not-valid-json' };
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([entry])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result.uploadId1.partKeys).toEqual([entry.key]);
        expect(result.uploadId1.sproxydKeys).toEqual(new Set());
    });

    test('records partKey but skips sproxydKeys when partLocations is missing', async () => {
        const entry = { key: 'uploadId1..|..00001', value: JSON.stringify({}) };
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([entry])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result.uploadId1.partKeys).toEqual([entry.key]);
        expect(result.uploadId1.sproxydKeys).toEqual(new Set());
    });

    test('records partKey but skips sproxydKeys when partLocations is empty', async () => {
        const entry = { key: 'uploadId1..|..00001', value: JSON.stringify({ partLocations: [] }) };
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([entry])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(result.uploadId1.partKeys).toEqual([entry.key]);
        expect(result.uploadId1.sproxydKeys).toEqual(new Set());
    });

    test('follows pagination using last key of each page as the next marker', async () => {
        const e1 = partEntry('uploadId1', 1, ['key-A']);
        const e2 = partEntry('uploadId2', 1, ['key-B']);
        httpRequest
            .mockResolvedValueOnce(fakeResponse(200, partsPage([e1], true)))
            .mockResolvedValueOnce(fakeResponse(200, partsPage([e2])));

        const result = await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set());

        expect(Object.keys(result)).toEqual(expect.arrayContaining(['uploadId1', 'uploadId2']));
        expect(httpRequest).toHaveBeenCalledTimes(2);
        expect(httpRequest).toHaveBeenNthCalledWith(2, 'GET', partsUrl({ marker: e1.key }), undefined);
    });

    test('pageSize option controls maxKeys in the URL', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([])));

        await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set(), { pageSize: 42 });

        expect(httpRequest).toHaveBeenCalledWith('GET', partsUrl({ maxKeys: 42 }), undefined);
    });

    test('retry option is forwarded to httpRequest', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([])));
        const retry = { times: 5, interval: 1000 };

        await collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set(), { retry });

        expect(httpRequest).toHaveBeenCalledWith('GET', partsUrl(), retry);
    });

    test('throws when the listing returns a non-200 non-404 status', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(500));

        await expect(collectOrphanParts(BUCKETD_HOSTPORT, SHADOW_BUCKET, new Set()))
            .rejects.toThrow('returned status 500');
    });
});

// ---------------------------------------------------------------------------
// buildOrphanMap tests
// ---------------------------------------------------------------------------

describe('buildOrphanMap', () => {
    test('makes HTTP calls in the correct three-step order', async () => {
        // step 1: overview listing
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));
        // step 2: parts listing
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([])));
        // step 3: overview re-check
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));

        await buildOrphanMap(BUCKETD_HOSTPORT, SHADOW_BUCKET, { retry: RETRY_PARAMS });

        expect(httpRequest).toHaveBeenCalledTimes(3);
        expect(httpRequest).toHaveBeenNthCalledWith(1, 'GET', overviewUrl(), RETRY_PARAMS);
        expect(httpRequest).toHaveBeenNthCalledWith(2, 'GET', partsUrl(), RETRY_PARAMS);
        expect(httpRequest).toHaveBeenNthCalledWith(3, 'GET', overviewUrl(), RETRY_PARAMS);
    });

    test('returns empty map when shadow bucket has no parts', async () => {
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([])));
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));

        const result = await buildOrphanMap(BUCKETD_HOSTPORT, SHADOW_BUCKET);

        expect(result).toEqual({});
    });

    test('returns orphan map for upload IDs that have parts but no overview key', async () => {
        const entry = partEntry('uploadId1', 1, ['sproxyd-key-A']);
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([entry])));
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));

        const result = await buildOrphanMap(BUCKETD_HOSTPORT, SHADOW_BUCKET);

        expect(result).toEqual({
            uploadId1: {
                partKeys: [entry.key],
                sproxydKeys: new Set(['sproxyd-key-A']),
            },
        });
    });

    test('upload IDs present in the step 1 overview are excluded from the result', async () => {
        const e1 = partEntry('uploadIdLive', 1, ['key-live']);
        const e2 = partEntry('uploadIdOrphan', 1, ['key-orphan']);
        // step 1: uploadIdLive already has an overview key
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([
            overviewKey('some-object', 'uploadIdLive'),
        ])));
        // step 2: both IDs have parts; collectOrphanParts skips uploadIdLive
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([e1, e2])));
        // step 3: uploadIdLive still has an overview key
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([
            overviewKey('some-object', 'uploadIdLive'),
        ])));

        const result = await buildOrphanMap(BUCKETD_HOSTPORT, SHADOW_BUCKET);

        expect(Object.keys(result)).toEqual(['uploadIdOrphan']);
    });

    test('removes upload IDs that gained an overview key between step 1 and step 3 (race condition)', async () => {
        const entry = partEntry('uploadId1', 1, ['sproxyd-key-A']);
        // step 1: no overview keys yet
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([])));
        // step 2: uploadId1 appears as an orphan candidate
        httpRequest.mockResolvedValueOnce(fakeResponse(200, partsPage([entry])));
        // step 3: uploadId1 now has an overview key (MPU completed between step 1 and step 2)
        httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage([
            overviewKey('some-object', 'uploadId1'),
        ])));

        const result = await buildOrphanMap(BUCKETD_HOSTPORT, SHADOW_BUCKET);

        expect(result).toEqual({});
    });
});

// ---------------------------------------------------------------------------
// cleanupOrphanEntry helpers and tests
// ---------------------------------------------------------------------------

const SPROXYD_HOSTPORT = 'localhost:8181';
const SPROXYD_ALIAS = 'test-alias';

function sproxydDeleteUrl(key) {
    return `http://${SPROXYD_HOSTPORT}/${SPROXYD_ALIAS}/${key}`;
}

function partDeleteUrl(partKey) {
    return `http://${BUCKETD_HOSTPORT}/default/bucket/${SHADOW_BUCKET}/${encodeURIComponent(partKey)}`;
}

function makeReqLogger() {
    return { error: jest.fn(), debug: jest.fn() };
}

describe('cleanupOrphanEntry', () => {
    beforeAll(async () => {
        // Initialise the module-level sproxydAlias variable used to build URLs.
        const aliasBody = JSON.stringify({ 'ring_driver:0': { alias: SPROXYD_ALIAS } });
        httpRequest.mockResolvedValueOnce({ statusCode: 200, body: aliasBody });
        await getSproxydAlias();
    });

    test('does nothing when keysToDelete and partKeys are both empty', async () => {
        const reqLogger = makeReqLogger();
        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: [] }, [], undefined);

        expect(httpRequest).not.toHaveBeenCalled();
    });

    test('sends DELETE for each sproxyd key', async () => {
        httpRequest
            .mockResolvedValueOnce({ statusCode: 200, body: '' })
            .mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: [] }, ['key-A', 'key-B'], undefined);

        expect(httpRequest).toHaveBeenCalledTimes(2);
        expect(httpRequest).toHaveBeenNthCalledWith(1, 'DELETE', sproxydDeleteUrl('key-A'), undefined);
        expect(httpRequest).toHaveBeenNthCalledWith(2, 'DELETE', sproxydDeleteUrl('key-B'), undefined);
    });

    test('sends DELETE for each part key', async () => {
        httpRequest
            .mockResolvedValueOnce({ statusCode: 200, body: '' })
            .mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: ['part-key-1', 'part-key-2'] }, [], undefined);

        expect(httpRequest).toHaveBeenCalledTimes(2);
        expect(httpRequest).toHaveBeenNthCalledWith(1, 'DELETE', partDeleteUrl('part-key-1'), undefined);
        expect(httpRequest).toHaveBeenNthCalledWith(2, 'DELETE', partDeleteUrl('part-key-2'), undefined);
    });

    test('deletes sproxyd keys before part keys', async () => {
        httpRequest
            .mockResolvedValueOnce({ statusCode: 200, body: '' })
            .mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: ['part-key-1'] }, ['sproxyd-key-A'], undefined);

        expect(httpRequest).toHaveBeenNthCalledWith(1, 'DELETE', sproxydDeleteUrl('sproxyd-key-A'), undefined);
        expect(httpRequest).toHaveBeenNthCalledWith(2, 'DELETE', partDeleteUrl('part-key-1'), undefined);
    });

    test('forwards retry param to httpRequest', async () => {
        const retry = { times: 3, interval: 100 };
        httpRequest.mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: [] }, ['key-A'], retry);

        expect(httpRequest).toHaveBeenCalledWith('DELETE', sproxydDeleteUrl('key-A'), retry);
    });

    test('logs error and continues when sproxyd DELETE returns non-200', async () => {
        httpRequest
            .mockResolvedValueOnce({ statusCode: 500, body: '' })
            .mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: [] }, ['key-A', 'key-B'], undefined);

        expect(reqLogger.error).toHaveBeenCalledTimes(1);
        expect(reqLogger.error).toHaveBeenCalledWith(
            'failed to delete orphaned sproxyd key', expect.objectContaining({ sproxydKey: 'key-A' }),
        );
        expect(httpRequest).toHaveBeenCalledTimes(2);
    });

    test('logs error and continues when sproxyd DELETE throws', async () => {
        httpRequest
            .mockRejectedValueOnce(new Error('network error'))
            .mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: [] }, ['key-A', 'key-B'], undefined);

        expect(reqLogger.error).toHaveBeenCalledTimes(1);
        expect(reqLogger.error).toHaveBeenCalledWith(
            'failed to delete orphaned sproxyd key', expect.objectContaining({ sproxydKey: 'key-A' }),
        );
        expect(httpRequest).toHaveBeenCalledTimes(2);
    });

    test('accepts 404 on part DELETE without logging an error', async () => {
        httpRequest.mockResolvedValueOnce({ statusCode: 404, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: ['part-key-1'] }, [], undefined);

        expect(reqLogger.error).not.toHaveBeenCalled();
    });

    test('logs error and continues when part DELETE returns non-200 non-404', async () => {
        httpRequest
            .mockResolvedValueOnce({ statusCode: 500, body: '' })
            .mockResolvedValueOnce({ statusCode: 200, body: '' });
        const reqLogger = makeReqLogger();

        await cleanupOrphanEntry(reqLogger, BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, SHADOW_BUCKET,
            { partKeys: ['part-key-1', 'part-key-2'] }, [], undefined);

        expect(reqLogger.error).toHaveBeenCalledTimes(1);
        expect(reqLogger.error).toHaveBeenCalledWith(
            'failed to delete orphaned part metadata', expect.objectContaining({ partKey: 'part-key-1' }),
        );
        expect(httpRequest).toHaveBeenCalledTimes(2);
    });
});

// ---------------------------------------------------------------------------
// cleanupOrphans tests
// ---------------------------------------------------------------------------

describe('cleanupOrphans', () => {
    beforeEach(() => {
        listVersions.mockReset();
    });

    // Build an orphan map entry
    function makeOrphanEntry(partKeys, sproxydKeyList) {
        return { partKeys, sproxydKeys: new Set(sproxydKeyList) };
    }

    // Build a version listing entry as yielded by listVersions
    function versionWithUploadId(uploadId, locationKeys = []) {
        return {
            key: 'some-object',
            versionId: 'some-version-id',
            value: { uploadId, location: locationKeys.map(key => ({ key })) },
        };
    }

    test('resolves without any DELETE calls when orphanMap is empty', async () => {
        listVersions.mockImplementation(async function* () {});

        await cleanupOrphans(BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, 'test-bucket', SHADOW_BUCKET, {});

        expect(httpRequest).not.toHaveBeenCalled();
    });

    test('passes bucketdHostport, bucket, pageSize and retry to listVersions', async () => {
        listVersions.mockImplementation(async function* () {});
        const retry = { times: 3, interval: 100 };

        await cleanupOrphans(BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, 'test-bucket', SHADOW_BUCKET, {},
            { pageSize: 42, retry });

        expect(listVersions).toHaveBeenCalledWith(
            BUCKETD_HOSTPORT, 'test-bucket', expect.objectContaining({ pageSize: 42, retry }),
        );
    });

    test('deletes all sproxyd keys and part keys for upload IDs not matched by any version', async () => {
        listVersions.mockImplementation(async function* () {});
        httpRequest.mockResolvedValue({ statusCode: 200, body: '' });
        const orphanMap = {
            uploadId1: makeOrphanEntry(['uploadId1..|..00001'], ['sproxyd-key-A', 'sproxyd-key-B']),
        };

        await cleanupOrphans(BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, 'test-bucket', SHADOW_BUCKET, orphanMap);

        expect(httpRequest).toHaveBeenCalledWith('DELETE', sproxydDeleteUrl('sproxyd-key-A'), undefined);
        expect(httpRequest).toHaveBeenCalledWith('DELETE', sproxydDeleteUrl('sproxyd-key-B'), undefined);
        expect(httpRequest).toHaveBeenCalledWith('DELETE', partDeleteUrl('uploadId1..|..00001'), undefined);
        expect(Object.keys(orphanMap)).toHaveLength(0);
    });

    test('skips versions with no uploadId or with uploadId not in orphanMap', async () => {
        listVersions.mockImplementation(async function* () {
            yield { key: 'obj1', versionId: 'v1', value: {} }; // no uploadId
            yield { key: 'obj2', versionId: 'v2', value: { uploadId: 'other-upload-id' } };
        });
        httpRequest.mockResolvedValue({ statusCode: 200, body: '' });
        const orphanMap = {
            uploadId1: makeOrphanEntry([], ['sproxyd-key-A']),
        };

        await cleanupOrphans(BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, 'test-bucket', SHADOW_BUCKET, orphanMap);

        // uploadId1 is not matched by any version, so it's cleaned up in the remaining phase
        expect(httpRequest).toHaveBeenCalledWith('DELETE', sproxydDeleteUrl('sproxyd-key-A'), undefined);
    });

    test('skips sproxyd keys referenced by a completed version, deletes only orphaned ones', async () => {
        listVersions.mockImplementation(async function* () {
            yield versionWithUploadId('uploadId1', ['sproxyd-key-A']); // key-A is referenced
        });
        httpRequest.mockResolvedValue({ statusCode: 200, body: '' });
        const orphanMap = {
            uploadId1: makeOrphanEntry([], ['sproxyd-key-A', 'sproxyd-key-B']),
        };

        await cleanupOrphans(BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, 'test-bucket', SHADOW_BUCKET, orphanMap);

        expect(httpRequest).toHaveBeenCalledWith('DELETE', sproxydDeleteUrl('sproxyd-key-B'), undefined);
        expect(httpRequest).not.toHaveBeenCalledWith('DELETE', sproxydDeleteUrl('sproxyd-key-A'), undefined);
    });

    test('upload ID matched by a version is not processed again in the remaining-orphans phase', async () => {
        listVersions.mockImplementation(async function* () {
            yield versionWithUploadId('uploadId1', []);
        });
        httpRequest.mockResolvedValue({ statusCode: 200, body: '' });
        const orphanMap = {
            uploadId1: makeOrphanEntry(['uploadId1..|..00001'], ['sproxyd-key-A']),
        };

        await cleanupOrphans(BUCKETD_HOSTPORT, SPROXYD_HOSTPORT, 'test-bucket', SHADOW_BUCKET, orphanMap);

        // 2 calls: sproxyd-key-A + part-key; not 4 (which would happen if double-processed)
        expect(httpRequest).toHaveBeenCalledTimes(2);
        expect(Object.keys(orphanMap)).toHaveLength(0);
    });
});
