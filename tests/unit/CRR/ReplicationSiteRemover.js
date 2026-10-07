const werelogs = require('werelogs');

const ReplicationSiteRemover = require('../../../CRR/ReplicationSiteRemover');
const { objectMd } = require('../../utils/crr');

const { getGlobalStatus, removeSiteFromLists } = ReplicationSiteRemover;

const logger = new werelogs.Logger('ReplicationSiteRemover::tests', 'debug', 'debug');

function makeMd(replicationInfo, extra = {}) {
    return JSON.stringify({ ...objectMd, ...extra, replicationInfo });
}

// object broken by crrExistingObjects with SITE_NAME=foo
const brokenReplicationInfo = {
    status: 'PENDING',
    backends: [
        { site: 'destination', status: 'COMPLETED', dataStoreVersionId: '' },
        { site: 'foo', status: 'PENDING', dataStoreVersionId: '' },
    ],
    content: ['DATA', 'METADATA'],
    destination: 'arn:aws:s3:::destination',
    storageClass: 'destination,foo',
    role: 'arn:aws:iam::root:role/s3-replication-role',
    storageType: '',
    dataStoreVersionId: '',
};

/**
 * Builds a remover with mocked S3 and metadata clients.
 * @param {Object} params - Extra constructor params.
 * @param {Object} opts - Mock setup.
 * @param {Array<Object>} opts.pages - ListObjectVersions responses, in order.
 * @param {Object} opts.mds - Metadata blob per "key/versionId".
 * @param {Set<string>} [opts.failGet] - "key/versionId" whose getMetadata fails.
 * @returns {ReplicationSiteRemover} Remover with mocks.
 */
function initRemover(params, opts) {
    const remover = new ReplicationSiteRemover({
        buckets: ['bucket0'],
        siteToRemove: 'foo',
        accessKey: 'ak',
        secretKey: 'sk',
        endpoint: 'http://dummyEndpoint:8000',
        dryRun: false,
        ...params,
    }, logger);
    const pages = [...opts.pages];
    remover.s3.send = jest.fn(() => Promise.resolve(pages.shift()));
    remover.cloudserverClient.getMetadata = jest.fn((p, cb) => {
        const id = `${p.Key}/${p.VersionId}`;
        if (opts.failGet && opts.failGet.has(id)) {
            return cb(new Error('getMetadata failed'));
        }
        return cb(null, { Body: opts.mds[id] });
    });
    remover.cloudserverClient.putMetadata = jest.fn((p, cb) => cb(null, {}));
    return remover;
}

function page(versions, deleteMarkers = [], next = {}) {
    return {
        Versions: versions.map(([Key, VersionId]) => ({ Key, VersionId })),
        DeleteMarkers: deleteMarkers.map(([Key, VersionId]) => ({ Key, VersionId })),
        NextKeyMarker: next.key,
        NextVersionIdMarker: next.versionId,
    };
}

function writtenReplicationInfo(remover, callIndex = 0) {
    return JSON.parse(remover.cloudserverClient.putMetadata.mock.calls[callIndex][0].Body).replicationInfo;
}

describe('ReplicationSiteRemover helpers', () => {
    it('getGlobalStatus: FAILED, then PENDING, then COMPLETED', () => {
        expect(getGlobalStatus([{ status: 'COMPLETED' }])).toBe('COMPLETED');
        expect(getGlobalStatus([{ status: 'COMPLETED' }, { status: 'PENDING' }])).toBe('PENDING');
        expect(getGlobalStatus([{ status: 'PENDING' }, { status: 'FAILED' }])).toBe('FAILED');
    });

    it('removeSiteFromLists: removes the site and its aligned storage type', () => {
        expect(removeSiteFromLists('aws-location,foo', 'aws_s3,azure', 'foo'))
            .toEqual({ storageClass: 'aws-location', storageType: 'aws_s3' });
    });

    it('removeSiteFromLists: removes a site tagged preferred_read', () => {
        expect(removeSiteFromLists('foo:preferred_read,destination', '', 'foo'))
            .toEqual({ storageClass: 'destination', storageType: '' });
    });

    it('removeSiteFromLists: keeps storageType when lists are not aligned', () => {
        expect(removeSiteFromLists('destination,foo', '', 'foo'))
            .toEqual({ storageClass: 'destination', storageType: '' });
    });

    it('removeSiteFromLists: no change when the site is absent', () => {
        expect(removeSiteFromLists('destination', '', 'foo'))
            .toEqual({ storageClass: 'destination', storageType: '' });
    });
});

describe('ReplicationSiteRemover', () => {
    it('should remove the site and set COMPLETED when the other sites are COMPLETED', async () => {
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplicationInfo) },
        });
        const stats = await remover.run();
        expect(stats).toEqual({
            scanned: 1, updated: 1, reset: 0, skipped: 0, manual: 0, errors: 0,
        });
        const repInfo = writtenReplicationInfo(remover);
        expect(repInfo.status).toBe('COMPLETED');
        expect(repInfo.storageClass).toBe('destination');
        expect(repInfo.backends).toEqual([
            { site: 'destination', status: 'COMPLETED', dataStoreVersionId: '' },
        ]);
    });

    it('should keep PENDING when another site is still PENDING', async () => {
        const repInfo = JSON.parse(JSON.stringify(brokenReplicationInfo));
        repInfo.backends[0].status = 'PENDING';
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(repInfo) },
        });
        await remover.run();
        expect(writtenReplicationInfo(remover).status).toBe('PENDING');
    });

    it('should write nothing in dry run', async () => {
        const remover = initRemover({ dryRun: true }, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplicationInfo) },
        });
        const stats = await remover.run();
        expect(remover.cloudserverClient.putMetadata).not.toHaveBeenCalled();
        expect(stats.updated).toBe(1);
    });

    it('should default to dry run', async () => {
        const remover = initRemover({ dryRun: undefined }, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplicationInfo) },
        });
        await remover.run();
        expect(remover.cloudserverClient.putMetadata).not.toHaveBeenCalled();
    });

    it('should skip objects without the site', async () => {
        const clean = {
            ...brokenReplicationInfo,
            status: 'COMPLETED',
            backends: [brokenReplicationInfo.backends[0]],
            storageClass: 'destination',
        };
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(clean) },
        });
        const stats = await remover.run();
        expect(remover.cloudserverClient.putMetadata).not.toHaveBeenCalled();
        expect(stats.skipped).toBe(1);
    });

    it('should reset the replication info when the site is the only one', async () => {
        // crrExistingObjects with SITE_NAME=foo on an object never replicated
        const onlyFoo = {
            ...brokenReplicationInfo,
            backends: [brokenReplicationInfo.backends[1]],
            storageClass: 'foo',
        };
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(onlyFoo) },
        });
        const stats = await remover.run();
        expect(stats).toEqual(expect.objectContaining({ updated: 1, reset: 1, manual: 0 }));
        expect(writtenReplicationInfo(remover)).toEqual({
            status: '',
            backends: [],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
        });
    });

    it('should not reset in dry run', async () => {
        const onlyFoo = {
            ...brokenReplicationInfo,
            backends: [brokenReplicationInfo.backends[1]],
            storageClass: 'foo',
        };
        const remover = initRemover({ dryRun: true }, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(onlyFoo) },
        });
        const stats = await remover.run();
        expect(remover.cloudserverClient.putMetadata).not.toHaveBeenCalled();
        expect(stats).toEqual(expect.objectContaining({ updated: 1, reset: 1 }));
    });

    it('should flag for manual review when storageClass has another site without backend', async () => {
        const fooBackendOnly = {
            ...brokenReplicationInfo,
            backends: [brokenReplicationInfo.backends[1]],
            storageClass: 'destination,foo',
        };
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(fooBackendOnly) },
        });
        const stats = await remover.run();
        expect(remover.cloudserverClient.putMetadata).not.toHaveBeenCalled();
        expect(stats).toEqual(expect.objectContaining({ updated: 0, reset: 0, manual: 1 }));
    });

    it('should process delete markers', async () => {
        const remover = initRemover({}, {
            pages: [page([], [['key0', 'dm0']])],
            mds: { 'key0/dm0': makeMd(brokenReplicationInfo, { isDeleteMarker: true }) },
        });
        const stats = await remover.run();
        expect(stats.updated).toBe(1);
        expect(writtenReplicationInfo(remover).status).toBe('COMPLETED');
    });

    it('should count an error and keep going', async () => {
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0'], ['key1', 'v1']])],
            mds: { 'key1/v1': makeMd(brokenReplicationInfo) },
            failGet: new Set(['key0/v0']),
        });
        const stats = await remover.run();
        expect(stats.errors).toBe(1);
        expect(stats.updated).toBe(1);
    });

    it('should list all pages', async () => {
        const remover = initRemover({}, {
            pages: [
                page([['key0', 'v0']], [], { key: 'key0', versionId: 'v0' }),
                page([['key1', 'v1']]),
            ],
            mds: {
                'key0/v0': makeMd(brokenReplicationInfo),
                'key1/v1': makeMd(brokenReplicationInfo),
            },
        });
        const stats = await remover.run();
        expect(remover.s3.send).toHaveBeenCalledTimes(2);
        expect(remover.s3.send.mock.calls[1][0].input).toEqual(expect.objectContaining({
            KeyMarker: 'key0', VersionIdMarker: 'v0',
        }));
        expect(stats.updated).toBe(2);
    });

    it('should stop at MAX_UPDATES and keep the resume markers', async () => {
        const remover = initRemover({ buckets: ['bucket0', 'bucket1'], maxUpdates: 1 }, {
            pages: [
                page([['key0', 'v0']], [], { key: 'key0', versionId: 'v0' }),
                page([['key1', 'v1']]),
            ],
            mds: {
                'key0/v0': makeMd(brokenReplicationInfo),
                'key1/v1': makeMd(brokenReplicationInfo),
            },
        });
        const stats = await remover.run();
        expect(remover.s3.send).toHaveBeenCalledTimes(1);
        expect(stats.updated).toBe(1);
        expect(remover._keyMarker).toBe('key0');
        expect(remover._versionIdMarker).toBe('v0');
    });

    it('should resume from the input markers on the first bucket only', async () => {
        const remover = initRemover({
            buckets: ['bucket0', 'bucket1'], keyMarker: 'key0', versionIdMarker: 'v0',
        }, {
            pages: [page([]), page([])],
            mds: {},
        });
        await remover.run();
        expect(remover.s3.send.mock.calls[0][0].input).toEqual(expect.objectContaining({
            Bucket: 'bucket0', KeyMarker: 'key0', VersionIdMarker: 'v0',
        }));
        expect(remover.s3.send.mock.calls[1][0].input).toEqual(expect.objectContaining({
            Bucket: 'bucket1', KeyMarker: undefined, VersionIdMarker: undefined,
        }));
    });

    it('should stop the run on a listing error', async () => {
        const remover = initRemover({}, { pages: [], mds: {} });
        remover.s3.send = jest.fn(() => Promise.reject(new Error('listing failed')));
        await expect(remover.run()).rejects.toThrow('listing failed');
    });
});
