const werelogs = require('werelogs');

const ReplicationSiteRemover = require('../../../CRR/ReplicationSiteRemover');
const { objectMd } = require('../../utils/crr');

const { getGlobalStatus, removeSiteFromLists, getEmptyReplicationInfo } = ReplicationSiteRemover;

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
 * @param {Array<Object>} [opts.rules] - Bucket replication rules (default: one rule without
 * StorageClass). null: the bucket has no replication configuration.
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
    const rules = opts.rules === undefined
        ? [{ Prefix: '', Status: 'Enabled', Destination: { Bucket: 'arn:aws:s3:::destination' } }]
        : opts.rules;
    remover.s3.send = jest.fn(command => {
        if (command.constructor.name === 'GetBucketReplicationCommand') {
            if (rules === null) {
                const err = new Error('The replication configuration was not found');
                err.name = 'ReplicationConfigurationNotFoundError';
                return Promise.reject(err);
            }
            return Promise.resolve({ ReplicationConfiguration: { Role: 'role', Rules: rules } });
        }
        return Promise.resolve(pages.shift());
    });
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

function listCalls(remover) {
    return remover.s3.send.mock.calls
        .filter(([command]) => command.constructor.name === 'ListObjectVersionsCommand');
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

    it('getGlobalStatus: a replica stays REPLICA', () => {
        expect(getGlobalStatus([{ status: 'REPLICA' }])).toBe('REPLICA');
        // the other backends are a frozen copy of the source (BB-875)
        expect(getGlobalStatus([{ status: 'PENDING' }, { status: 'REPLICA' }])).toBe('REPLICA');
        expect(getGlobalStatus([{ status: 'COMPLETED' }, { status: 'REPLICA' }])).toBe('REPLICA');
        expect(getGlobalStatus([{ status: 'FAILED' }, { status: 'REPLICA' }])).toBe('REPLICA');
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

    it('getEmptyReplicationInfo: returns a fresh empty copy each time', () => {
        const first = getEmptyReplicationInfo();
        first.backends.push({ site: 'foo' });
        const second = getEmptyReplicationInfo();
        expect(second.status).toBe('');
        expect(second.backends).toEqual([]);
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
            scanned: 1, updated: 1, reset: 0, skipped: 0, errors: 0,
        });
        const repInfo = writtenReplicationInfo(remover);
        expect(repInfo.status).toBe('COMPLETED');
        expect(repInfo.storageClass).toBe('destination');
        expect(repInfo.backends).toEqual([
            { site: 'destination', status: 'COMPLETED', dataStoreVersionId: '' },
        ]);
    });

    it('should keep REPLICA on a replica', async () => {
        // replica on the DR site, broken by a run with SITE_NAME=foo
        const brokenReplica = {
            ...brokenReplicationInfo,
            backends: [
                { site: 'destination', status: 'REPLICA', dataStoreVersionId: '' },
                brokenReplicationInfo.backends[1],
            ],
        };
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplica) },
        });
        await remover.run();
        const repInfo = writtenReplicationInfo(remover);
        expect(repInfo.status).toBe('REPLICA');
        expect(repInfo.backends).toEqual([{ site: 'destination', status: 'REPLICA', dataStoreVersionId: '' }]);
    });

    it('should keep REPLICA on a replica of a multi-destination source', async () => {
        // S3C 9.5 replica on "primary" with a frozen dr=PENDING, then a run with SITE_NAME=foo
        const brokenReplica = {
            ...brokenReplicationInfo,
            status: 'PENDING',
            backends: [
                { site: 'dr', status: 'PENDING', dataStoreVersionId: '' },
                { site: 'primary', status: 'REPLICA', dataStoreVersionId: '' },
                { site: 'foo', status: 'PENDING', dataStoreVersionId: '' },
            ],
            storageClass: 'dr,primary,foo',
        };
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplica) },
        });
        await remover.run();
        const repInfo = writtenReplicationInfo(remover);
        expect(repInfo.status).toBe('REPLICA');
        expect(repInfo.storageClass).toBe('dr,primary');
        expect(repInfo.backends).toEqual([
            { site: 'dr', status: 'PENDING', dataStoreVersionId: '' },
            { site: 'primary', status: 'REPLICA', dataStoreVersionId: '' },
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
        expect(stats).toEqual(expect.objectContaining({ updated: 1, reset: 1 }));
        // same as an object never set up for replication, in the installed arsenal format
        expect(writtenReplicationInfo(remover)).toEqual(JSON.parse(JSON.stringify(getEmptyReplicationInfo())));
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
        expect(listCalls(remover)).toHaveLength(2);
        expect(listCalls(remover)[1][0].input).toEqual(expect.objectContaining({
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
        expect(listCalls(remover)).toHaveLength(1);
        expect(stats.updated).toBe(1);
        expect(remover._keyMarker).toBe('key0');
        expect(remover._versionIdMarker).toBe('v0');
    });

    it('should stop at MAX_SCANNED even when nothing is updated', async () => {
        const clean = {
            ...brokenReplicationInfo,
            status: 'COMPLETED',
            backends: [brokenReplicationInfo.backends[0]],
            storageClass: 'destination',
        };
        const remover = initRemover({ buckets: ['bucket0', 'bucket1'], maxScanned: 1 }, {
            pages: [
                page([['key0', 'v0']], [], { key: 'key0', versionId: 'v0' }),
                page([['key1', 'v1']]),
            ],
            mds: {
                'key0/v0': makeMd(clean),
                'key1/v1': makeMd(clean),
            },
        });
        const stats = await remover.run();
        expect(listCalls(remover)).toHaveLength(1);
        expect(stats).toEqual(expect.objectContaining({ scanned: 1, updated: 0 }));
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
        expect(listCalls(remover)[0][0].input).toEqual(expect.objectContaining({
            Bucket: 'bucket0', KeyMarker: 'key0', VersionIdMarker: 'v0',
        }));
        expect(listCalls(remover)[1][0].input).toEqual(expect.objectContaining({
            Bucket: 'bucket1', KeyMarker: undefined, VersionIdMarker: undefined,
        }));
    });

    it('should stop the run on a listing error', async () => {
        const remover = initRemover({}, { pages: [], mds: {} });
        remover.s3.send = jest.fn(() => Promise.reject(new Error('listing failed')));
        await expect(remover.run()).rejects.toThrow('listing failed');
    });
    it('should keep isReplica when resetting a replica', async () => {
        // S3C 10.1 replica, then a run with SITE_NAME=foo: foo is its only site
        const replicaWithOnlyFoo = {
            ...brokenReplicationInfo,
            backends: [brokenReplicationInfo.backends[1]],
            storageClass: 'foo',
            isReplica: true,
        };
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(replicaWithOnlyFoo) },
        });
        const stats = await remover.run();
        expect(stats.reset).toBe(1);
        const repInfo = writtenReplicationInfo(remover);
        expect(repInfo.isReplica).toBe(true);
        expect(repInfo.backends).toEqual([]);
        expect(repInfo.status).toBe('');
    });

    it('should refuse to remove a site named in the bucket rule', async () => {
        const remover = initRemover({ siteToRemove: 'destination' }, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplicationInfo) },
            rules: [{ Prefix: '', Status: 'Enabled',
                Destination: { Bucket: 'arn:aws:s3:::destination', StorageClass: 'destination' } }],
        });
        await expect(remover.run()).rejects.toThrow('SITE_TO_REMOVE "destination" is a destination');
        expect(listCalls(remover)).toHaveLength(0);
        expect(remover.cloudserverClient.putMetadata).not.toHaveBeenCalled();
    });

    it('should remove a site named in the bucket rule with forceRemoveConfiguredSite', async () => {
        const remover = initRemover({ siteToRemove: 'destination', forceRemoveConfiguredSite: true }, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplicationInfo) },
            rules: [{ Prefix: '', Status: 'Enabled',
                Destination: { Bucket: 'arn:aws:s3:::destination', StorageClass: 'destination' } }],
        });
        const stats = await remover.run();
        expect(stats.updated).toBe(1);
        expect(writtenReplicationInfo(remover).backends.map(b => b.site)).toEqual(['foo']);
    });

    it('should process a bucket without replication configuration', async () => {
        const remover = initRemover({}, {
            pages: [page([['key0', 'v0']])],
            mds: { 'key0/v0': makeMd(brokenReplicationInfo) },
            rules: null,
        });
        const stats = await remover.run();
        expect(stats.updated).toBe(1);
    });
});
