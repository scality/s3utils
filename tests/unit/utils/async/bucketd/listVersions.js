jest.mock('../../../../../utils/async/httpRequest');

const httpRequest = require('../../../../../utils/async/httpRequest');
const listVersions = require('../../../../../utils/async/bucketd/listVersions');

const BUCKETD_HOSTPORT = 'localhost:9000';
const BUCKET = 'test-bucket';

// Build a fake httpRequest response object
function fakeResponse(statusCode, body) {
    return { statusCode, body: JSON.stringify(body) };
}

// Build a listing page response body
function listingPage({
    versions = [], isTruncated = false, nextKeyMarker = '', nextVersionIdMarker = '',
} = {}) {
    return {
        Versions: versions,
        IsTruncated: isTruncated,
        NextKeyMarker: nextKeyMarker,
        NextVersionIdMarker: nextVersionIdMarker,
    };
}

// Build a listing entry with minimal metadata inline
function versionEntry(key, versionId, mdOverrides = {}) {
    const md = {
        'content-length': 1024,
        versionId,
        'location': [{ key: 'sproxyd-key' }],
        ...mdOverrides,
    };
    return { key, versionId, value: JSON.stringify(md) };
}

// Collect all items yielded by the async generator into an array
async function collectAll(gen) {
    const items = [];
    for await (const item of gen) {
        items.push(item);
    }
    return items;
}

// Declare the exact sequence of HTTP exchanges expected during a test.
//
// Each exchange is { url, status, body?, retryParams? }. The mock validates
// that every httpRequest call is a GET that hits the declared URL in order
// and returns the declared status code and (optionally) JSON-serialised body.
//
// After the code under test runs, call mock.assertAllConsumed() to verify
// that every declared exchange was actually triggered.
//
// Example:
//   const mock = mockHttpExchanges([
//       { url: listingUrl(), status: 200, body: listingPage({ versions: [...] }) },
//       { url: objectUrl('key1', 'v1'), status: 404 },
//   ]);
//   const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
//   mock.assertAllConsumed();
function mockHttpExchanges(exchanges) {
    let callIndex = 0;
    httpRequest.mockImplementation((method, url, retryParams) => {
        const exchange = exchanges[callIndex];
        if (!exchange) {
            throw new Error(
                `Unexpected httpRequest call #${callIndex + 1}: ${method} ${url}`,
            );
        }
        expect(method).toBe('GET');
        expect(url).toBe(exchange.url);
        expect(retryParams).toStrictEqual(exchange.retryParams);
        callIndex++;
        const body = exchange.body !== undefined ? JSON.stringify(exchange.body) : '';
        return Promise.resolve({ statusCode: exchange.status, body });
    });
    return {
        assertAllConsumed() {
            expect(httpRequest).toHaveBeenCalledTimes(exchanges.length);
        },
    };
}

// ---------------------------------------------------------------------------
// URL builders — mirror the construction in listVersions.js so tests stay
// readable without repeating the concatenation logic inline.
// ---------------------------------------------------------------------------

function listingUrl({
    maxKeys = 1000, prefix = '', keyMarker = '', versionIdMarker = '',
} = {}) {
    return `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}`
        + `?listingType=DelimiterVersions&maxKeys=${maxKeys}${
            prefix ? `&prefix=${encodeURIComponent(prefix)}` : ''
        }&keyMarker=${encodeURIComponent(keyMarker)}`
        + `&versionIdMarker=${encodeURIComponent(versionIdMarker)}`;
}

function objectUrl(key, versionId) {
    const base = `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/${encodeURIComponent(key)}`;
    if (versionId === undefined) {
        return base;
    }
    return `${base}?versionId=${encodeURIComponent(versionId)}`;
}

beforeEach(() => {
    jest.resetAllMocks();
});

describe('listVersions', () => {
    describe('basic listing', () => {
        test('empty bucket yields nothing', async () => {
            httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage()));

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
            expect(items).toHaveLength(0);
        });

        test('single page yields all versions', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl(),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key1', 'vid1'),
                            versionEntry('key2', 'vid2'),
                        ],
                    }),
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
            mock.assertAllConsumed();

            expect(items).toHaveLength(2);
            expect(items[0]).toMatchObject({ key: 'key1', versionId: 'vid1' });
            expect(items[1]).toMatchObject({ key: 'key2', versionId: 'vid2' });
        });
    });

    describe('options', () => {
        test('pageSize controls maxKeys in listing URL', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl({
                        maxKeys: 42,
                    }),
                    status: 200,
                    body: listingPage(),
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET, {
                pageSize: 42,
            }));
            mock.assertAllConsumed();

            expect(items).toHaveLength(0);
        });

        test('prefix is included in listing URL', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl({
                        prefix: 'some/prefix/',
                    }),
                    status: 200,
                    body: listingPage(),
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET, {
                prefix: 'some/prefix/',
            }));
            mock.assertAllConsumed();

            expect(items).toHaveLength(0);
        });

        test('keyMarker and versionIdMarker are passed in the first request URL', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl({
                        keyMarker: 'a/key',
                        versionIdMarker: 'a version',
                    }),
                    status: 200,
                    body: listingPage(),
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET, {
                keyMarker: 'a/key',
                versionIdMarker: 'a version',
            }));
            mock.assertAllConsumed();

            expect(items).toHaveLength(0);
        });

        test('maxItems stops yielding after the limit is reached', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl({
                        maxKeys: 2,
                    }),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key1', 'vid1'),
                            versionEntry('key2', 'vid2'),
                        ],
                        isTruncated: true,
                        nextKeyMarker: 'key2',
                        nextVersionIdMarker: 'vid2',
                    }),
                },
                {
                    url: listingUrl({
                        maxKeys: 1,
                        keyMarker: 'key2',
                        versionIdMarker: 'vid2',
                    }),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key3', 'vid3'),
                        ],
                    }),
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET, {
                pageSize: 2,
                maxItems: 3,
            }));
            mock.assertAllConsumed();

            expect(items).toHaveLength(3);
            expect(items[0]).toMatchObject({ key: 'key1', versionId: 'vid1' });
            expect(items[1]).toMatchObject({ key: 'key2', versionId: 'vid2' });
            expect(items[2]).toMatchObject({ key: 'key3', versionId: 'vid3' });
        });

        test('retry option is forwarded to httpRequest', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl(),
                    status: 200,
                    body: listingPage(),
                    retryParams: {
                        times: 5,
                        interval: 5000,
                    },
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET, {
                retry: {
                    times: 5,
                    interval: 5000,
                },
            }));
            mock.assertAllConsumed();

            expect(items).toHaveLength(0);
        });
    });

    describe('full metadata fetch (large MPU / pruned location)', () => {
        test('fetches full metadata when location is absent and content-length > 0', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl(),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key1', 'vid1'),
                            versionEntry('key2', 'vid2', { location: undefined }),
                        ],
                    }),
                },
                {
                    url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key2`
                        + '?versionId=vid2',
                    status: 200,
                    body: {
                        'content-length': 1024,
                        'versionId': 'vid2',
                        'location': [
                            { key: 'sproxyd-key-1' },
                            { key: 'sproxyd-key-2' },
                        ],
                    },
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
            mock.assertAllConsumed();

            expect(items).toHaveLength(2);
            expect(items[0]).toMatchObject({ key: 'key1', versionId: 'vid1' });
            expect(items[1]).toMatchObject({
                key: 'key2',
                versionId: 'vid2',
                value: {
                    location: [
                        { key: 'sproxyd-key-1' },
                        { key: 'sproxyd-key-2' },
                    ],
                },
            });
        });

        test('skips entry when full metadata returns 404', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl(),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key1', 'vid1'),
                            versionEntry('key2', 'vid2', { location: undefined }),
                        ],
                    }),
                },
                {
                    url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key2`
                        + '?versionId=vid2',
                    status: 404,
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
            mock.assertAllConsumed();

            expect(items).toHaveLength(1);
            expect(items[0]).toMatchObject({ key: 'key1', versionId: 'vid1' });
        });

        test('does NOT fetch full metadata when content-length is 0', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl(),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key1', 'vid1', {
                                'content-length': 0,
                                'location': null,
                            }),
                        ],
                    }),
                },
            ]);

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
            mock.assertAllConsumed();

            expect(items).toHaveLength(1);
            expect(items[0]).toMatchObject({
                key: 'key1',
                versionId: 'vid1',
                value: {
                    location: null,
                },
            });
        });

        describe('non-versioned objects (versionId === "null")', () => {
            test('fetches without versionId query param and returns metadata', async () => {
                const mock = mockHttpExchanges([
                    {
                        url: listingUrl(),
                        status: 200,
                        body: listingPage({
                            versions: [
                                versionEntry('key1', 'null', {
                                    versionId: undefined,
                                    location: undefined,
                                }),
                            ],
                        }),
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`,
                        status: 200,
                        body: {
                            'content-length': 1024,
                            'location': [
                                { key: 'sproxyd-key-1' },
                                { key: 'sproxyd-key-2' },
                            ],
                        },
                    },
                ]);

                const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
                mock.assertAllConsumed();

                expect(items).toHaveLength(1);
                expect(items[0]).toMatchObject({
                    key: 'key1',
                    versionId: 'null',
                    value: {
                        location: [
                            { key: 'sproxyd-key-1' },
                            { key: 'sproxyd-key-2' },
                        ],
                    },
                });
            });

            test('skips when fetched metadata has a versionId field (overwritten by versioned object)', async () => {
                const mock = mockHttpExchanges([
                    {
                        url: listingUrl(),
                        status: 200,
                        body: listingPage({
                            versions: [
                                versionEntry('key1', 'null', {
                                    versionId: undefined,
                                    location: undefined,
                                }),
                            ],
                        }),
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`,
                        status: 200,
                        body: {
                            'content-length': 1024,
                            'versionId': 'newvid',
                            'location': [
                                { key: 'sproxyd-key-1' },
                                { key: 'sproxyd-key-2' },
                            ],
                        },
                    },
                ]);

                const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
                mock.assertAllConsumed();

                expect(items).toHaveLength(0);
            });
        });

        describe('null-version fallback (isNull in listing metadata)', () => {
            test('uses primary versionId URL if it exists', async () => {
                const mock = mockHttpExchanges([
                    {
                        url: listingUrl(),
                        status: 200,
                        body: listingPage({
                            versions: [
                                versionEntry('key1', 'vid1', {
                                    versionId: 'vid1',
                                    location: undefined,
                                    isNull: true,
                                }),
                            ],
                        }),
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`
                            + '?versionId=vid1',
                        status: 200,
                        body: {
                            'content-length': 1024,
                            'versionId': 'vid1',
                            'isNull': true,
                            'location': [
                                { key: 'sproxyd-key-1' },
                                { key: 'sproxyd-key-2' },
                            ],
                        },
                    },
                ]);

                const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
                mock.assertAllConsumed();

                expect(items).toHaveLength(1);
                expect(items[0]).toMatchObject({
                    key: 'key1',
                    versionId: 'vid1',
                    value: {
                        location: [
                            { key: 'sproxyd-key-1' },
                            { key: 'sproxyd-key-2' },
                        ],
                    },
                });
            });

            test('falls back to master-key URL when primary versionId URL returns 404', async () => {
                const mock = mockHttpExchanges([
                    {
                        url: listingUrl(),
                        status: 200,
                        body: listingPage({
                            versions: [
                                versionEntry('key1', 'vid1', {
                                    versionId: 'vid1',
                                    location: undefined,
                                    isNull: true,
                                }),
                            ],
                        }),
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`
                            + '?versionId=vid1',
                        status: 404,
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`,
                        status: 200,
                        body: {
                            'content-length': 1024,
                            'versionId': 'vid1',
                            'isNull': true,
                            'location': [
                                { key: 'sproxyd-key-1' },
                                { key: 'sproxyd-key-2' },
                            ],
                        },
                    },
                ]);

                const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
                mock.assertAllConsumed();

                expect(items).toHaveLength(1);
                expect(items[0]).toMatchObject({
                    key: 'key1',
                    versionId: 'vid1',
                    value: {
                        location: [
                            { key: 'sproxyd-key-1' },
                            { key: 'sproxyd-key-2' },
                        ],
                    },
                });
            });

            test('falls back to ?versionId=null URL when master-key versionId does not match', async () => {
                const mock = mockHttpExchanges([
                    {
                        url: listingUrl(),
                        status: 200,
                        body: listingPage({
                            versions: [
                                versionEntry('key1', 'vid1', {
                                    versionId: 'vid1',
                                    location: undefined,
                                    isNull: true,
                                }),
                            ],
                        }),
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`
                            + '?versionId=vid1',
                        status: 404,
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`,
                        status: 200,
                        body: {
                            'content-length': 1024,
                            'versionId': 'othervid',
                            'location': [{ key: 'sproxyd-key' }],
                        },
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`
                            + '?versionId=null',
                        status: 200,
                        body: {
                            'content-length': 1024,
                            'versionId': 'vid1',
                            'isNull': true,
                            'location': [
                                { key: 'sproxyd-key-1' },
                                { key: 'sproxyd-key-2' },
                            ],
                        },
                    },
                ]);

                const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
                mock.assertAllConsumed();

                expect(items).toHaveLength(1);
                expect(items[0]).toMatchObject({
                    key: 'key1',
                    versionId: 'vid1',
                    value: {
                        location: [
                            { key: 'sproxyd-key-1' },
                            { key: 'sproxyd-key-2' },
                        ],
                    },
                });
            });

            test('skips when all fallback URLs fail to match', async () => {
                const mock = mockHttpExchanges([
                    {
                        url: listingUrl(),
                        status: 200,
                        body: listingPage({
                            versions: [
                                versionEntry('key1', 'vid1', {
                                    versionId: 'vid1',
                                    location: undefined,
                                    isNull: true,
                                }),
                            ],
                        }),
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`
                            + '?versionId=vid1',
                        status: 404,
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`,
                        status: 404,
                    },
                    {
                        url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key1`
                            + '?versionId=null',
                        status: 404,
                    },
                ]);

                const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
                mock.assertAllConsumed();

                expect(items).toHaveLength(0);
            });
        });
    });

    describe('error handling', () => {
        test('throws when listing page returns non-200 status', async () => {
            httpRequest.mockResolvedValueOnce(fakeResponse(500));

            await expect(collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET)))
                .rejects.toThrow('returned status 500');
        });

        test('skips entries with malformed (non-JSON) metadata in listing response', async () => {
            httpRequest.mockResolvedValueOnce(fakeResponse(200, listingPage({
                versions: [
                    versionEntry('key1', 'vid1'),
                    {
                        key: 'key2',
                        versionId: 'vid2',
                        value: '{NOTJSON}',
                    },
                ],
            })));

            const items = await collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET));
            expect(items).toHaveLength(1);
            expect(items[0]).toMatchObject({
                key: 'key1',
                versionId: 'vid1',
                value: {
                    location: [{ key: 'sproxyd-key' }],
                },
            });
        });

        test('throws when full metadata fetch returns non-200 and non-404 status', async () => {
            const mock = mockHttpExchanges([
                {
                    url: listingUrl(),
                    status: 200,
                    body: listingPage({
                        versions: [
                            versionEntry('key1', 'vid1'),
                            versionEntry('key2', 'vid2', { location: undefined }),
                        ],
                    }),
                },
                {
                    url: `http://${BUCKETD_HOSTPORT}/default/bucket/${BUCKET}/key2`
                        + '?versionId=vid2',
                    status: 503,
                },
            ]);

            await expect(collectAll(listVersions(BUCKETD_HOSTPORT, BUCKET)))
                .rejects.toThrow('returned status 503');
        });
    });
});
