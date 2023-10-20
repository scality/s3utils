const xml2js = require('xml2js');
const { recoverListing } = require('../../../utils/safeList');


describe('test safeListObjectVersions', () => {
    const testCases = [
        {
            description: 'should parse valid empty response',
            response: {
                ListVersionsResult: {
                    Name: 'my-bucket',
                    IsTruncated: false,
                    MaxKeys: 1000,
                },
            },
            expectedData: {
                Name: 'my-bucket',
                IsTruncated: false,
                MaxKeys: 1000,
                Versions: [],
                DeleteMarkers: [],
                CommonPrefixes: [],
            },
        },
        {
            description: 'should return error for invalid empty response',
            response: {},
            expectedError: new Error('im an original error'),
        },
        {
            description: 'should parse truncated response',
            response: {
                ListVersionsResult: {
                    Name: 'my-bucket',
                    IsTruncated: true,
                    MaxKeys: 1000,
                    NextKeyMarker: 'my-next-key-marker',
                    NextVersionIdMarker: 'my-next-version-id-marker',
                    Version: [
                        {
                            Key: 'my-key',
                            VersionId: 'my-version-id',
                            IsLatest: true,
                            LastModified: '2021-01-01T00:00:00.000Z',
                            ETag: 'my-etag',
                            Size: 123,
                            StorageClass: 'STANDARD',
                            Owner: {
                                ID: 'my-id',
                                DisplayName: 'my-display-name',
                            },
                        },
                    ],
                    DeleteMarker: [
                        {
                            Key: 'my-deleted-key',
                            VersionId: 'my-deleted-version-id',
                            IsLatest: true,
                            LastModified: '2021-01-01T00:00:00.000Z',
                            ETag: 'my-etag',
                            Owner: {
                                ID: 'my-id',
                                DisplayName: 'my-display-name',
                            },
                        },
                    ],
                    CommonPrefix: [
                        {
                            Prefix: 'my-prefix',
                        },
                    ],
                },
            },
            expectedData: {
                Name: 'my-bucket',
                IsTruncated: true,
                MaxKeys: 1000,
                NextKeyMarker: 'my-next-key-marker',
                NextVersionIdMarker: 'my-next-version-id-marker',
                Versions: [
                    {
                        Key: 'my-key',
                        VersionId: 'my-version-id',
                        IsLatest: true,
                        LastModified: new Date('2021-01-01T00:00:00.000Z'),
                        ETag: 'my-etag',
                        Size: 123,
                        StorageClass: 'STANDARD',
                        Owner: {
                            ID: 'my-id',
                            DisplayName: 'my-display-name',
                        },
                    },
                ],
                DeleteMarkers: [
                    {
                        Key: 'my-deleted-key',
                        VersionId: 'my-deleted-version-id',
                        IsLatest: true,
                        LastModified: new Date('2021-01-01T00:00:00.000Z'),
                        ETag: 'my-etag',
                        Owner: {
                            ID: 'my-id',
                            DisplayName: 'my-display-name',
                        },
                    },
                ],
                CommonPrefixes: [
                    {
                        Prefix: 'my-prefix',
                    },
                ],
            },
        },
        {
            description: 'should recover bad last-modified date',
            response: {
                ListVersionsResult: {
                    Name: 'my-bucket',
                    IsTruncated: true,
                    MaxKeys: 1000,
                    NextKeyMarker: 'my-next-key-marker',
                    NextVersionIdMarker: 'my-next-version-id-marker',
                    Version: [
                        {
                            Key: 'my-key',
                            VersionId: 'my-version-id',
                            IsLatest: true,
                            LastModified: 'undefined',
                            ETag: 'my-etag',
                            Size: 123,
                            StorageClass: 'STANDARD',
                            Owner: {
                                ID: 'my-id',
                                DisplayName: 'my-display-name',
                            },
                        },
                    ],
                    DeleteMarker: [
                        // LastModified is missing
                        {
                            Key: 'my-deleted-key',
                            VersionId: 'my-deleted-version-id',
                            IsLatest: true,
                            ETag: 'my-etag',
                            Owner: {
                                ID: 'my-id',
                                DisplayName: 'my-display-name',
                            },
                        },
                    ],
                    CommonPrefix: [],
                },

            },
            expectedData: {
                Name: 'my-bucket',
                IsTruncated: true,
                MaxKeys: 1000,
                NextKeyMarker: 'my-next-key-marker',
                NextVersionIdMarker: 'my-next-version-id-marker',
                Versions: [
                    {
                        Key: 'my-key',
                        VersionId: 'my-version-id',
                        IsLatest: true,
                        LastModified: 'undefined',
                        ETag: 'my-etag',
                        Size: 123,
                        StorageClass: 'STANDARD',
                        Owner: {
                            ID: 'my-id',
                            DisplayName: 'my-display-name',
                        },
                    },
                ],
                DeleteMarkers: [
                    {
                        Key: 'my-deleted-key',
                        VersionId: 'my-deleted-version-id',
                        IsLatest: true,
                        ETag: 'my-etag',
                        Owner: {
                            ID: 'my-id',
                            DisplayName: 'my-display-name',
                        },
                    },
                ],
                CommonPrefixes: [],
            },
        },
        {
            description: 'should recover bad content-length',
            response: {
                ListVersionsResult: {
                    Name: 'my-bucket',
                    IsTruncated: true,
                    MaxKeys: 1000,
                    NextKeyMarker: 'my-next-key-marker',
                    NextVersionIdMarker: 'my-next-version-id-marker',
                    Version: [
                        {
                            Key: 'my-key',
                            VersionId: 'my-version-id',
                            IsLatest: true,
                            LastModified: '2021-01-01T00:00:00.000Z',
                            ETag: 'my-etag',
                            Size: 'foobar',
                            StorageClass: 'STANDARD',
                            Owner: {
                                ID: 'my-id',
                                DisplayName: 'my-display-name',
                            },
                        },
                    ],
                    DeleteMarker: [
                        {
                            Key: 'my-deleted-key',
                            VersionId: 'my-deleted-version-id',
                            IsLatest: true,
                            LastModified: '2021-01-01T00:00:00.000Z',
                            ETag: 'my-etag',
                            Owner: {
                                ID: 'my-id',
                                DisplayName: 'my-display-name',
                            },
                        },
                    ],
                    CommonPrefix: [],
                },
            },
            expectedData: {
                Name: 'my-bucket',
                IsTruncated: true,
                MaxKeys: 1000,
                NextKeyMarker: 'my-next-key-marker',
                NextVersionIdMarker: 'my-next-version-id-marker',
                Versions: [
                    {
                        Key: 'my-key',
                        VersionId: 'my-version-id',
                        IsLatest: true,
                        LastModified: new Date('2021-01-01T00:00:00.000Z'),
                        ETag: 'my-etag',
                        Size: 'foobar',
                        StorageClass: 'STANDARD',
                        Owner: {
                            ID: 'my-id',
                            DisplayName: 'my-display-name',
                        },
                    },
                ],
                DeleteMarkers: [
                    {
                        Key: 'my-deleted-key',
                        VersionId: 'my-deleted-version-id',
                        IsLatest: true,
                        LastModified: new Date('2021-01-01T00:00:00.000Z'),
                        ETag: 'my-etag',
                        Owner: {
                            ID: 'my-id',
                            DisplayName: 'my-display-name',
                        },
                    },
                ],
                CommonPrefixes: [],
            },
        },
        {
            description: 'should pass through original error is response is not recoverable',
            response: {},
            expectedError: new Error('im an original error'),
        },
    ];


    testCases.forEach(testCase => {
        it(testCase.description, done => {
            const xmlBuilder = new xml2js.Builder();
            const body = Buffer.from(xmlBuilder.buildObject(testCase.response));
            const resp = {
                httpResponse: {
                    body,
                },
                error: testCase.expectedError || null,
            };

            recoverListing(resp, (err, data) => {
                expect(err).toStrictEqual(testCase.expectedError || null);
                expect(data).toStrictEqual(testCase.expectedData);
                done();
            });
        });
    });

    it('should pass through original error if the XML fails to parse', done => {
        const error = new Error('im an original error');
        const resp = {
            httpResponse: {
                body: Buffer.from('im not xml'),
            },
            error,
        };

        recoverListing(resp, (err, data) => {
            expect(err).toStrictEqual(error);
            expect(data).toStrictEqual(undefined);
            done();
        });
    });
});
