const ReplicationStatusUpdater = require('../../CRR/ReplicationStatusUpdater');

const listVersionRes = {
    IsTruncated: false,
    Versions: [
        {
            ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
            ChecksumAlgorithm: [],
            Size: 3263,
            StorageClass: 'STANDARD',
            Key: 'key0',
            VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
            IsLatest: true,
            LastModified: '2024-01-05T13:11:31.861Z',
            Owner: {
                DisplayName: 'bart',
                ID: '0',
            },
        },
    ],
    DeleteMarkers: [],
    Name: 'bucket0',
    MaxKeys: 1000,
    CommonPrefixes: [],
};

const listVersionWithMarkerRes = {
    IsTruncated: true,
    Versions: [
        {
            ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
            ChecksumAlgorithm: [],
            Size: 3263,
            StorageClass: 'STANDARD',
            Key: 'key0',
            VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
            IsLatest: true,
            LastModified: '2024-01-05T13:11:31.861Z',
            Owner: {
                DisplayName: 'bart',
                ID: '0',
            },
        },
    ],
    DeleteMarkers: [],
    Name: 'bucket0',
    MaxKeys: 1,
    CommonPrefixes: [],
    NextVersionIdMarker: 'aJdO148N3LjN00000000001I4j3QKItW',
    NextKeyMarker: 'key0',
};

const listVersionsRes = {
    IsTruncated: false,
    Versions: [
        {
            ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
            ChecksumAlgorithm: [],
            Size: 3263,
            StorageClass: 'STANDARD',
            Key: 'key0',
            VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
            IsLatest: true,
            LastModified: '2024-01-05T13:11:31.861Z',
            Owner: {
                DisplayName: 'bart',
                ID: '0',
            },
        },
        {
            ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
            ChecksumAlgorithm: [],
            Size: 3263,
            StorageClass: 'STANDARD',
            Key: 'key0',
            VersionId: 'aJdO148N3LjN00000000001I4j3QKItV',
            IsLatest: true,
            LastModified: '2024-01-05T13:11:32.861Z',
            Owner: {
                DisplayName: 'bart',
                ID: '0',
            },
        },
    ],
    DeleteMarkers: [],
    Name: 'bucket0',
    MaxKeys: 1000,
    CommonPrefixes: [],
};

const getBucketReplicationRes = {
    ReplicationConfiguration: {
        Role: 'arn:aws:iam::root:role/s3-replication-role',
        Rules: [
            {
                ID: 'r0',
                Prefix: '',
                Status: 'Enabled',
                Destination: {
                    Bucket: 'arn:aws:s3:::destination',
                    StorageClass: 'aws-location',
                },
            },
        ],
    },
};

const objectMd = {
    'owner-display-name': 'bart',
    'owner-id': 'f2ae8ca93fb44fe7ef409dbfdc0e0873b921fe4183364ede136be8c44756acda',
    'content-length': 3263,
    'content-md5': 'dabcc341ecab339daf766e1cddd5d1bb',
    'x-amz-version-id': 'null',
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'STANDARD',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'x-amz-website-redirect-location': '',
    'acl': {
        Canned: 'private', FULL_CONTROL: [], WRITE_ACP: [], READ: [], READ_ACP: [],
    },
    'key': '',
    'location': [{
        key: '7751DDF49AA3B289C2D261ED1FD09A596D5D3F20',
        size: 3263,
        start: 0,
        dataStoreName: 'us-east-1',
        dataStoreType: 'scality',
        dataStoreETag: '1:dabcc341ecab339daf766e1cddd5d1bb',
    }],
    'isDeleteMarker': false,
    'tags': {},
    'replicationInfo': {
        status: '',
        backends: [],
        content: [],
        destination: '',
        storageClass: '',
        role: '',
        storageType: '',
        dataStoreVersionId: '',
    },
    'dataStoreName': 'us-east-1',
    'originOp': 's3:ObjectCreated:Put',
    'last-modified': '2024-01-05T13:11:31.861Z',
    'md-model-version': 3,
    'versionId': '98295539708053999999RG001  ',
};

const getMetadataRes = {
    Body: JSON.stringify(objectMd),
};

const putMetadataRes = { versionId: '98295539708053999999RG001  ' };

// V2 bucket replication config: one rule per destination, each with an Account field
const getBucketReplicationV2Res = {
    ReplicationConfiguration: {
        Role: 'arn:aws:iam::8765432:role/sourceRole,arn:aws:iam::111111111111:role/repRule',
        Rules: [
            {
                ID: 'rule1-dest-A',
                Filter: { Prefix: '' },
                Priority: 1,
                Status: 'Enabled',
                Destination: {
                    Bucket: 'arn:aws:s3:::bucket-a',
                    StorageClass: 'dest-A',
                    Account: '222222222222',
                },
            },
            {
                ID: 'rule1-dest-B',
                Filter: { Prefix: 'docs/' },
                Priority: 2,
                Status: 'Enabled',
                Destination: {
                    Bucket: 'arn:aws:s3:::bucket-b',
                    StorageClass: 'dest-B',
                    Account: '333333333333',
                },
            },
        ],
    },
};

/**
 * Initializes the ReplicationStatusUpdater class with mock methods for testing.
 *
 * This function creates an instance of the ReplicationStatusUpdater class using the provided configuration.
 * It then replaces certain methods of this instance with Jest mock functions. These mocked methods include
 * the S3 client's generic `send` method for 'listObjectVersions' and 'getBucketReplication', as well as 
 * `getMetadata`, `putMetadata` for the Backbeat client.
 * These mock functions are designed to simulate the behavior of the actual AWS S3 and Backbeat
 * clients without making real API calls, which is useful for isolated testing of the ReplicationStatusUpdater
 * functionality.
 *
 * @param {Object} config - The configuration object used to initialize the ReplicationStatusUpdater instance.
 * @param {Logger} log - The logging object to be used by the ReplicationStatusUpdater.
 * @param {Object} customS3Responses - Optional custom responses for S3 commands.
 * @returns {ReplicationStatusUpdater} An instance of ReplicationStatusUpdater with mocked methods.
 */
function initializeCrrWithMocks(config, log, customS3Responses = {}) {
    if (config.endpoint === undefined) {
        // eslint-disable-next-line no-param-reassign
        config.endpoint = 'http://dummyEndpoint:8000';
    }
    const crr = new ReplicationStatusUpdater(config, log);
    const defaultResponses = {
        ListObjectVersionsCommand: listVersionRes,
        GetBucketReplicationCommand: getBucketReplicationRes,
    };

    const responses = { ...defaultResponses, ...customS3Responses };
    const sendMock = jest.fn(command => {
        const commandName = command.constructor.name;
        if (responses[commandName]) {
            return Promise.resolve(responses[commandName]);
        }
        return Promise.reject(new Error(`Unknown command: ${commandName}`));
    });

    const getMetadataMock = jest.fn((params, cb) => cb(null, getMetadataRes));
    const putMetadataMock = jest.fn((params, cb) => cb(null, putMetadataRes));

    crr.s3.send = sendMock;
    crr.cloudserverclient.getMetadata = getMetadataMock;
    crr.cloudserverclient.putMetadata = putMetadataMock;

    return crr;
}

module.exports = {
    initializeCrrWithMocks,
    listVersionRes,
    listVersionsRes,
    listVersionWithMarkerRes,
    getBucketReplicationRes,
    getBucketReplicationV2Res,
    getMetadataRes,
    putMetadataRes,
    objectMd,
};
