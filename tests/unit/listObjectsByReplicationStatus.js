const {
    listObjectsByReplicationStatus,
    ERR_NO_BUCKETS,
    ERR_NO_ENDPOINT,
    ERR_NO_ACCESS_KEY,
    ERR_NO_SECRET_KEY,
    ERR_REPLICATION_STATUS_NOT_DEFINED,
    ERR_INVALID_REPLICATION_STATUS,
} = require('../../listObjectsByReplicationStatus');

describe('listObjectsByReplicationStatus - Input Validation', () => {
    const validOptions = {
        buckets: 'test-bucket',
        accessKey: 'test-access-key',
        secretKey: 'test-secret-key',
        endpoint: 'http://localhost:8000',
        replicationStatus: 'PENDING',
    };

    const missingFieldTests = [
        { field: 'buckets', expectedError: ERR_NO_BUCKETS },
        { field: 'endpoint', expectedError: ERR_NO_ENDPOINT },
        { field: 'accessKey', expectedError: ERR_NO_ACCESS_KEY },
        { field: 'secretKey', expectedError: ERR_NO_SECRET_KEY },
        { field: 'replicationStatus', expectedError: ERR_REPLICATION_STATUS_NOT_DEFINED },
    ];

    missingFieldTests.forEach(({ field, expectedError }) => {
        it(`should reject when ${field} is missing`, async () => {
            const options = { ...validOptions };
            delete options[field];
            await expect(listObjectsByReplicationStatus(options))
                .rejects
                .toThrow(expectedError);
        });
    });

    it('should reject when buckets is empty string', async () => {
        const options = { ...validOptions, buckets: '' };
        await expect(listObjectsByReplicationStatus(options))
            .rejects
            .toThrow(ERR_NO_BUCKETS);
    });

    it('should reject when buckets is only whitespace', async () => {
        const options = { ...validOptions, buckets: '   ' };
        await expect(listObjectsByReplicationStatus(options))
            .rejects
            .toThrow(ERR_NO_BUCKETS);
    });

    it('should reject when replicationStatus contains invalid status', async () => {
        const options = { ...validOptions, replicationStatus: 'INVALID_STATUS' };
        await expect(listObjectsByReplicationStatus(options))
            .rejects
            .toThrow(ERR_INVALID_REPLICATION_STATUS);
    });

    it('should reject when replicationStatus contains mix of valid and invalid statuses', async () => {
        const options = { ...validOptions, replicationStatus: 'PENDING,INVALID,COMPLETED' };
        await expect(listObjectsByReplicationStatus(options))
            .rejects
            .toThrow(ERR_INVALID_REPLICATION_STATUS);
    });

    it('should accept all valid replication statuses', async () => {
        const options = {
            ...validOptions,
            replicationStatus: 'NEW,PENDING,COMPLETED,FAILED,REPLICA',
        };
        await expect(listObjectsByReplicationStatus(options))
            .rejects
            .not.toThrow('invalid REPLICATION_STATUS');
    });
});
