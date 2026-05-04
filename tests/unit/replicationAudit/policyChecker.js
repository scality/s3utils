const {
    parseLeaderAddress,
    policyAllowsReplication,
} = require('../../../replicationAudit/check-replication-permissions');

describe('parseLeaderAddress', () => {
    test('ip only defaults port to 4300', () => {
        expect(parseLeaderAddress('10.0.0.1')).toEqual({ ip: '10.0.0.1', port: 4300 });
    });

    test('ip:port extracts both', () => {
        expect(parseLeaderAddress('10.0.0.1:4301')).toEqual({ ip: '10.0.0.1', port: 4301 });
    });

    test('undefined defaults to 127.0.0.1:4300', () => {
        expect(parseLeaderAddress(undefined)).toEqual({ ip: '127.0.0.1', port: 4300 });
    });

    test('empty string defaults to 127.0.0.1:4300', () => {
        expect(parseLeaderAddress('')).toEqual({ ip: '127.0.0.1', port: 4300 });
    });

    test('custom high port', () => {
        expect(parseLeaderAddress('172.16.0.5:4304')).toEqual({ ip: '172.16.0.5', port: 4304 });
    });
});

describe('policyAllowsReplication', () => {
    const bucketName = 'source-bucket';

    describe('valid policies (should allow)', () => {
        test('policy with explicit s3:ReplicateObject and matching bucket', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with s3:* wildcard action', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:*',
                    Resource: '*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with full wildcard (*) action', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: '*',
                    Resource: '*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with s3:Replicate* wildcard', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:Replicate*',
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with action array including s3:ReplicateObject', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: [
                        's3:GetObjectVersion',
                        's3:GetObjectVersionAcl',
                        's3:ReplicateObject',
                        's3:ReplicateDelete',
                    ],
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with resource array including matching bucket', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: [
                        'arn:aws:s3:::other-bucket/*',
                        'arn:aws:s3:::source-bucket/*',
                    ],
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with multiple statements, one matching', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [
                    {
                        Effect: 'Allow',
                        Action: 's3:ListBucket',
                        Resource: 'arn:aws:s3:::source-bucket',
                    },
                    {
                        Effect: 'Allow',
                        Action: 's3:ReplicateObject',
                        Resource: 'arn:aws:s3:::source-bucket/*',
                    },
                ],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with arn:aws:s3:::* resource wildcard', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });

        test('policy with exact bucket ARN (no /* suffix)', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::source-bucket',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(true);
        });
    });

    describe('invalid policies (should reject)', () => {
        test('policy with Deny effect', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Deny',
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('policy missing s3:ReplicateObject action', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: [
                        's3:GetObjectVersion',
                        's3:GetObjectVersionAcl',
                    ],
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('policy with wrong bucket resource', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::other-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('policy with empty Statement array', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('policy with no Statement property', () => {
            const policy = {
                Version: '2012-10-17',
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('empty policy object', () => {
            expect(policyAllowsReplication({}, bucketName)).toBe(false);
        });

        test('policy with only bucket-level actions (no object actions)', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: [
                        's3:ListBucket',
                        's3:GetReplicationConfiguration',
                    ],
                    Resource: 'arn:aws:s3:::source-bucket',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('policy with s3:ReplicateDelete but not s3:ReplicateObject', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateDelete',
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('policy with partial bucket name match', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::source-bucket-backup/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });
    });

    describe('edge cases', () => {
        test('statement with missing Effect defaults to implicit deny', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Action: 's3:ReplicateObject',
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('statement with missing Action', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('statement with missing Resource', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('statement with null Action', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: null,
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('statement with empty Action array', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: [],
                    Resource: 'arn:aws:s3:::source-bucket/*',
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });

        test('statement with empty Resource array', () => {
            const policy = {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Action: 's3:ReplicateObject',
                    Resource: [],
                }],
            };
            expect(policyAllowsReplication(policy, bucketName)).toBe(false);
        });
    });
});
