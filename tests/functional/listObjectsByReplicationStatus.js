const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');
const { PutBucketVersioningCommand, PutBucketReplicationCommand, DeleteBucketReplicationCommand, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { CreatePolicyCommand, CreateRoleCommand, AttachRolePolicyCommand, DetachRolePolicyCommand, DeleteRoleCommand, DeletePolicyCommand, DeleteUserCommand } = require('@aws-sdk/client-iam');
const { listObjectsByReplicationStatus } = require('../../listObjectsByReplicationStatus');

const {
    iamHost,
    iamPort,
    s3Host,
    s3Port,
    adminAccessKeyId,
    adminSecretAccessKey,
    createTestAccount,
    deleteTestAccount,
} = require('../utils/S3Setup');

const log = new Logger('listObjectsByReplicationStatus:test');

async function configureCrr(accountSource, accountDest) {
    // activate bucket versioning on source and destination buckets
    log.info('Enabling bucket versioning on source and destination buckets');
    await accountSource.s3Client.send(new PutBucketVersioningCommand({
        Bucket: accountSource.bucketName,
        VersioningConfiguration: {
            Status: 'Enabled',
        },
    }));

    await accountDest.s3Client.send(new PutBucketVersioningCommand({
        Bucket: accountDest.bucketName,
        VersioningConfiguration: {
            Status: 'Enabled',
        },
    }));

    log.info('Creating IAM policies and roles for CRR');
    // create policy
    const policy = {
        Version:'2012-10-17',
        Statement:[
            {
                Effect:'Allow',
                Action:[
                    's3:GetObjectVersion',
                    's3:GetObjectVersionAcl',
                    's3:ReplicateObject'
                ],
                Resource:[
                    `arn:aws:s3:::${accountSource.bucketName}/*`
                ]
            },
            {
                Effect:'Allow',
                Action:[
                    's3:ListBucket',
                    's3:GetReplicationConfiguration'
                ],
                Resource:[
                    'arn:aws:s3:::source'
                ]
            },
            {
                Effect:'Allow',
                Action:[
                    's3:ReplicateObject',
                    's3:ReplicateDelete'
                ],
                Resource:`arn:aws:s3:::${accountDest.bucketName}/*`
            }
        ]
    };
    await accountSource.iamClient.send(new CreatePolicyCommand({
        PolicyName: 'crr-policy',
        PolicyDocument: JSON.stringify(policy),
    }));

    await accountDest.iamClient.send(new CreatePolicyCommand({
        PolicyName: 'crr-policy',
        PolicyDocument: JSON.stringify(policy),
    }));

    log.info('Creating IAM roles');
    // create trust
    const trust = {
        Version:'2012-10-17',
        Statement:[
            {
                Effect:'Allow',
                Principal:{
                    Service:'backbeat'
                },
                Action:'sts:AssumeRole'
            }
        ]
    };
    await accountSource.iamClient.send(new CreateRoleCommand({
        RoleName: 'crr-trust-role',
        AssumeRolePolicyDocument: JSON.stringify(trust),
    }));
    await accountDest.iamClient.send(new CreateRoleCommand({
        RoleName: 'crr-trust-role',
        AssumeRolePolicyDocument: JSON.stringify(trust),
    }));

    log.info('Attaching policies to roles');
    // attach role to policy
    await accountSource.iamClient.send(new AttachRolePolicyCommand({
        RoleName: 'crr-trust-role',
        PolicyArn: `arn:aws:iam::${accountSource.accountId}:policy/crr-policy`,
    }));
    await accountDest.iamClient.send(new AttachRolePolicyCommand({
        RoleName: 'crr-trust-role',
        PolicyArn: `arn:aws:iam::${accountDest.accountId}:policy/crr-policy`,
    }));

    log.info('Setting bucket replication configuration on source bucket');
    const replication = {
        Role: `arn:aws:iam::${accountSource.accountId}:role/crr-trust-role,arn:aws:iam::${accountDest.accountId}:role/crr-trust-role`,
        Rules: [
            {
                Prefix: '',
                Status: 'Enabled',
                Destination: {
                    Bucket: `arn:aws:s3:::${accountDest.bucketName}`
                }
            }
        ]
    };
    await accountSource.s3Client.send(new PutBucketReplicationCommand({
        Bucket: accountSource.bucketName,
        ReplicationConfiguration: replication
    }));

}

async function removeCrrConfiguration(account) {
    log.info('Removing bucket crr configuration', { bucket: account.bucketName });
    try {
        await account.s3Client.send(new DeleteBucketReplicationCommand({ Bucket: account.bucketName }));
    } catch (err) {
        log.error('Error removing bucket replication configuration', {
            bucket: account.bucketName,
            error: err.message,
        });
    }

    // Clean up IAM resources first (roles and policies must be deleted before account)
    log.info('Cleaning up IAM resources', { account: account.accountName });
    try {
        // Detach policy from role
        await account.iamClient.send(new DetachRolePolicyCommand({
            RoleName: 'crr-trust-role',
            PolicyArn: `arn:aws:iam::${account.accountId}:policy/crr-policy`,
        }));
        log.info('Detached policy from role');
    } catch (err) {
        log.error('Error detaching policy from role', { error: err.message });
    }

    try {
        // Delete role
        await account.iamClient.send(new DeleteRoleCommand({ RoleName: 'crr-trust-role' }));
        log.info('Deleted IAM role');
    } catch (err) {
        log.error('Error deleting role', { error: err.message });
    }

    try {
        // Delete policy
        await account.iamClient.send(new DeletePolicyCommand({
            PolicyArn: `arn:aws:iam::${account.accountId}:policy/crr-policy`,
        }));
        log.info('Deleted IAM policy');
    } catch (err) {
        log.error('Error deleting policy', { error: err.message });
    }

    try {
        // Delete IAM user
        await account.iamClient.send(new DeleteUserCommand({ UserName: account.iamUser }));
        log.info('Deleted IAM user');
    } catch (err) {
        log.error('Error deleting IAM user', { error: err.message });
    }
}


describe('listObjectsByReplicationStatus', () => {
    let vaultClient;
    let accountSource;
    let accountDest;

    beforeAll(async () => {
        vaultClient = new vaultclient.Client(
            iamHost,
            iamPort,
            false,
            undefined,
            undefined,
            undefined,
            undefined,
            adminAccessKeyId,
            adminSecretAccessKey
        );
    });

    beforeEach(async () => {
        log.info('Setting up test accounts and buckets');
        accountSource = await createTestAccount(vaultClient);
        accountDest = await createTestAccount(vaultClient);
        log.info(`Account source: ${accountSource.accountName} and dest: ${accountDest.accountName} were created`);
        log.info('Configuring CRR between source and destination buckets');
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

    it('should list objects by replication status', async () => {
        // Add data to source bucket
        log.info('Uploading test objects to source bucket');
        const testObjects = [
            { Key: 'test-object-1', Body: 'data to replicate 1' },
            { Key: 'test-object-2', Body: 'data to replicate 2' },
            { Key: 'test-object-3', Body: 'data to replicate 3' },
        ];

        for (const obj of testObjects) {
            await accountSource.s3Client.send(new PutObjectCommand({
                Bucket: accountSource.bucketName,
                Key: obj.Key,
                Body: obj.Body,
            }));
            log.info('Uploaded object', { key: obj.Key });
        }

        // Verify objects have replication status
        log.info('Verifying objects have replication status');
        for (const obj of testObjects) {
            const headResult = await accountSource.s3Client.send(new HeadObjectCommand({
                Bucket: accountSource.bucketName,
                Key: obj.Key,
            }));
            log.info('Object metadata', {
                key: obj.Key,
                replicationStatus: headResult.ReplicationStatus,
                versionId: headResult.VersionId
            });
            expect(headResult.ReplicationStatus).toBeDefined();
        }

        // Create a custom logger to capture log entries
        const capturedLogs = [];
        const captureLogger = new Logger('s3utils:listObjectsByReplicationStatus:test');
        const originalInfo = captureLogger.info.bind(captureLogger);
        captureLogger.info = function (message, data) {
            capturedLogs.push({ message, data });
            return originalInfo(message, data);
        };

        // Execute the listObjectsByReplicationStatus function directly
        log.info('Executing listObjectsByReplicationStatus function');
        const endpoint = `http://${s3Host}:${s3Port}`;

        // Call the function directly for coverage
        await listObjectsByReplicationStatus({
            buckets: accountSource.bucketName,
            accessKey: accountSource.accountAccessKey,
            secretKey: accountSource.accountSecretKey,
            endpoint,
            replicationStatus: 'PENDING,FAILED,COMPLETED',
            logger: captureLogger,
        });

        log.info('Function execution completed successfully');

        // Verify that objects were found and logged
        const foundObjectLogs = capturedLogs.filter(entry =>
            entry.message === 'object with matching replication status found'
        );

        log.info('Found object logs', { count: foundObjectLogs.length });
        expect(foundObjectLogs.length).toBe(testObjects.length);

        // Verify that all test objects were found
        const foundKeys = foundObjectLogs.map(entry => entry.data.Key);
        for (const testObj of testObjects) {
            expect(foundKeys).toContain(testObj.Key);
            log.info('Verified object was listed', { key: testObj.Key });
        }

        // Verify that the objects have the expected replication status
        for (const logEntry of foundObjectLogs) {
            expect(['PENDING', 'FAILED', 'COMPLETED']).toContain(logEntry.data.ReplicationStatus);
            log.info('Verified replication status', {
                key: logEntry.data.Key,
                status: logEntry.data.ReplicationStatus
            });
        }
    }, 30000);
});
