const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');
const { PutObjectCommand, HeadObjectCommand, CreateBucketCommand } = require('@aws-sdk/client-s3');
const { CreatePolicyCommand, CreateUserCommand, CreateAccessKeyCommand, AttachUserPolicyCommand } = require('@aws-sdk/client-iam');
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
    configureCrr,
    removeCrrConfiguration,
} = require('../utils/S3Setup');

const log = new Logger('listObjectsByReplicationStatus:test');


describe('listObjectsByReplicationStatus', () => {
    let vaultClient;
    let accountSource;
    let accountDest;
    let endpoint;

    beforeEach(async () => {
        endpoint = `http://${s3Host}:${s3Port}`;
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

    it('should log an error when using credentials without access to the bucket', async () => {
        // Create a test user with no bucket permissions
        const noPermAccount = await createTestAccount(vaultClient);

        log.info('Testing listObjectsByReplicationStatus with unauthorized key pair');

        // Create a custom logger to capture logs

        const capturedLogs = [];
        const captureLogger = new Logger('s3utils:listObjectsByReplicationStatus:test');
        const originalInfo = captureLogger.info.bind(captureLogger);
        const originalError = captureLogger.error.bind(captureLogger);
        captureLogger.info = function (message, data) {
            capturedLogs.push({ level: 'info', message, ...data });
            return originalInfo(message, data);
        };
        captureLogger.error = function (message, data) {
            capturedLogs.push({ level: 'error', message, ...data });
            return originalError(message, data);
        };

        try {
            // Attempt to list objects by replication status with no permissions
            await listObjectsByReplicationStatus({
                buckets: accountSource.bucketName,
                accessKey: noPermAccount.accountAccessKey,
                secretKey: noPermAccount.accountSecretKey,
                endpoint,
                replicationStatus: 'PENDING', // Choose a valid status
                logger: captureLogger,
            });
        } catch (err) {
            // The function may throw, but regardless we want to inspect the logs
        } finally {
            // Clean up the account
            await deleteTestAccount(vaultClient, noPermAccount);
        }

        const authErrorLogs = capturedLogs.filter(entry =>
            entry && entry.error && entry.error.Code === 'AccessDenied');
        expect(authErrorLogs).toBeDefined();
        expect(authErrorLogs.length).toBe(1);
    });
    it('should log one error per object when an IAM user can list the bucket but cannot head objects', async () => {
        // Create an IAM user in accountSource with a restrictive policy
        const restrictedUserName = `list-only-user-${Date.now()}`;
        const s3ResourceArn = `arn:aws:s3:::${accountSource.bucketName}`;
        const s3ObjectResourceArn = `arn:aws:s3:::${accountSource.bucketName}/*`;

        // Create the user
        await accountSource.iamClient.send(new CreateUserCommand({
            UserName: restrictedUserName,
        }));

        // Create access keys for the new user
        const createAccessKeyResp = await accountSource.iamClient.send(new CreateAccessKeyCommand({
            UserName: restrictedUserName,
        }));
        const accessKeyId = createAccessKeyResp.AccessKey.AccessKeyId;
        const secretAccessKey = createAccessKeyResp.AccessKey.SecretAccessKey;

        // Define a policy to allow only ListBucket (no GetObject, HeadObject)
        const listOnlyPolicyDoc = {
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Allow',
                    Action: [
                        's3:ListBucket'
                    ],
                    Resource: [s3ResourceArn]
                }
                // No object-level permissions on purpose
            ]
        };

        // Create the policy
        const createPolicyResp = await accountSource.iamClient.send(new CreatePolicyCommand({
            PolicyName: `ListOnlyPolicy-${Date.now()}`,
            PolicyDocument: JSON.stringify(listOnlyPolicyDoc),
        }));
        const policyArn = createPolicyResp.Policy.Arn;

        // Attach policy to user
        await accountSource.iamClient.send(new AttachUserPolicyCommand({
            UserName: restrictedUserName,
            PolicyArn: policyArn,
        }));

        // Add objects as the admin
        const testObjects = [
            { Key: 'test-limited-access-object-1.txt', Body: 'object contents for head failure 1' },
            { Key: 'test-limited-access-object-2.txt', Body: 'object contents for head failure 2' },
        ];
        const objectVersions = [];
        for (const obj of testObjects) {
            const putObjectResponse = await accountSource.s3Client.send(new PutObjectCommand({
                Bucket: accountSource.bucketName,
                Key: obj.Key,
                Body: obj.Body,
            }));
            objectVersions.push({ Key: obj.Key, VersionId: putObjectResponse.VersionId });
        }

        // Build a custom logger to capture errors
        const capturedLogs = [];
        const captureLogger = new Logger('s3utils:listObjectsByReplicationStatus:test');
        const originalError = captureLogger.error.bind(captureLogger);
        captureLogger.error = function (message, data) {
            capturedLogs.push({ level: 'error', message, ...(data || {}) });
            return originalError(message, data);
        };

        // Run the function with this user
        try {
            await listObjectsByReplicationStatus({
                buckets: accountSource.bucketName,
                accessKey: accessKeyId,
                secretKey: secretAccessKey,
                endpoint,
                replicationStatus: 'PENDING',
                logger: captureLogger,
            });
        } catch (err) {
            // Swallow error, we're inspecting logs
        } 

        // We expect error logs related to inability to HeadObject
        const objectAccessErrors = capturedLogs.filter(log =>
            log.level === 'error' &&
            log.message === 'error getting object metadata' &&
            log.error && 
                (log.error.$metadata && log.error.$metadata.httpStatusCode === 403) 
        );
        expect(objectAccessErrors).toBeDefined();
        expect(objectAccessErrors.length).toBe(testObjects.length);
    });

    it('should not fail when scanning a non-replicated bucket', async () => {
        // Create a new bucket without replication
        const nonRepBucketName = `${accountSource.bucketName}-norep-${Math.random().toString(36).substr(2, 6)}`;
        await accountSource.s3Client.send(new CreateBucketCommand({
            Bucket: nonRepBucketName,
        }));

        // Put one or more objects in the non-replicated bucket
        const testObjects = [
            { Key: 'unreplicated-obj-1', Body: 'no replica data 1' },
            { Key: 'unreplicated-obj-2', Body: 'no replica data 2' },
        ];

        for (const obj of testObjects) {
            await accountSource.s3Client.send(new PutObjectCommand({
                Bucket: nonRepBucketName,
                Key: obj.Key,
                Body: obj.Body,
            }));
        }

        // Use a capture logger to verify code path
        const capturedLogs = [];
        const captureLogger = new Logger('s3utils:listObjectsByReplicationStatus:test');
        const originalWarn = captureLogger.warn.bind(captureLogger);
        const originalError = captureLogger.error.bind(captureLogger);

        captureLogger.warn = function (message, data) {
            capturedLogs.push({ level: 'warn', message, ...(data || {}) });
            return originalWarn(message, data);
        };
        captureLogger.error = function (message, data) {
            capturedLogs.push({ level: 'error', message, ...(data || {}) });
            return originalError(message, data);
        };

        const endpoint = `http://${s3Host}:${s3Port}`;

        // Should not throw
        let errorCaught = null;
        try {
            await listObjectsByReplicationStatus({
                buckets: nonRepBucketName,
                accessKey: accountSource.accountAccessKey,
                secretKey: accountSource.accountSecretKey,
                endpoint,
                replicationStatus: 'PENDING',
                logger: captureLogger,
            });
        } catch (err) {
            errorCaught = err;
        }

        expect(errorCaught).toBeNull();

        const fatalErrors = capturedLogs.filter(log =>
            log.level === 'error'
        );
        expect(fatalErrors.length).toBe(0);

    });

    it('should return a HTTP 404 when scanning a non-existent bucket', async () => {
        const nonExistentBucket = 'this-bucket-does-not-exist';
        const capturedLogs = [];
        const captureLogger = new Logger('s3utils:listObjectsByReplicationStatus:test');
        const originalError = captureLogger.error.bind(captureLogger);
        captureLogger.error = function (message, data) {
            capturedLogs.push({ level: 'error', message, ...(data || {}) });
            return originalError(message, data);
        };

        const endpoint = `http://${s3Host}:${s3Port}`;
        let errorCaught = null;
        try {
            await listObjectsByReplicationStatus({
                buckets: nonExistentBucket,
                accessKey: accountSource.accountAccessKey,
                secretKey: accountSource.accountSecretKey,
                endpoint,
                replicationStatus: 'PENDING',
                logger: captureLogger,
            });
        } catch (err) {
            errorCaught = err;
        }

        // Should throw or log 404 error (catch either JS error or log capturing it)
        expect(errorCaught && errorCaught.$metadata && errorCaught.$metadata.httpStatusCode === 404).toBe(true);
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
        // Filter out the test object from the previous test (test-limited-access-object.txt)
        const foundObjectLogs = capturedLogs.filter(entry =>
            entry.message === 'object with matching replication status found' 
        );

        log.info('Found object logs', { count: foundObjectLogs.length });
        expect(foundObjectLogs.length).toBe(testObjects.length);

        // Verify that all test objects were found and bucketName is present for each
        const foundKeys = foundObjectLogs.map(entry => entry.data.Key);
        for (const testObj of testObjects) {
            expect(foundKeys).toContain(testObj.Key);
            // Expect the bucketName to be included in the log entry
            const logEntry = foundObjectLogs.find(e => e.data.Key === testObj.Key);
            expect(logEntry.data.bucketName).toBe(accountSource.bucketName);
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
