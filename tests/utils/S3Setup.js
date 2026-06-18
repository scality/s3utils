const { S3Client, CreateBucketCommand, ListBucketsCommand, ListObjectVersionsCommand, DeleteObjectsCommand, DeleteBucketCommand, PutBucketVersioningCommand, PutBucketReplicationCommand, DeleteBucketReplicationCommand } = require('@aws-sdk/client-s3');
const { IAMClient, CreateUserCommand, CreatePolicyCommand, CreateRoleCommand, AttachRolePolicyCommand, DetachRolePolicyCommand, DeleteRoleCommand, DeletePolicyCommand, DeleteUserCommand, ListRolesCommand, ListAttachedRolePoliciesCommand, ListUsersCommand, ListAttachedUserPoliciesCommand, DetachUserPolicyCommand } = require('@aws-sdk/client-iam');
const { promisify } = require('util');
const { Logger } = require('werelogs');
const admincredentials = require('vaultclient/tests/utils/admincredentials.json');
const crypto = require('crypto');

const log = new Logger('S3Setup');

const iamHost = process.env.IAM_HOST || 'localhost';
const iamPort = process.env.IAM_PORT || 8600;
const s3Host = process.env.S3_HOST || 'localhost';
const s3Port = process.env.S3_PORT || 8000;
const adminAccessKeyId = process.env.ADMIN_ACCESS_KEY_ID || Object.keys(admincredentials)[0];
const adminSecretAccessKey = process.env.ADMIN_SECRET_ACCESS_KEY || admincredentials[adminAccessKeyId];
const region = 'us-east-1';


async function createTestAccount(vaultClient) {
    const iamEndpoint = `http://${iamHost}:${iamPort}`;
    const s3Endpoint = `http://${s3Host}:${s3Port}`;
    const randomSuffix = crypto.randomBytes(4).toString('hex').substring(0, 6);
    const accountName = `test-account-${randomSuffix}`;
    const accountEmail = `${accountName}@example.com`;
    const bucketName = accountName;
    const iamUser = `${accountName}-user`;
    // Create account with vaultclient
    const account = await promisify(vaultClient.createAccount.bind(vaultClient))(
        accountName, { email: accountEmail });
    const accountId = account.account.id;
    // Create account access key
    const credentials = await promisify(vaultClient.generateAccountAccessKey.bind(
        vaultClient))(accountName);
    const accountAccessKey = credentials.id;
    const accountSecretKey = credentials.value;

    const iamClient = new IAMClient({
        credentials: {
            accessKeyId: accountAccessKey,
            secretAccessKey: accountSecretKey
        },
        endpoint: iamEndpoint,
        region,
    });

    const s3Client = new S3Client({
        credentials: {
            accessKeyId: accountAccessKey,
            secretAccessKey: accountSecretKey
        },
        region,
        endpoint: s3Endpoint,
        forcePathStyle: true,
    });

    await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
    await iamClient.send(new CreateUserCommand({ UserName: iamUser }));

    return {
        accountName,
        accountEmail,
        accountId,
        account,
        bucketName,
        accountAccessKey,
        accountSecretKey,
        iamUser,
        iamClient,
        s3Client,
    };

}


async function deleteTestAccount(vaultClient, account) {
    // Delete all buckets in the account
    const bucketsResp = await account.s3Client.send(new ListBucketsCommand({}));
    for (const bucket of (bucketsResp.Buckets || [])) {
        log.info('Deleting bucket', { bucket: bucket.Name });
        // empty bucket - need to delete all versions and delete markers for versioned buckets
        // Note: no pagination — silently misses objects beyond 1000. Fine for test cleanup.
        const listedObjects = await account.s3Client.send(new ListObjectVersionsCommand({ Bucket: bucket.Name }));

        const objectsToDelete = [];

        if (listedObjects.Versions && listedObjects.Versions.length > 0) {
            listedObjects.Versions.forEach(({ Key, VersionId }) => {
                objectsToDelete.push({ Key, VersionId });
            });
        }

        if (listedObjects.DeleteMarkers && listedObjects.DeleteMarkers.length > 0) {
            listedObjects.DeleteMarkers.forEach(({ Key, VersionId }) => {
                objectsToDelete.push({ Key, VersionId });
            });
        }

        if (objectsToDelete.length > 0) {
            await account.s3Client.send(new DeleteObjectsCommand({
                Bucket: bucket.Name,
                Delete: { Objects: objectsToDelete },
            }));
        }

        await account.s3Client.send(new DeleteBucketCommand({ Bucket: bucket.Name }));
        log.info('Deleted bucket', { bucket: bucket.Name });
    }

    // List and delete all IAM users
    try {
        const listUsersResp = await account.iamClient.send(new ListUsersCommand({}));
        if (listUsersResp.Users && Array.isArray(listUsersResp.Users)) {
            for (const user of listUsersResp.Users) {
                // Detach all attached managed policies and delete the policies
                try {
                    const attachedPolicyResp = await account.iamClient.send(new ListAttachedUserPoliciesCommand({
                        UserName: user.UserName
                    }));
                    if (attachedPolicyResp.AttachedPolicies) {
                        for (const pol of attachedPolicyResp.AttachedPolicies) {
                            // Detach the managed policy from the user
                            await account.iamClient.send(new DetachUserPolicyCommand({
                                UserName: user.UserName,
                                PolicyArn: pol.PolicyArn
                            }));
                            log.info('Detached managed policy from IAM user', { iamUser: user.UserName, PolicyArn: pol.PolicyArn });

                            // Try to delete the managed policy (ignore errors if others still attached)
                            try {
                                await account.iamClient.send(new DeletePolicyCommand({
                                    PolicyArn: pol.PolicyArn
                                }));
                                log.info('Deleted managed policy', { PolicyArn: pol.PolicyArn });
                            } catch (delPolErr) {
                                // Policy might be attached to another user or resource, or policy is AWS managed
                                log.info('Could not delete managed policy', { PolicyArn: pol.PolicyArn, error: delPolErr && delPolErr.message });
                            }
                        }
                    }
                } catch (err) {
                    log.error('Error detaching/deleting managed policies from user', { iamUser: user.UserName, error: err });
                }

                // Do not remove inline policies

                await account.iamClient.send(new DeleteUserCommand({ UserName: user.UserName }));
                log.info('Deleted IAM user', { iamUser: user.UserName });
            }
        }
    } catch (err) {
        log.error('Error listing or deleting IAM users', { error: err });
    }

    // List and delete all IAM roles owned by the test account
    try {
        const rolesResp = await account.iamClient.send(new ListRolesCommand({}));

        if (rolesResp.Roles && Array.isArray(rolesResp.Roles)) {
            for (const role of rolesResp.Roles) {
                log.info('Deleting IAM role', { RoleName: role.RoleName });
                try {
                    // Before deleting, need to detach all policies from the role
                    const attachedPolicies = await account.iamClient.send(
                        new ListAttachedRolePoliciesCommand({ RoleName: role.RoleName })
                    );
                    if (attachedPolicies.AttachedPolicies) {
                        for (const policy of attachedPolicies.AttachedPolicies) {
                            await account.iamClient.send(
                                new DetachRolePolicyCommand({
                                    RoleName: role.RoleName,
                                    PolicyArn: policy.PolicyArn
                                })
                            );
                            log.info('Detached policy from role', { RoleName: role.RoleName, PolicyArn: policy.PolicyArn });
                            try {
                                await account.iamClient.send(
                                    new DeletePolicyCommand({ PolicyArn: policy.PolicyArn })
                                );
                                log.info('Deleted policy', { PolicyArn: policy.PolicyArn });
                            } catch (delPolErr) {
                                // Policy may still be attached to another role
                                log.info('Could not delete policy', { PolicyArn: policy.PolicyArn, error: delPolErr && delPolErr.message });
                            }
                        }
                    }
                    await account.iamClient.send(
                        new DeleteRoleCommand({ RoleName: role.RoleName })
                    );
                    log.info('Deleted role', { RoleName: role.RoleName });
                } catch (roleErr) {
                    log.error('Error deleting IAM role', { RoleName: role.RoleName, error: roleErr });
                }
            }
        }
    } catch (err) {
        log.error('Error listing IAM roles', { error: err });
    }

    // Delete account with vaultclient
    await promisify(vaultClient.deleteAccount.bind(vaultClient))(account.accountName);
    log.info('Deleted account', { account: account.accountName });
}


/**
 * Configure CRR between a source and a destination test account.
 * Enables versioning on both buckets, creates the IAM policies/roles
 * required by backbeat, and sets a single-rule replication config on
 * the source bucket.
 *
 * @param {Object} accountSource - object returned by createTestAccount
 * @param {Object} accountDest - object returned by createTestAccount
 * @param {Object} [opts]
 * @param {string} [opts.storageClass] - optional StorageClass on the rule's
 *   Destination (some flows match the destination by StorageClass / siteName).
 *   If omitted, the rule has no StorageClass.
 * @returns {Promise<void>}
 */
async function configureCrr(accountSource, accountDest, opts = {}) {
    log.info('Enabling bucket versioning on source and destination buckets');
    await accountSource.s3Client.send(new PutBucketVersioningCommand({
        Bucket: accountSource.bucketName,
        VersioningConfiguration: { Status: 'Enabled' },
    }));

    await accountDest.s3Client.send(new PutBucketVersioningCommand({
        Bucket: accountDest.bucketName,
        VersioningConfiguration: { Status: 'Enabled' },
    }));

    log.info('Creating IAM policies and roles for CRR');
    const policy = {
        Version: '2012-10-17',
        Statement: [
            {
                Effect: 'Allow',
                Action: [
                    's3:GetObjectVersion',
                    's3:GetObjectVersionAcl',
                    's3:ReplicateObject',
                ],
                Resource: [`arn:aws:s3:::${accountSource.bucketName}/*`],
            },
            {
                Effect: 'Allow',
                Action: ['s3:ListBucket', 's3:GetReplicationConfiguration'],
                Resource: ['arn:aws:s3:::source'],
            },
            {
                Effect: 'Allow',
                Action: ['s3:ReplicateObject', 's3:ReplicateDelete'],
                Resource: `arn:aws:s3:::${accountDest.bucketName}/*`,
            },
        ],
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
    const trust = {
        Version: '2012-10-17',
        Statement: [{
            Effect: 'Allow',
            Principal: { Service: 'backbeat' },
            Action: 'sts:AssumeRole',
        }],
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
    await accountSource.iamClient.send(new AttachRolePolicyCommand({
        RoleName: 'crr-trust-role',
        PolicyArn: `arn:aws:iam::${accountSource.accountId}:policy/crr-policy`,
    }));
    await accountDest.iamClient.send(new AttachRolePolicyCommand({
        RoleName: 'crr-trust-role',
        PolicyArn: `arn:aws:iam::${accountDest.accountId}:policy/crr-policy`,
    }));

    log.info('Setting bucket replication configuration on source bucket');
    const destination = { Bucket: `arn:aws:s3:::${accountDest.bucketName}` };
    if (opts.storageClass) {
        destination.StorageClass = opts.storageClass;
    }
    const replication = {
        Role: `arn:aws:iam::${accountSource.accountId}:role/crr-trust-role,`
            + `arn:aws:iam::${accountDest.accountId}:role/crr-trust-role`,
        Rules: [{ Prefix: '', Status: 'Enabled', Destination: destination }],
    };
    await accountSource.s3Client.send(new PutBucketReplicationCommand({
        Bucket: accountSource.bucketName,
        ReplicationConfiguration: replication,
    }));
}

/**
 * Tear down the CRR configuration set up by configureCrr() for a single
 * account: removes the bucket replication config (if any) and deletes the
 * IAM role/policy/user. Best-effort; logs and swallows individual errors
 * so a partial setup can still be cleaned up.
 */
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

    log.info('Cleaning up IAM resources', { account: account.accountName });
    try {
        await account.iamClient.send(new DetachRolePolicyCommand({
            RoleName: 'crr-trust-role',
            PolicyArn: `arn:aws:iam::${account.accountId}:policy/crr-policy`,
        }));
        log.info('Detached policy from role');
    } catch (err) {
        log.error('Error detaching policy from role', { error: err.message });
    }

    try {
        await account.iamClient.send(new DeleteRoleCommand({ RoleName: 'crr-trust-role' }));
        log.info('Deleted IAM role');
    } catch (err) {
        log.error('Error deleting role', { error: err.message });
    }

    try {
        await account.iamClient.send(new DeletePolicyCommand({
            PolicyArn: `arn:aws:iam::${account.accountId}:policy/crr-policy`,
        }));
        log.info('Deleted IAM policy');
    } catch (err) {
        log.error('Error deleting policy', { error: err.message });
    }

    try {
        await account.iamClient.send(new DeleteUserCommand({ UserName: account.iamUser }));
        log.info('Deleted IAM user');
    } catch (err) {
        log.error('Error deleting IAM user', { error: err.message });
    }
}

module.exports = {
    createTestAccount,
    deleteTestAccount,
    configureCrr,
    removeCrrConfiguration,
    iamHost,
    iamPort,
    s3Host,
    s3Port,
    adminAccessKeyId,
    adminSecretAccessKey,
    region,
};
