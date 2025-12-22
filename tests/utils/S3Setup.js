const { S3Client, CreateBucketCommand, ListObjectVersionsCommand, DeleteObjectsCommand, DeleteBucketCommand, PutBucketVersioningCommand, PutBucketReplicationCommand, DeleteBucketReplicationCommand, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { IAMClient, CreateUserCommand, CreatePolicyCommand, CreateRoleCommand, AttachRolePolicyCommand, DetachRolePolicyCommand, DeleteRoleCommand, DeletePolicyCommand, DeleteUserCommand } = require('@aws-sdk/client-iam');
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
    // Delete bucket
    log.info('Deleting bucket', { bucket: account.bucketName });
    // empty bucket - need to delete all versions and delete markers for versioned buckets
    const listedObjects = await account.s3Client.send(new ListObjectVersionsCommand({ Bucket: account.bucketName }));

    const objectsToDelete = [];

    // Add all object versions
    if (listedObjects.Versions && listedObjects.Versions.length > 0) {
        listedObjects.Versions.forEach(({ Key, VersionId }) => {
            log.info('Scheduling object version for deletion', { key: Key, versionId: VersionId });
            objectsToDelete.push({ Key, VersionId });
        });
    }

    // Add all delete markers
    if (listedObjects.DeleteMarkers && listedObjects.DeleteMarkers.length > 0) {
        listedObjects.DeleteMarkers.forEach(({ Key, VersionId }) => {
            log.info('Scheduling delete marker for deletion', { key: Key, versionId: VersionId });
            objectsToDelete.push({ Key, VersionId });
        });
    }

    // Delete all versions and markers
    if (objectsToDelete.length > 0) {
        const deleteParams = {
            Bucket: account.bucketName,
            Delete: { Objects: objectsToDelete }
        };
        await account.s3Client.send(new DeleteObjectsCommand(deleteParams));
    }

    await account.s3Client.send(new DeleteBucketCommand({ Bucket: account.bucketName }));
    log.info('Deleted bucket', { bucket: account.bucketName });

    // Delete account with vaultclient
    await promisify(vaultClient.deleteAccount.bind(vaultClient))(account.accountName);
    log.info('Deleted account', { account: account.accountName });
}


module.exports = {
    createTestAccount,
    deleteTestAccount,
    iamHost,
    iamPort,
    s3Host,
    s3Port,
    adminAccessKeyId,
    adminSecretAccessKey,
    region,
};
