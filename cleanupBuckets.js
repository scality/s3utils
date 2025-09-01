const { http, https } = require('httpagent');
const { 
    S3Client, 
    ListObjectVersionsCommand, 
    DeleteObjectsCommand, 
    ListMultipartUploadsCommand,
    AbortMultipartUploadCommand, 
} = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const { Logger } = require('werelogs');

const log = new Logger('s3utils::emptyBucket');
// configurable params
const BUCKETS = process.argv[2] ? process.argv[2].split(',') : null;
const { ACCESS_KEY } = process.env;
const { SECRET_KEY } = process.env;
const { ENDPOINT } = process.env;
if (!BUCKETS || BUCKETS.length === 0) {
    log.fatal('No buckets given as input! Please provide '
        + 'a comma-separated list of buckets');
    process.exit(1);
}
if (!ENDPOINT) {
    log.fatal('ENDPOINT not defined!');
    process.exit(1);
}
if (!ACCESS_KEY) {
    log.fatal('ACCESS_KEY not defined');
    process.exit(1);
}
if (!SECRET_KEY) {
    log.fatal('SECRET_KEY not defined');
    process.exit(1);
}
const LISTING_LIMIT = 1000;

const s3 = new S3Client({
    credentials: {
        accessKeyId: ACCESS_KEY,
        secretAccessKey: SECRET_KEY,
    },
    endpoint: ENDPOINT,
    region: 'us-east-1',
    forcePathStyle: true,
    requestHandler: new NodeHttpHandler({
        httpAgent: new http.Agent({ keepAlive: true }),
        httpsAgent: new https.Agent({ 
            keepAlive: true,
            rejectUnauthorized: false
        }),
    }),
});

// return object with key and version_id
function _getKeys(keys) {
    return keys.map(v => ({
        Key: v.Key,
        VersionId: v.VersionId,
    }));
}

// delete all versions of an object
async function _deleteVersions(bucket, objectsToDelete) {
    const params = {
        Bucket: bucket,
        Delete: { Objects: objectsToDelete },
    };
    const command = new DeleteObjectsCommand(params);
    try {
        await s3.send(command);
        objectsToDelete.forEach(v => log.info(`deleted key: ${v.Key}`));
    } catch (err) {
        log.error('batch delete err', err);
        throw err;
    }
}

async function cleanupVersions(bucket) {
    let VersionIdMarker = null;
    let KeyMarker = null;
    let IsTruncated = true;
    
    while (IsTruncated) {
        const data = await s3.send(new ListObjectVersionsCommand({
            Bucket: bucket,
            MaxKeys: LISTING_LIMIT,
            VersionIdMarker,
            KeyMarker,
        }));
    
        VersionIdMarker = data.NextVersionIdMarker;
        KeyMarker = data.NextKeyMarker;
        IsTruncated = data.IsTruncated;
        const keysToDelete = _getKeys(data.Versions || []);
        const markersToDelete = _getKeys(data.DeleteMarkers || []);
        const allObjectsToDelete = keysToDelete.concat(markersToDelete);
        
        if (allObjectsToDelete.length > 0) {
            await _deleteVersions(bucket, allObjectsToDelete);
        } else {
            log.info(`No objects to delete for bucket ${bucket}`);
        }
    }
}

async function abortAllMultipartUploads(bucket) {
    const res = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
    log.info(`Found ${res.Uploads ? res.Uploads.length : 0} multipart uploads to abort`);

    if (!res || !res.Uploads || res.Uploads.length === 0) {
        return;
    }
    
    const CONCURRENCY = 10;
    for (let i = 0; i < res.Uploads.length; i += CONCURRENCY) {
        const batch = res.Uploads.slice(i, i + CONCURRENCY);
        const deleteMpuPromises = batch.map(async item => {
            const { Key, UploadId } = item;
            const params = { Bucket: bucket, Key, UploadId };
            return await s3.send(new AbortMultipartUploadCommand(params));
        });
        await Promise.all(deleteMpuPromises);
    }
}

async function _cleanupBucket(bucket) {
    try {
        await Promise.all([
            cleanupVersions(bucket),
            abortAllMultipartUploads(bucket),
        ]);
        log.info(`completed cleaning up of bucket: ${bucket}`);
    } catch (err) {
        log.error('error occured deleting objects', err);
        throw err;
    }
}

async function cleanupBuckets(buckets) {
    try {
        for (const bucket of buckets) {
            await _cleanupBucket(bucket);
        }
        log.info('completed cleaning all buckets');
    } catch (err) {
        log.error('error occured deleting objects', err);
    }
}

cleanupBuckets(BUCKETS);
