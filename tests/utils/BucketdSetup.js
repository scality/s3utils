const crypto = require('crypto');

/**
 * Helpers to create buckets and object metadata directly through the
 * bucketd HTTP API, which allows to create metadata that cannot be
 * created through the S3 API, like broken metadata
 */

const bucketdHostport = process.env.BUCKETD_HOSTPORT || 'localhost:9000';
const bucketdURL = `http://${bucketdHostport}`;

function bucketURL(bucket) {
    return `${bucketdURL}/default/bucket/${bucket}`;
}

function objectURL(bucket, key) {
    return `${bucketURL(bucket)}/${encodeURIComponent(key)}`;
}

async function checkedFetch(method, url, body) {
    const res = await fetch(url, { method, body });
    if (res.status !== 200) {
        throw new Error(`${method} ${url} returned status ${res.status}`);
    }
    return res;
}

/**
 * Generate a unique bucket name
 *
 * @param {string} prefix - bucket name prefix
 * @return {string} - bucket name
 */
function uniqueBucketName(prefix) {
    return `${prefix}-${crypto.randomBytes(4).toString('hex')}`;
}

/**
 * Create a bucket in bucketd
 *
 * @param {string} bucket - bucket name
 * @return {Promise<undefined>} - resolves when created
 */
async function createBucket(bucket) {
    await checkedFetch('POST', bucketURL(bucket), '{}');
}

/**
 * Write the raw metadata of an object in bucketd
 *
 * @param {string} bucket - bucket name
 * @param {string} key - object key, may contain a version ID suffix
 * separated by a '\0' character for versioned keys
 * @param {object|string} md - metadata, as an object or as a raw
 * JSON string
 * @return {Promise<undefined>} - resolves when written
 */
async function putObjectMD(bucket, key, md) {
    const value = typeof md === 'string' ? md : JSON.stringify(md);
    await checkedFetch('POST', objectURL(bucket, key), value);
}

/**
 * Create a bucket and write a set of object metadata into it
 *
 * @param {string} bucket - bucket name
 * @param {object} objects - object metadata keyed by object key,
 * see putObjectMD()
 * @return {Promise<undefined>} - resolves when created
 */
async function createBucketWithObjects(bucket, objects) {
    await createBucket(bucket);
    for (const [key, md] of Object.entries(objects)) {
        await putObjectMD(bucket, key, md);
    }
}

/**
 * List the raw listing entries of a bucket
 *
 * @param {string} bucket - bucket name
 * @return {Promise<object[]>} - listing entries, as { key, value }
 * where value is a JSON string
 */
async function listBucket(bucket) {
    const entries = [];
    let marker = '';
    while (true) {
        const res = await checkedFetch(
            'GET', `${bucketURL(bucket)}?maxKeys=1000&marker=${encodeURIComponent(marker)}`);
        const { Contents, IsTruncated } = await res.json();
        entries.push(...Contents);
        if (!IsTruncated) {
            return entries;
        }
        marker = Contents[Contents.length - 1].key;
    }
}

/**
 * Delete all object metadata of a bucket, then the bucket itself
 *
 * Does nothing if the bucket does not exist.
 *
 * @param {string} bucket - bucket name
 * @return {Promise<undefined>} - resolves when deleted
 */
async function deleteBucket(bucket) {
    const head = await fetch(bucketURL(bucket), { method: 'HEAD' });
    if (head.status === 404) {
        return;
    }
    for (const { key } of await listBucket(bucket)) {
        await checkedFetch('DELETE', objectURL(bucket, key));
    }
    await checkedFetch('DELETE', bucketURL(bucket));
}

/**
 * Get the ID of the raft session hosting a bucket
 *
 * @param {string} bucket - bucket name
 * @return {Promise<string>} - raft session ID
 */
async function getBucketRaftSession(bucket) {
    const res = await checkedFetch('GET', `${bucketdURL}/_/buckets/${bucket}/id`);
    return (await res.text()).trim();
}

module.exports = {
    bucketdHostport,
    uniqueBucketName,
    createBucket,
    putObjectMD,
    createBucketWithObjects,
    deleteBucket,
    getBucketRaftSession,
};
