function getObjectURL(bucket, objectKey) {
    if (!bucket) {
        return 's3://';
    }
    if (!objectKey) {
        return `s3://${bucket}`;
    }
    return `s3://${bucket}/${encodeURIComponent(objectKey)}`;
}

module.exports = getObjectURL;
