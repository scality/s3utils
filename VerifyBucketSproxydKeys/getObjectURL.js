function getObjectURL(bucket, objectKey) {
    if (!bucket) {
        return 's3://';
    }
    if (!objectKey) {
        return `s3://${bucket}`;
    }
    return `s3://${bucket}/${encodeURI(objectKey)}`;
}

module.exports = getObjectURL;
