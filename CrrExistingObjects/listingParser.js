function listingParser(entries) {
    if (!entries) {
        return entries;
    }
    return entries.map(entry => {
        const tmp = JSON.parse(entry.value);
        return {
            Key: entry.key,
            Size: tmp['content-length'],
            ETag: tmp['content-md5'],
            VersionId: tmp.versionId,
            IsNull: tmp.isNull,
            IsDeleteMarker: tmp.isDeleteMarker,
            LastModified: tmp['last-modified'],
            Owner: {
                DisplayName: tmp['owner-display-name'],
                ID: tmp['owner-id'],
            },
            StorageClass: tmp['x-amz-storage-class'],
        };
    });
}

module.exports = listingParser;
