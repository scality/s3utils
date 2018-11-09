# s3utils
S3 Connector and Zenko Utilities

Run the docker container as
```
docker run -d -e 'ACCESS_KEY=accessKey' -e 'SECRET_KEY=secretkey' -e 'ENDPOINT=http://127.0.0.1:8000' zenko/s3utils
```

## Trigger CRR on objects that were put before replication was enabled on the bucket

1. Enable versioning and setup replication on the bucket
2. Run script as
```
node crrExistingObjects.js testbucket1,testbucket2
```

# Empty a versioned bucket

This script deletes all versions of objects in the bucket including delete markers,
and aborts any ongoing multipart uploads to prepare the bucket for deletion.

**Note: This will delete data associated with the objects and it's not recoverable**
```
node cleanupBuckets.js testbucket1,testbucket2
```
