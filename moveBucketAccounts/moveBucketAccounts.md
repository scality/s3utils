# How to Use moveBucketAccounts.js

The moveBucketAccounts script is for transferring buckets and their
contents from one IAM account to another.

To launch the script, use the command:

    $ node moveBucketAccounts.js <path/to/json>

The required json file must include `bucketd` and `buckets` keys.
`bucketd` is an object that contains a bootstrap list for your metadata
server.

For example:

    {
       “bucketd”: {
          “bootstrap”: [ “localhost:9000” ]
       }
    }

An optional `log` key can also be included under `bucketd`.

The `buckets` key contains an array of objects, one for each bucket to
be transferred. Each object must contain five keys: `bucketName` (the
name of the bucket), `srcCanonicalId` (the current bucket owner\'s
canonical ID), `srcOwnerName` (the current bucket owner's display name),
`destCanonicalId` (the desired bucket owner\'s canonical ID), and
`destOwnerName` (the desired bucket owner's display name).

For example:

    {
       “buckets”: [
          {
             “bucketName”: “<yourBucketName>”,
             “srcCanonicalId”: “2055e7ae097a7…”,
             “srcOwnerName”: “test_1552891315”,
             “destCanonicalId”: “2ecf2f89ee49…”,
             “destOwnerName”: “test_1548117091”
          },
          {
             “bucketName”: “<yourOtherBucketName>”,
             “srcCanonicalId”: “2ecf2f89ee49…”,
             “srcOwnerName”: “test_1552891315”,
             “destCanonicalId”: “2055e7ae097a7…”,
             “destOwnerName”: “test_1548117091”
          }
       ]
    }

**Note:**
  The example abbreviates the canonical IDs. Include the entire ID in your
  json.

Three environment variables, ACCESS\_KEY, SECRET\_KEY, and ENDPOINT,
must be set before the script can be run. ENDPOINT is your S3C
instance\'s URL (\"<http://localhost>\", for example), and ACCESS\_KEY
and SECRET\_KEY contain S3C credentials that have read access to all
buckets.

When run, the script transfers each listed bucket and its contents to
the destination IAM account. If the script is interrupted, run it again.
Buckets already transferred are skipped.
