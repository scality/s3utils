# How to Use migrateBuckets.js

The migrateBuckets script is for transferring buckets and their
contents from one IAM account to another.

To launch the script, use the command:

    $ node migrateBuckets.js -b <bucketName> -s <srcCanoncialId> \
      -o <srcOwnerName> -d <destCanonicalId> -e <destOwnerName> -c <mdConfigPath>

The `-b` option specifies the name of the bucket to migrate.

The `-s` option specifies the canonical ID of the current bucket owner account.

The `-d` option specifies the canonical ID of the target bucket owner account.

The `-o` option specifies the display name of the target bucket owner account.

The `-c` option specifies the absolute path to a json file.

All options are required.
The json file must include a `bucketd` key. `bucketd` is an object that contains
a bootstrap list for your metadata server.

For example:

    {
       “bucketd”: {
          “bootstrap”: [“localhost:9000”]
       }
    }

Include an optional `https` key if your S3C instance is configured with https.
This object must contain `key`, `cert`, and `ca` keys with paths to the
corresponding files as values.

For example:

    {
       "https": {
          "key": <path/to/key>,
          "cert": <path/to/cert>,
          "ca": <path/to/ca>
       }
    }

**Note:**
   Your S3C `config.json` may be used as your json file for this script.

Three environment variables, ACCESS\_KEY, SECRET\_KEY, and ENDPOINT,
must be set before the script can be run. ENDPOINT is your S3C
instance\'s URL (\"<http://localhost>\", for example), and ACCESS\_KEY
and SECRET\_KEY contain S3C credentials that have read access to all
buckets.

When run, the script migrates the specified bucket and its contents to
the destination IAM account. If the script is interrupted, run it again.
Buckets already migrated are skipped.

**Note:**
   If an incorrect destination canonical ID and/or owner name is supplied when
   the script is run, access to that bucket may be temporarily lost. To restore
   access, run the script again with the incorrect canonical ID as the source
   parameters, and the correct values as the destination parameters.
