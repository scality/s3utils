# TL;DR Complete Workflow Example

Here's a complete example running the two scripts and audit the IAM policies used by CRR:

From your local machine: copy scripts to the supervisor

```bash
scp replicationAudit/list-buckets-with-replication.sh root@<supervisor-ip>:/root/
scp replicationAudit/check-replication-permissions.js root@<supervisor-ip>:/root/
```

Connect to the supervisor
```bash
ssh root@<supervisor-ip>
```

Then, from the supervisor, go to the federation directory
(by default `/srv/scality/s3/s3-offline/federation`):

```bash
cd /srv/scality/s3/s3-offline/federation
ENV_DIR=s3config

# Step 1: Copy scripts to S3 connector node
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m copy \
    -a 'src=/root/list-buckets-with-replication.sh dest=/root/'
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m copy \
    -a 'src=/root/check-replication-permissions.js dest={{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs'

# Step 2: Run list-buckets-with-replication.sh
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'BUCKETD_HOST=$(ss -nlptH "sport = :9000" | awk "{print \$4}"| cut -d: -f1) bash /root/list-buckets-with-replication.sh'

# Step 3: Find the vault-metadata repd leader IP (port 5300)
ansible -i env/$ENV_DIR/inventory md1-cluster1 -m shell \
    -a 'curl -s http://localhost:5300/_/raft/leader'
# Note the "ip" value from the output, e.g., {"ip":"10.160.116.162","port":4300}

# Step 4: Set the LEADER_IP variable and run the permission check script
# Note: replace ctrctl with docker on RHEL/CentOS 7
LEADER_IP=<leader-ip-from-step-3>

ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a "mv /root/buckets-with-replication.json {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs && \
        ctrctl exec scality-vault{{ container_name_suffix | default("")}} node /logs/check-replication-permissions.js \
        /logs/buckets-with-replication.json $LEADER_IP /logs/missing.json"

# Step 5: Retrieve results
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'cat {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs/missing.json' \
    | grep -v CHANGED | tee /root/replicationAudit_missing.json

# Step 6: Clean up remote files
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'rm -f {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs/missing.json \
       {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs/check-replication-permissions.js \
       {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs/buckets-with-replication.json \
       /root/list-buckets-with-replication.sh'

# Step 7 (optional): Fix missing permissions
# Run from your local machine (requires vaultclient and @aws-sdk/client-iam)
node replicationAudit/fix-missing-replication-permissions.js \
    /root/replicationAudit_missing.json <supervisor-ip> admin1.json

# Step 8: Re-run check to verify fixes (repeat steps 3-5)
```

# Scripts Documentation

## list-buckets-with-replication.sh

Lists all buckets with replication enabled across all accounts using the metadata API (bucketd).

### Prerequisites

- Access to an S3 connector node (`runners_s3`) with bucketd running
- `curl` and `jq` installed
- Bucketd accessible on port 9000 (default)

### Usage

1. Copy the script to the supervisor:
   ```bash
   scp replicationAudit/list-buckets-with-replication.sh root@<supervisor-ip>:/root/
   ```

2. Connect to the supervisor as root and go to the federation directory
   (by default `/srv/scality/s3/s3-offline/federation`):
   ```bash
   ssh root@<supervisor-ip>
   cd /srv/scality/s3/s3-offline/federation
   ENV_DIR=s3config
   ```

3. Copy the script to an S3 connector node:
   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m copy \
       -a 'src=/root/list-buckets-with-replication.sh dest=/root/'
   ```

4. Run the script:
   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a 'bash /root/list-buckets-with-replication.sh'
   ```

5. Retrieve the output file:
   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a 'cat /root/buckets-with-replication.json'
   ```

### Configuration

Environment variables can be used to customize behavior:

| Variable | Default | Description |
|----------|---------|-------------|
| `BUCKETD_HOST` | localhost | Bucketd hostname |
| `BUCKETD_PORT` | 9000 | Bucketd port |
| `BATCH_SIZE` | 10 | Number of parallel curl requests (increase for faster execution) |
| `OUTPUT_FILE` | buckets-with-replication.json | Output file path |

### Output Format

The script produces a JSON file with metadata and results:

```json
{
  "metadata": {
    "timestamp": "2026-01-19T10:07:25Z",
    "durationSeconds": 2,
    "bucketdUrl": "http://localhost:9000",
    "counts": {
      "totalBucketsScanned": 150,
      "bucketsWithReplication": 3,
      "bucketsWithoutReplication": 147,
      "fetchErrors": 0
    }
  },
  "results": [
    {
      "bucket": "source-bucket",
      "owner": "267390090509",
      "ownerDisplayName": "testaccount",
      "sourceRole": "arn:aws:iam::267390090509:role/crr-source-role"
    }
  ]
}
```

### How It Works

1. **Discovers buckets**: Queries `/default/bucket/users..bucket` which contains all bucket names across all accounts
2. **Fetches attributes**: For each bucket, fetches attributes via `/default/attributes/{bucket}` in batches of 10 (with connection reuse)
3. **Filters replication**: Identifies buckets with `replicationConfiguration` containing at least one enabled rule
4. **Extracts metadata**: Outputs bucket name, owner info, and the source role (part before the comma in the role ARN)

### Example Run

```
=== List Buckets with Replication Enabled ===
Bucketd: http://localhost:9000
Batch size: 10
Output file: buckets-with-replication.json

Checking bucketd connectivity...
Connected to bucketd

Step 1: Fetching bucket list...
Found 150 buckets (excluding internal buckets)

Step 2: Fetching bucket attributes in batches of 10...
  Processed 10/150 buckets (batch 1)
  Processed 20/150 buckets (batch 2)
  ...

=== Summary ===
Total buckets scanned:      150
  With replication:         3
  Without replication:      147
  Fetch errors:             0
Duration:                   2s
Output saved to: buckets-with-replication.json

Done.
```

### Troubleshooting

**"Cannot connect to bucketd"**
- Ensure you're running on an S3 connector node (`runners_s3`) where bucketd is running
- Check if bucketd is listening: `ss -tlnp | grep 9000`

**"Found 0 buckets"**
- Verify users..bucket is accessible: `curl -s http://localhost:9000/default/bucket/users..bucket | jq '.Contents[].key'`

**Script timeout**
- For large deployments, consider running directly on the S3 connector node via interactive SSH

---

## check-replication-permissions.js

Checks if replication roles have `s3:ReplicateObject` permission by directly
querying Vault metadata via the repd protocol (no vaultclient or credentials
needed).

### Prerequisites

- Output from `list-buckets-with-replication.sh` (buckets-with-replication.json)
- Access to an S3 connector node (`runners_s3`) with a container that has Node.js (e.g., vault container)

### How It Works

The script is self-contained with inlined functions from MetaData and Vault:

- **Protocol constants** - from `MetaData/lib/protocol.json`
- **Protocol encoding** - from `MetaData/lib/server/ProtoBuilder.js`
- **Key generation** - from `vault/lib/Indexer.js`

This ensures portability while using the exact same protocol and key formats as Vault.

### Usage

1. First, run `list-buckets-with-replication.sh` to generate the bucket list
   (see above)

2. Copy the script to the supervisor:

   ```bash
   scp replicationAudit/check-replication-permissions.js root@<supervisor-ip>:/root/
   ```

3. Connect to the supervisor as root (if not already connected) and go to the
   federation directory (by default `/srv/scality/s3/s3-offline/federation`):

   ```bash
   ssh root@<supervisor-ip>
   cd /srv/scality/s3/s3-offline/federation
   ENV_DIR=s3config
   ```

4. Copy the script to an S3 connector node:

   ```bash
   ansible -i env/$ENV_DIR/inventory 'runners_s3[0]' -m copy \
       -a 'src=/root/check-replication-permissions.js dest={{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs'
   ```

5. Find the vault-metadata repd leader IP:

   ```bash
   ansible -i env/$ENV_DIR/inventory md1-cluster1 -m shell \
       -a 'curl -s http://localhost:5300/_/raft/leader'
   ```

   This returns JSON like `{"ip":"10.160.116.162","port":4300}` - use the `ip` value.

   **Note:** Vault metadata uses port 5300 for admin.

6. Copy files to `/var/tmp` (mounted in vault container) and run the script:

   ```bash
   LEADER_IP=<leader-ip-from-step-5>

   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a "cp /root/buckets-with-replication.json {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs && \
           ctrctl exec scality-vault{{ container_name_suffix | default("")}} node /logs/check-replication-permissions.js \
           /logs/buckets-with-replication.json $LEADER_IP /logs/missing.json"
   ```

8. Retrieve the output:

   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a 'cat {{ env_host_logs}}/scality-vault{{ container_name_suffix | default("")}}/logs/missing.json'
   ```

### Command Line Arguments

```
node check-replication-permissions.js [input-file] [leader-ip] [output-file] [--include-policies]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `input-file` | /root/buckets-with-replication.json | Input JSON from list script |
| `leader-ip` | 127.0.0.1 | Vault-metadata repd leader IP |
| `output-file` | /root/missing-replication-permissions.json | Output file path |
| `--include-policies` | (not set) | Include full policy documents in output |

### Output Format

> **Breaking change (since 1.17.5):** The output now includes `ownerDisplayName`
> in each result entry. This field is required by
> `fix-missing-replication-permissions.js` to identify accounts without an
> extra API call. If you ran `check-replication-permissions.js` on version
> 1.17.4 or earlier, **re-run it** to produce an output that
> `fix-missing-replication-permissions.js` can consume.

The script produces a JSON file with metadata and results. The `results` array
contains **only buckets missing the `s3:ReplicateObject` permission**.

**Default output (compact):**

```json
{
  "metadata": {
    "timestamp": "2026-01-19T15:27:11.557Z",
    "durationMs": 24,
    "durationHuman": "0.0s",
    "repdLeader": "10.160.112.179:4300",
    "inputFile": "/tmp/buckets.json",
    "counts": {
      "totalBuckets": 6,
      "bucketsOk": 3,
      "bucketsMissingPermission": 3,
      "bucketsSkipped": 0,
      "bucketsWithErrors": 0,
      "uniqueRolesChecked": 2
    }
  },
  "results": [
    {
      "bucket": "bucket-old-1",
      "ownerDisplayName": "testaccount",
      "sourceRole": "arn:aws:iam::267390090509:role/crr-role-outdated",
      "policies": [
        {
          "name": "crr-policy-outdated",
          "path": "/",
          "allowsReplicateObject": false
        }
      ]
    }
  ]
}
```

**With `--include-policies` (full policy documents):**

```json
{
  "metadata": {
    "timestamp": "2026-01-19T15:27:11.557Z",
    "durationMs": 24,
    "durationHuman": "0.0s",
    "repdLeader": "10.160.112.179:4300",
    "inputFile": "/tmp/buckets.json",
    "counts": {
      "totalBuckets": 6,
      "bucketsOk": 3,
      "bucketsMissingPermission": 3,
      "bucketsSkipped": 0,
      "bucketsWithErrors": 0,
      "uniqueRolesChecked": 2
    }
  },
  "results": [
    {
      "bucket": "bucket-old-1",
      "sourceRole": "arn:aws:iam::267390090509:role/crr-role-outdated",
      "policies": [
        {
          "name": "crr-policy-outdated",
          "path": "/",
          "allowsReplicateObject": false,
          "document": {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Action": ["s3:GetObjectVersion", "s3:GetObjectVersionAcl"],
                "Resource": ["arn:aws:s3:::bucket-old-1/*"]
              },
              {
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:GetReplicationConfiguration"],
                "Resource": ["arn:aws:s3:::bucket-old-1"]
              }
            ]
          }
        }
      ]
    }
  ]
}
```

In this example, the policy is missing `s3:ReplicateObject` - it only has
`s3:GetObjectVersion` and `s3:GetObjectVersionAcl`.

**Fields:**

- `bucket`: Source bucket name with replication enabled
- `sourceRole`: The IAM role ARN configured for replication
- `error`: (optional) Error message if role lookup failed
- `policies`: Array of all policies attached to the role, each containing:
  - `name`: Policy name
  - `path`: Policy path (e.g., `/` or `/custom/path/`)
  - `allowsReplicateObject`: Whether this policy grants `s3:ReplicateObject`
    (always false in output since we only output missing permissions)
  - `document`: (only with `--include-policies`) Full IAM policy document for analysis

### Script Logic

1. **Connects to repd**: TCP connection to vault-metadata repd on port 4300
2. **For each bucket's replication role**:
   - Get role ID: `linkRoleArn(arn)` → role ID
   - List attached policies: `policyByRoleId(accountId, roleId, '', '')`
   - Get each policy: `policyByName(accountId, policyName)`
3. **Check permissions**: Evaluates if any policy allows `s3:ReplicateObject`
4. **Output**: Only buckets missing the required permission

### Vault Metadata Key Schema

The script queries the `vaultdb` database using these key patterns:

| Purpose | Key Pattern | Example |
|---------|-------------|---------|
| Role by ARN | `roleArn:{arn}` | `roleArn:arn:aws:iam::123:role/MyRole` |
| Role data | `linkAccount:{id}:roleId:{roleId}` | `linkAccount:123:roleId:AROAXXX` |
| Policies | `linkAccount:{id}:roleId:{roleId}:policy:{path}` | `...policy:/my-policy` |
| Policy content | `accountId:{id}:policyName:{name}` | `accountId:123:policyName:my-policy` |

### Example Run

```
=== Check Replication Role Permissions ===
Input:  /tmp/buckets.json
Output: /tmp/missing.json
Repd:   10.160.112.172:4300

Processing 6 buckets...

[1/6] bucket-new-1 -> OK
[2/6] bucket-new-2 -> OK
[3/6] bucket-new-3 -> OK
[4/6] bucket-old-1 -> MISSING: s3:ReplicateObject
[5/6] bucket-old-2 -> MISSING: s3:ReplicateObject
[6/6] bucket-old-3 -> MISSING: s3:ReplicateObject

=== Summary ===
Total checked: 6
Missing permission: 3
Output saved to: /tmp/missing.json
```

### Troubleshooting

**"Role not found in vault"**

- The role ARN in the bucket's replication configuration doesn't exist in vault
- The role may have been deleted after replication was configured

**"No policies attached to role"**

- The role exists but has no managed policies attached
- Attach a policy with `s3:ReplicateObject` permission to the role

**"Missing s3:ReplicateObject"**

- The role has policies but none grant `s3:ReplicateObject` on the source bucket
- Review the policy documents in the output to see what permissions are granted
- Ensure the policy has:
  - Action: `s3:ReplicateObject` (or `s3:*` or `s3:Replicate*`)
  - Resource: matching the source bucket ARN (e.g., `arn:aws:s3:::bucket-name/*`)

**Connection timeout or refused**

- Ensure you're connecting to the correct repd leader IP
- The script must run inside a container that can reach repd on port 4300
- Find the leader: `curl -s http://localhost:5300/_/raft/leader`

**Script timeout**

- For many buckets, run directly on the S3 connector node via interactive SSH

---

## fix-missing-replication-permissions.js

Reads the output of `check-replication-permissions.js` and creates IAM policies
with `s3:ReplicateObject` for roles that are missing it.

The script applies **minimal changes**: one policy per bucket, with an explicit
Statement ID (`AllowReplicateObjectAuditFix`) so the policies are easily
identifiable later. Using one policy per bucket makes re-runs truly idempotent:
if the policy already exists, its document is guaranteed to be identical.

### Prerequisites

- Output from `check-replication-permissions.js` (missing.json)
- Vault admin credentials (`admin1.json` with `accessKey` and `secretKeyValue`).
  Found on the supervisor at:
  ```
  /srv/scality/s3/s3-offline/federation/env/<ENV_DIR>/vault/admin-clientprofile/admin1.json
  ```
- Network access to Vault admin/IAM API (port 8600) from the machine running the script
- `vaultclient` and `@aws-sdk/client-iam` installed (both in s3utils dependencies)

### Usage

```bash
node fix-missing-replication-permissions.js <input-file> <vault-host> <admin-config> [output-file] [options]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `input-file` | (required) | Path to missing.json from check script |
| `vault-host` | (required) | Vault admin host (e.g., 13.50.166.21) |
| `admin-config` | (required) | Path to admin credentials JSON |
| `output-file` | replication-fix-results.json | Output file path |
| `--iam-port <port>` | 8600 | Vault admin and IAM API port |
| `--https` | (not set) | Use HTTPS to connect to Vault |
| `--dry-run` | (not set) | Show what would be done without making changes |

### How It Works

1. **Reads** the missing permissions file and enriches each entry with parsed role fields
2. **Maps** account IDs to names using `ownerDisplayName` from the input (no API call)
3. For each bucket entry (grouped by account for credential reuse):
   - **Generates** a temporary access key via vault admin API (15-minute auto-expiry)
   - **Creates** one IAM policy per bucket with `s3:ReplicateObject`
   - **Attaches** the policy to the role
   - **Deletes** the temporary access key (falls back to auto-expiry on failure)
4. **Writes** results to the output file

### Policy Created

For each bucket, the script creates a policy named
`s3-replication-audit-fix-<bucketName>`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowReplicateObjectAuditFix",
    "Effect": "Allow",
    "Action": "s3:ReplicateObject",
    "Resource": "arn:aws:s3:::bucket-old-1/*"
  }]
}
```

### Output Format

```json
{
  "metadata": {
    "timestamp": "2026-02-23T20:35:00.000Z",
    "durationMs": 65,
    "inputFile": "missing.json",
    "dryRun": false,
    "counts": {
      "totalBucketsProcessed": 3,
      "totalBucketsFixed": 3,
      "policiesCreated": 3,
      "policiesAttached": 3,
      "keysCreated": 1,
      "keysDeleted": 1,
      "errors": 0
    }
  },
  "fixes": [
    {
      "accountId": "267390090509",
      "accountName": "testaccount",
      "roleName": "crr-role-outdated",
      "roleArn": "arn:aws:iam::267390090509:role/crr-role-outdated",
      "policyName": "s3-replication-audit-fix-bucket-old-1",
      "policyArn": "arn:aws:iam::267390090509:policy/s3-replication-audit-fix-bucket-old-1",
      "bucket": "bucket-old-1",
      "status": "success"
    },
    {
      "accountId": "267390090509",
      "accountName": "testaccount",
      "roleName": "crr-role-outdated",
      "roleArn": "arn:aws:iam::267390090509:role/crr-role-outdated",
      "policyName": "s3-replication-audit-fix-bucket-old-2",
      "policyArn": "arn:aws:iam::267390090509:policy/s3-replication-audit-fix-bucket-old-2",
      "bucket": "bucket-old-2",
      "status": "success"
    }
  ],
  "errors": []
}
```

### Example Run

```
=== Fix Missing Replication Permissions ===
Input:  missing.json
Output: replication-fix-results.json
Vault/IAM: 13.50.166.21:8600

Processing 3 bucket(s)

[1/3] Bucket "bucket-old-1" — role "crr-role-outdated" — account "testaccount"
  Created policy "s3-replication-audit-fix-bucket-old-1"
  Attached policy to role "crr-role-outdated"
[2/3] Bucket "bucket-old-2" — role "crr-role-outdated" — account "testaccount"
  Created policy "s3-replication-audit-fix-bucket-old-2"
  Attached policy to role "crr-role-outdated"
[3/3] Bucket "bucket-old-3" — role "crr-role-outdated" — account "testaccount"
  Created policy "s3-replication-audit-fix-bucket-old-3"
  Attached policy to role "crr-role-outdated"
Deleted temp key for account "testaccount" (267390090509)

=== Summary ===
Buckets processed:     3
Buckets fixed:         3
Policies created:      3
Policies attached:     3
Keys created:          1
Keys deleted:          1
Errors:                0
Duration:              0.7s
Output saved to: replication-fix-results.json

Done.
```

### Idempotency

The script is safe to run multiple times:

- Each bucket has its own policy, so if it already exists the document is
  guaranteed to be identical — `EntityAlreadyExists` is a true no-op
- Attaching an already-attached policy is a no-op in IAM
- Temporary access keys auto-expire after 15 minutes even if deletion fails

### Troubleshooting

**"No ownerDisplayName found for account"**

- The input file is missing `ownerDisplayName`. Re-run `check-replication-permissions.js`
  to generate a fresh output that includes this field.

**"Failed to generate temp key"**

- Verify admin credentials in the config file
- Ensure vault admin API is reachable on the specified host and port

**IAM operation errors**

- Check that the IAM port is correct (default 8600, may differ per deployment)
- Verify the role still exists in vault

**"Failed to delete temp key"**

- Non-critical: the key auto-expires after 15 minutes
- The error is logged but does not prevent other operations
