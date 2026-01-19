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
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m copy \
       -a 'src=/root/check-replication-permissions.js dest=/root/'
   ```

5. Find the vault-metadata repd leader IP:

   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a 'curl -s http://localhost:5300/_/raft/leader'
   ```

   This returns JSON like `{"ip":"10.160.116.162","port":4300}` - use the `ip` value.

   **Note:** Vault metadata uses port 5300 for admin.

6. Find the vault container ID:

   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a 'crictl ps | awk "/scality-vault/ {print \$1}"'
   ```

7. Copy files to `/var/tmp` (mounted in vault container) and run the script:

   ```bash
   VAULT_CONTAINER=<vault-container-id>
   LEADER_IP=<leader-ip-from-step-5>

   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a "cp /root/check-replication-permissions.js /var/tmp/ && \
           cp /root/buckets-with-replication.json /var/tmp/ && \
           crictl exec $VAULT_CONTAINER node /var/tmp/check-replication-permissions.js \
           /var/tmp/buckets-with-replication.json $LEADER_IP /var/tmp/missing.json"
   ```

8. Retrieve the output:

   ```bash
   ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
       -a 'cat /var/tmp/missing.json'
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

### Complete Workflow Example

Here's a complete example running both scripts end-to-end:

```bash
# From your local machine: copy scripts to the supervisor
scp replicationAudit/list-buckets-with-replication.sh root@<supervisor-ip>:/root/
scp replicationAudit/check-replication-permissions.js root@<supervisor-ip>:/root/

# Connect to the supervisor
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
    -a 'src=/root/check-replication-permissions.js dest=/root/'

# Step 2: Run list-buckets-with-replication.sh
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'bash /root/list-buckets-with-replication.sh'

# Step 3: Find the vault-metadata repd leader IP (port 5300)
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'curl -s http://localhost:5300/_/raft/leader'
# Note the "ip" value from the output, e.g., {"ip":"10.160.116.162","port":4300}

# Step 4: Find the vault container ID
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'crictl ps | awk "/scality-vault/ {print \$1}"'
# Note the container ID from the output

# Step 5: Set variables and run the permission check script
VAULT_CONTAINER=<vault-container-id-from-step-4>
LEADER_IP=<leader-ip-from-step-3>

ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a "cp /root/check-replication-permissions.js /var/tmp/ && \
        cp /root/buckets-with-replication.json /var/tmp/ && \
        crictl exec $VAULT_CONTAINER node /var/tmp/check-replication-permissions.js \
        /var/tmp/buckets-with-replication.json $LEADER_IP /var/tmp/missing.json"

# Step 6: Retrieve results
ansible -i env/$ENV_DIR/inventory runners_s3[0] -m shell \
    -a 'cat /var/tmp/missing.json'
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
