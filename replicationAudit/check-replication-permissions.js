#!/usr/bin/env node
/* eslint-disable no-console */
/**
 * check-replication-permissions.js
 *
 * Checks if replication roles have s3:ReplicateObject permission
 * by querying Vault metadata directly via repd protocol.
 *
 * Usage: node check-replication-permissions.js [input-file] [leader-ip] [output-file]
 *
 * How it connects to vault metadata:
 *
 *   Vault metadata has no HTTP frontend (no bucketd). This script connects
 *   directly to repd (the raft-based metadata store) on TCP port 4300
 *   using a simple protocol: 4-byte length prefix + JSON payload.
 */

const net = require('net');
const fs = require('fs');

// ===========================================================================
// Configuration
// ===========================================================================
const CONFIG = {
    inputFile: process.argv[2] || '/root/buckets-with-replication.json',
    leaderIp: process.argv[3] || '127.0.0.1',
    outputFile: process.argv[4] || '/root/missing-replication-permissions.json',
    repdPort: 4300,
    dbName: 'vaultdb',
    includePolicies: process.argv.includes('--include-policies'),
    requestTimeoutMs: 10000,
};

// ===========================================================================
// Logging
// ===========================================================================
function log(message) {
    console.error(message);
}

function logProgress(current, total, bucket, status) {
    process.stderr.write(`[${current}/${total}] ${bucket} -> ${status}\n`);
}

// ===========================================================================
// Protocol constants and encoding
// [FROM MetaData/lib/protocol.json and MetaData/lib/server/ProtoBuilder.js]
//
// Messages are framed as: [4-byte length][JSON payload]
// ===========================================================================
const PROTOCOL = {
    FROM_REPD: 1,
    TYPE_REQUEST: 1,
    METHOD_GET: 2,
    METHOD_LIST: 4,
    HEADER_SIZE: 4,  // 32-bit big-endian integer for message length
};

function encodeMessage(payload) {
    const json = JSON.stringify(payload);
    const jsonLength = Buffer.byteLength(json);
    const buffer = Buffer.allocUnsafe(PROTOCOL.HEADER_SIZE + jsonLength);
    buffer.writeInt32BE(jsonLength);
    buffer.write(json, PROTOCOL.HEADER_SIZE);
    return buffer;
}

function tryReadMessage(buffer) {
    if (buffer.length < PROTOCOL.HEADER_SIZE) {
        return null;
    }

    const jsonLength = buffer.readUInt32BE(0);
    const totalLength = PROTOCOL.HEADER_SIZE + jsonLength;

    if (buffer.length < totalLength) {
        return null;
    }

    return {
        data: JSON.parse(buffer.slice(PROTOCOL.HEADER_SIZE, totalLength).toString()),
        remaining: buffer.slice(totalLength),
    };
}

// ===========================================================================
// Vault database key generation
// [FROM vault/lib/Indexer.js]
// ===========================================================================
const VaultKeys = {
    /** Get key for looking up role by ARN */
    roleByArn(roleArn) {
        return `roleArn:${roleArn}`;
    },

    /** Get key prefix for policies attached to a role */
    policiesByRole(accountId, roleId) {
        return `linkAccount:${accountId}:roleId:${roleId}:policy:`;
    },

    /** Get key for looking up policy by name */
    policyByName(accountId, policyName) {
        return `accountId:${accountId}:policyName:${policyName}`;
    },

    /** Get range limits for prefix-based listing */
    getRangeLimits(prefix) {
        const end = prefix.slice(0, -1) + String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);
        return [prefix, end];
    },

    /** Extract policy path and name from a policiesByRole key */
    extractPolicyFromKey(key) {
        const policyPart = key.split(':policy:')[1] || '';
        const lastSlash = policyPart.lastIndexOf('/');

        if (lastSlash === -1) {
            return { path: '/', name: policyPart };
        }

        return {
            path: policyPart.substring(0, lastSlash + 1),
            name: policyPart.substring(lastSlash + 1),
        };
    },
};

// ===========================================================================
// Repd Client
// ===========================================================================
class RepdClient {
    constructor(host, port, dbName) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.client = null;
        this.requestId = 0;
        this.receiveBuffer = Buffer.alloc(0);
        this.pendingCallback = null;
    }

    connect() {
        return new Promise((resolve, reject) => {
            this.client = net.connect(this.port, this.host);
            this.client.on('connect', resolve);
            this.client.on('error', reject);
            this.client.on('data', chunk => this.handleData(chunk));
        });
    }

    handleData(chunk) {
        this.receiveBuffer = Buffer.concat([this.receiveBuffer, chunk]);

        const message = tryReadMessage(this.receiveBuffer);
        if (message && this.pendingCallback) {
            this.receiveBuffer = message.remaining;
            const callback = this.pendingCallback;
            this.pendingCallback = null;
            callback(null, message.data);
        }
    }

    sendRequest(method, params) {
        return new Promise((resolve, reject) => {
            this.pendingCallback = (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response);
                }
            };

            const request = {
                from: PROTOCOL.FROM_REPD,
                type: PROTOCOL.TYPE_REQUEST,
                logUids: `check-${Date.now()}`,
                repd: {
                    method,
                    id: ++this.requestId,
                    db: this.dbName,
                    ...params,
                },
            };

            this.client.write(encodeMessage(request));

            setTimeout(() => {
                if (this.pendingCallback) {
                    this.pendingCallback = null;
                    reject(new Error('Request timeout'));
                }
            }, CONFIG.requestTimeoutMs);
        });
    }

    async get(key) {
        const response = await this.sendRequest(PROTOCOL.METHOD_GET, { key });
        return response.repd?.data;
    }

    async list(keyStart, keyEnd) {
        const response = await this.sendRequest(PROTOCOL.METHOD_LIST, {
            parameters: {
                gte: keyStart,
                lte: keyEnd,
                keys: true,
                values: true,
                maxKeys: 1000,
            },
        });
        return response.repd?.data || [];
    }

    close() {
        if (this.client) {
            this.client.end();
        }
    }
}

// ===========================================================================
// Permission checking helpers
// ===========================================================================

/** Extract account ID from a role ARN */
function getAccountIdFromArn(arn) {
    return arn.split(':')[4];
}

/** Check if a policy action matches s3:ReplicateObject */
function isReplicateAction(action) {
    return action === '*'
        || action === 's3:*'
        || action === 's3:Replicate*'
        || action === 's3:ReplicateObject';
}

/** Check if a policy resource matches the bucket */
function isMatchingResource(resource, bucketArn) {
    return resource === '*'
        || resource === 'arn:aws:s3:::*'
        || resource === 'arn:aws:s3:::*/*'
        || resource === bucketArn
        || resource === `${bucketArn}/*`;
}

/** Check if a policy document grants s3:ReplicateObject for a bucket */
function policyAllowsReplication(policyDoc, bucketName) {
    const bucketArn = `arn:aws:s3:::${bucketName}`;
    const statements = [].concat(policyDoc.Statement || []);

    for (const statement of statements) {
        if (statement.Effect !== 'Allow') {
            continue;
        }

        const actions = [].concat(statement.Action || []);
        const hasReplicateAction = actions.some(isReplicateAction);
        if (!hasReplicateAction) {
            continue;
        }

        const resources = [].concat(statement.Resource || []);
        const hasMatchingResource = resources.some(r => isMatchingResource(r, bucketArn));
        if (hasMatchingResource) {
            return true;
        }
    }

    return false;
}

/** Extract the current policy document from stored policy data */
function extractPolicyDocument(policyData) {
    const policy = JSON.parse(policyData);
    const defaultVersion = policy.defaultVersion || 1;
    const version = (policy.versions || []).find(v => v.id === defaultVersion);
    return version?.doc || {};
}

// ===========================================================================
// Core logic
// ===========================================================================

/**
 * Check if a bucket's replication role has s3:ReplicateObject permission
 */
async function checkBucketPermissions(repdClient, bucket, sourceRole) {
    const accountId = getAccountIdFromArn(sourceRole);
    const result = {
        bucket,
        sourceRole,
        policies: [],
    };

    // Get role ID from ARN
    const roleKey = VaultKeys.roleByArn(sourceRole);
    const roleId = await repdClient.get(roleKey);

    if (!roleId) {
        return {
            hasPermission: false,
            result: { ...result, error: 'Role not found in vault' },
        };
    }

    // List policies attached to the role
    const policyKeyPrefix = VaultKeys.policiesByRole(accountId, roleId);
    const [keyStart, keyEnd] = VaultKeys.getRangeLimits(policyKeyPrefix);
    const policyLinks = await repdClient.list(keyStart, keyEnd);

    if (policyLinks.length === 0) {
        return {
            hasPermission: false,
            result: { ...result, error: 'No policies attached to role' },
        };
    }

    // Check each attached policy
    let hasPermission = false;

    for (const link of policyLinks) {
        const { name: policyName, path: policyPath } = VaultKeys.extractPolicyFromKey(link.key);
        const policyKey = VaultKeys.policyByName(accountId, policyName);
        const policyData = await repdClient.get(policyKey);

        if (!policyData) {
            continue;
        }

        try {
            const policyDoc = extractPolicyDocument(policyData);
            const allowsReplication = policyAllowsReplication(policyDoc, bucket);

            const policyInfo = {
                name: policyName,
                path: policyPath,
                allowsReplicateObject: allowsReplication,
            };
            if (CONFIG.includePolicies) {
                policyInfo.document = policyDoc;
            }
            result.policies.push(policyInfo);

            if (allowsReplication) {
                hasPermission = true;
            }
        } catch (e) {
            result.policies.push({ name: policyName, error: 'Failed to parse policy' });
        }
    }

    return { hasPermission, result };
}

/** Build the output object with metadata and results */
function buildOutput(buckets, results, stats, rolesChecked, startTime) {
    const durationMs = Date.now() - startTime;

    return {
        metadata: {
            timestamp: new Date().toISOString(),
            durationMs,
            durationHuman: `${(durationMs / 1000).toFixed(1)}s`,
            repdLeader: `${CONFIG.leaderIp}:${CONFIG.repdPort}`,
            inputFile: CONFIG.inputFile,
            counts: {
                totalBuckets: buckets.length,
                bucketsOk: stats.ok,
                bucketsMissingPermission: stats.missing,
                bucketsSkipped: stats.skipped,
                bucketsWithErrors: stats.errors,
                uniqueRolesChecked: rolesChecked.size,
            },
        },
        results,
    };
}

/** Print summary to stderr */
function printSummary(bucketCount, stats, rolesChecked, durationHuman) {
    log('\n=== Summary ===');
    log(`Total checked:        ${bucketCount}`);
    log(`  OK:                 ${stats.ok}`);
    log(`  Missing permission: ${stats.missing}`);
    log(`  Skipped (no role):  ${stats.skipped}`);
    log(`  Errors:             ${stats.errors}`);
    log(`Unique roles checked: ${rolesChecked.size}`);
    log(`Duration:             ${durationHuman}`);
}

// ===========================================================================
// Main
// ===========================================================================
async function main() {
    const startTime = Date.now();

    log('=== Check Replication Role Permissions ===');
    log(`Input:  ${CONFIG.inputFile}`);
    log(`Output: ${CONFIG.outputFile}`);
    log(`Repd:   ${CONFIG.leaderIp}:${CONFIG.repdPort}`);
    log('');

    // Read input file
    const inputData = JSON.parse(fs.readFileSync(CONFIG.inputFile, 'utf8'));
    const buckets = inputData.results || inputData;  // Support both old and new format
    log(`Processing ${buckets.length} buckets...\n`);

    // Connect to repd
    const repdClient = new RepdClient(CONFIG.leaderIp, CONFIG.repdPort, CONFIG.dbName);
    await repdClient.connect();

    const stats = {
        ok: 0,
        missing: 0,
        skipped: 0,
        errors: 0,
    };
    const rolesChecked = new Set();
    const results = [];

    // Process each bucket
    for (let i = 0; i < buckets.length; i++) {
        const { bucket, sourceRole, ownerDisplayName } = buckets[i];

        if (!sourceRole) {
            logProgress(i + 1, buckets.length, bucket, 'SKIP (no role)');
            stats.skipped++;
            continue;
        }

        rolesChecked.add(sourceRole);

        try {
            const { hasPermission, result } = await checkBucketPermissions(repdClient, bucket, sourceRole);

            if (hasPermission) {
                logProgress(i + 1, buckets.length, bucket, 'OK');
                stats.ok++;
            } else {
                const reason = result.error || 's3:ReplicateObject';
                logProgress(i + 1, buckets.length, bucket, `MISSING: ${reason}`);
                result.ownerDisplayName = ownerDisplayName;
                results.push(result);
                stats.missing++;
            }
        } catch (e) {
            logProgress(i + 1, buckets.length, bucket, `ERROR: ${e.message}`);
            results.push({ bucket, sourceRole, ownerDisplayName, error: e.message, policies: [] });
            stats.errors++;
        }
    }

    repdClient.close();

    // Build and save output
    const output = buildOutput(buckets, results, stats, rolesChecked, startTime);
    fs.writeFileSync(CONFIG.outputFile, JSON.stringify(output, null, 2));

    // Print summary
    printSummary(buckets.length, stats, rolesChecked, output.metadata.durationHuman);

    if (results.length > 0) {
        log(`Output saved to: ${CONFIG.outputFile}`);
    }

    console.log(JSON.stringify(output, null, 2));
}

// Run main only when executed directly (not when required as a module)
if (require.main === module) {
    main().catch(e => {
        console.error('Fatal error:', e.message);
        process.exit(1);
    });
}

// Export for testing
module.exports = {
    policyAllowsReplication,
};
