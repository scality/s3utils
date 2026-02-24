#!/usr/bin/env node
/* eslint-disable no-console */
/**
 * fix-missing-replication-permissions.js
 *
 * Reads the output of check-replication-permissions.js and creates IAM policies
 * with s3:ReplicateObject for roles that are missing it, then attaches them.
 *
 * Usage: node fix-missing-replication-permissions.js <input-file> <vault-host> <admin-config> [output-file] [--iam-port <port>] [--https] [--dry-run]
 *
 * Requires: vaultclient, @aws-sdk/client-iam (both in s3utils dependencies)
 */

const fs = require('fs');
const http = require('http');
const https = require('https');
const { parseArgs } = require('util');
const { Client: VaultClient } = require('vaultclient');
const {
    IAMClient,
    CreatePolicyCommand,
    AttachRolePolicyCommand,
    DeleteAccessKeyCommand,
} = require('@aws-sdk/client-iam');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');

// ===========================================================================
// Constants
// ===========================================================================
const POLICY_PREFIX = 's3-replication-audit-fix';
const STATEMENT_ID = 'AllowReplicateObjectAuditFix';
const KEY_DURATION_SECONDS = '900'; // 15-minute auto-expiry safety net

// ===========================================================================
// Logging (stderr for progress, stdout reserved for JSON result)
// ===========================================================================
function log(message) {
    console.error(message);
}

// ===========================================================================
// Argument parsing (Node.js 22+ built-in util.parseArgs)
// ===========================================================================
function getConfig() {
    const { values, positionals } = parseArgs({
        allowPositionals: true,
        options: {
            'iam-port': { type: 'string', default: '8600' },
            'https': { type: 'boolean', default: false },
            'dry-run': { type: 'boolean', default: false },
        },
    });

    if (positionals.length < 3) {
        log('Usage: node fix-missing-replication-permissions.js'
            + ' <input-file> <vault-host> <admin-config>'
            + ' [output-file] [--iam-port <port>] [--https] [--dry-run]');
        process.exit(1);
    }

    const [inputFile, vaultHost, adminConfig, outputFile] = positionals;
    return {
        inputFile,
        vaultHost,
        iamPort: parseInt(values['iam-port'], 10),
        adminConfig,
        useHttps: values.https,
        outputFile: outputFile || 'replication-fix-results.json',
        dryRun: values['dry-run'],
    };
}

// ===========================================================================
// Helpers
// ===========================================================================

/** Parse role ARN to extract account ID and role name */
function parseRoleArn(arn) {
    const match = arn.match(/^arn:aws:iam::(\d+):role\/(.+)$/);
    if (!match) {
        throw new Error(`Invalid role ARN: ${arn}`);
    }
    return { accountId: match[1], roleName: match[2] };
}

/**
 * Group missing entries by role, collecting all affected buckets per role.
 *
 * Input (results from check-replication-permissions.js):
 *   [
 *     { bucket: "bucket-old-1", ownerDisplayName: "testaccount", sourceRole: "arn:aws:iam::123:role/crr-role" },
 *     { bucket: "bucket-old-2", ownerDisplayName: "testaccount", sourceRole: "arn:aws:iam::123:role/crr-role" },
 *   ]
 *
 * Output:
 *   [
 *     { accountId: "123", accountName: "testaccount", roleName: "crr-role",
 *       roleArn: "arn:aws:iam::123:role/crr-role", buckets: ["bucket-old-1", "bucket-old-2"] }
 *   ]
 */
function groupByRole(results) {
    const roles = Object.groupBy(results, entry => entry.sourceRole);

    return Object.entries(roles).map(([roleArn, entries]) => {
        const { accountId, roleName } = parseRoleArn(roleArn);
        return {
            accountId,
            accountName: entries[0].ownerDisplayName,
            roleName,
            roleArn,
            buckets: entries.map(e => e.bucket),
        };
    });
}

/** Build the IAM policy document */
function buildPolicyDocument(buckets) {
    return {
        Version: '2012-10-17',
        Statement: [{
            Sid: STATEMENT_ID,
            Effect: 'Allow',
            Action: 's3:ReplicateObject',
            Resource: buckets.map(b => `arn:aws:s3:::${b}/*`),
        }],
    };
}

/** Promisify vaultclient.generateAccountAccessKey (callback is 2nd arg) */
function generateAccountAccessKeyAsync(client, accountName, options) {
    return new Promise((resolve, reject) => {
        client.generateAccountAccessKey(accountName, (err, res) => {
            if (err) {
                return reject(err);
            }
            return resolve(res);
        }, options);
    });
}

/** Create an IAM client for a given account */
function createIAMClient(config, accessKeyId, secretKey) {
    const protocol = config.useHttps ? 'https' : 'http';
    return new IAMClient({
        region: 'us-east-1',
        endpoint: `${protocol}://${config.vaultHost}:${config.iamPort}`,
        credentials: { accessKeyId, secretAccessKey: secretKey },
        requestHandler: new NodeHttpHandler({
            httpAgent: new http.Agent({ keepAlive: true }),
            httpsAgent: new https.Agent({
                keepAlive: true,
                rejectUnauthorized: false,
            }),
        }),
    });
}


// ===========================================================================
// Main
// ===========================================================================
async function main() {
    const startTime = Date.now();
    const config = getConfig();

    log('=== Fix Missing Replication Permissions ===');
    log(`Input:  ${config.inputFile}`);
    log(`Output: ${config.outputFile}`);
    log(`Vault/IAM: ${config.vaultHost}:${config.iamPort}`);
    if (config.dryRun) {
        log('Mode:   DRY-RUN (no changes will be made)');
    }
    log('');

    // Read input
    const input = JSON.parse(fs.readFileSync(config.inputFile, 'utf8'));
    const entries = input.results || [];
    if (entries.length === 0) {
        log('No buckets with missing permissions. Nothing to fix.');
        process.exit(0);
    }

    // Validate input format: ownerDisplayName is required (added in 1.17.5)
    const missingOwner = entries.filter(e => !e.ownerDisplayName);
    if (missingOwner.length > 0) {
        log('ERROR: Input file is missing "ownerDisplayName" field in result entries.');
        log('This field was added in s3utils 1.17.5. If your input was generated');
        log('with version 1.17.4 or earlier, re-run check-replication-permissions.js');
        log('to produce an updated output.');
        process.exit(1);
    }

    // Group by role, sorted by account so roles in the same account are
    // processed consecutively — reduces the chance of cached credentials
    // expiring while other accounts are being processed.
    const roles = groupByRole(entries)
        .sort((a, b) => (a.accountId < b.accountId ? -1 : 1));

    log(`Processing ${roles.length} role(s)`);
    log('');

    // Read admin credentials
    const adminCreds = JSON.parse(fs.readFileSync(config.adminConfig, 'utf8'));

    // Create vault admin client
    const vaultAdmin = new VaultClient(
        config.vaultHost,
        config.iamPort,
        config.useHttps,   // useHttps
        undefined,         // key
        undefined,         // cert
        undefined,         // ca
        config.useHttps,   // ignoreCa (skip cert verification when using HTTPS)
        adminCreds.accessKey,
        adminCreds.secretKeyValue,
    );

    // Results tracking
    const outcome = {
        metadata: {
            timestamp: new Date().toISOString(),
            durationMs: 0,
            inputFile: config.inputFile,
            dryRun: config.dryRun,
            counts: {
                totalRolesProcessed: roles.length,
                totalBucketsFixed: 0,
                policiesCreated: 0,
                policiesAttached: 0,
                keysCreated: 0,
                keysDeleted: 0,
                errors: 0,
            },
        },
        fixes: [],
        errors: [],
    };

    // Cache IAM client per account to reuse connections across roles
    // and for cleanup (key deletion).
    // Map<accountId, { accountName, accessKeyId, iamClient }>
    const accountCache = new Map();

    for (let i = 0; i < roles.length; i++) {
        const { accountId, accountName, roleName, roleArn, buckets } = roles[i];
        const policyName = `${POLICY_PREFIX}-${roleName}`;
        const policyDocument = buildPolicyDocument(buckets);

        log(`[${i + 1}/${roles.length}] Role "${roleName}" — account "${accountName}" (${buckets.length} bucket(s))`);

        const fix = {
            accountId,
            accountName,
            roleName,
            roleArn,
            policyName,
            buckets,
            status: 'pending',
        };

        if (config.dryRun) {
            fix.status = 'dry-run';
            log(`  [DRY-RUN] Would create policy "${policyName}"`);
            log(`  [DRY-RUN] Would attach to role "${roleName}"`);
            log(`  Buckets: ${buckets.join(', ')}`);
            outcome.fixes.push(fix);
            continue;
        }

        try {
            // Get or create temporary credentials and IAM client for this account
            if (!accountCache.has(accountId)) {
                const keyResult = await generateAccountAccessKeyAsync(
                    vaultAdmin, accountName,
                    { durationSeconds: KEY_DURATION_SECONDS },
                );
                accountCache.set(accountId, {
                    accountName,
                    accessKeyId: keyResult.id,
                    iamClient: createIAMClient(config, keyResult.id, keyResult.value),
                });
                outcome.metadata.counts.keysCreated++;
            }

            const { iamClient } = accountCache.get(accountId);

            // Idempotent/safe to re-run: CreatePolicy reuses an existing policy
            // with the same name, and AttachRolePolicy is a no-op if
            // the policy is already attached to the role.
            let policyArn;
            try {
                const resp = await iamClient.send(new CreatePolicyCommand({
                    PolicyName: policyName,
                    PolicyDocument: JSON.stringify(policyDocument),
                }));
                policyArn = resp.Policy.Arn;
                outcome.metadata.counts.policiesCreated++;
                log(`  Created policy "${policyName}"`);
            } catch (err) {
                if (err.name === 'EntityAlreadyExistsException'
                    || err.Code === 'EntityAlreadyExists') {
                    policyArn = `arn:aws:iam::${accountId}:policy/${policyName}`;
                    log(`  Policy "${policyName}" already exists, reusing`);
                } else {
                    throw err;
                }
            }

            fix.policyArn = policyArn;

            await iamClient.send(new AttachRolePolicyCommand({
                RoleName: roleName,
                PolicyArn: policyArn,
            }));
            outcome.metadata.counts.policiesAttached++;
            log(`  Attached policy to role "${roleName}"`);

            fix.status = 'success';
            outcome.metadata.counts.totalBucketsFixed += buckets.length;
        } catch (err) {
            fix.status = 'error';
            fix.error = err.message;
            outcome.metadata.counts.errors++;
            outcome.errors.push({
                accountId,
                accountName,
                roleName,
                policyName,
                message: err.message,
            });
            log(`  ERROR: ${err.message}`);
        }

        outcome.fixes.push(fix);
    }

    // Cleanup: delete all temporary keys via IAM DeleteAccessKey
    for (const [accountId, { accountName, accessKeyId, iamClient }] of accountCache) {
        try {
            await iamClient.send(new DeleteAccessKeyCommand({
                AccessKeyId: accessKeyId,
            }));
            outcome.metadata.counts.keysDeleted++;
            log(`Deleted temp key for account "${accountName}" (${accountId})`);
        } catch (err) {
            log(`WARNING: Failed to delete temp key for account "${accountName}" (auto-expires in ${KEY_DURATION_SECONDS}s): ${err.message}`);
        }
    }

    // Finalize timing
    const durationMs = Date.now() - startTime;
    outcome.metadata.durationMs = durationMs;

    // Write output file
    fs.writeFileSync(config.outputFile, JSON.stringify(outcome, null, 2));

    // Print summary
    log('\n=== Summary ===');
    log(`Roles processed:       ${outcome.metadata.counts.totalRolesProcessed}`);
    log(`Buckets fixed:         ${outcome.metadata.counts.totalBucketsFixed}`);
    log(`Policies created:      ${outcome.metadata.counts.policiesCreated}`);
    log(`Policies attached:     ${outcome.metadata.counts.policiesAttached}`);
    log(`Keys created:          ${outcome.metadata.counts.keysCreated}`);
    log(`Keys deleted:          ${outcome.metadata.counts.keysDeleted}`);
    log(`Errors:                ${outcome.metadata.counts.errors}`);
    log(`Duration:              ${(durationMs / 1000).toFixed(1)}s`);
    log(`Output saved to: ${config.outputFile}`);
    log('');
    log('Done.');

    // JSON result to stdout
    console.log(JSON.stringify(outcome, null, 2));
}

main().catch(e => {
    console.error('Fatal error:', e.message);
    process.exit(1);
});
