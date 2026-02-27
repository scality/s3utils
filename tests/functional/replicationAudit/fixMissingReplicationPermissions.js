const { execFile } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const os = require('os');
const path = require('path');
const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');
const {
    CreateBucketCommand,
    PutBucketVersioningCommand,
    PutBucketReplicationCommand,
} = require('@aws-sdk/client-s3');
const {
    CreatePolicyCommand,
    CreateRoleCommand,
    AttachRolePolicyCommand,
    ListAttachedRolePoliciesCommand,
    GetPolicyCommand,
    GetPolicyVersionCommand,
} = require('@aws-sdk/client-iam');

const {
    iamHost,
    iamPort,
    adminAccessKeyId,
    adminSecretAccessKey,
    createTestAccount,
    deleteTestAccount,
} = require('../../utils/S3Setup');

const execFileAsync = promisify(execFile);

const log = new Logger('fixMissingReplicationPermissions:test');

const SCRIPT_PATH = path.resolve(__dirname, '../../../replicationAudit/fix-missing-replication-permissions.js');
const ROLE_NAME = 'crr-role-test';
const POLICY_PREFIX = 's3-replication-audit-fix';

/**
 * Set up CRR on the source account with a role that deliberately
 * omits s3:ReplicateObject, so the fix script has something to fix.
 */
async function configureCrrWithMissingPermission(accountSource, accountDest) {
    // Enable versioning on both buckets
    await accountSource.s3Client.send(new PutBucketVersioningCommand({
        Bucket: accountSource.bucketName,
        VersioningConfiguration: { Status: 'Enabled' },
    }));
    await accountDest.s3Client.send(new PutBucketVersioningCommand({
        Bucket: accountDest.bucketName,
        VersioningConfiguration: { Status: 'Enabled' },
    }));

    // Create a CRR policy that deliberately OMITS s3:ReplicateObject
    const incompletePolicyDoc = {
        Version: '2012-10-17',
        Statement: [
            {
                Effect: 'Allow',
                Action: [
                    's3:GetObjectVersion',
                    's3:GetObjectVersionAcl',
                ],
                Resource: [`arn:aws:s3:::${accountSource.bucketName}/*`],
            },
            {
                Effect: 'Allow',
                Action: [
                    's3:ListBucket',
                    's3:GetReplicationConfiguration',
                ],
                Resource: [`arn:aws:s3:::${accountSource.bucketName}`],
            },
        ],
    };

    await accountSource.iamClient.send(new CreatePolicyCommand({
        PolicyName: 'crr-policy-incomplete',
        PolicyDocument: JSON.stringify(incompletePolicyDoc),
    }));

    // Create trust policy for backbeat
    const trustDoc = {
        Version: '2012-10-17',
        Statement: [{
            Effect: 'Allow',
            Principal: { Service: 'backbeat' },
            Action: 'sts:AssumeRole',
        }],
    };

    await accountSource.iamClient.send(new CreateRoleCommand({
        RoleName: ROLE_NAME,
        AssumeRolePolicyDocument: JSON.stringify(trustDoc),
    }));

    await accountSource.iamClient.send(new AttachRolePolicyCommand({
        RoleName: ROLE_NAME,
        PolicyArn: `arn:aws:iam::${accountSource.accountId}:policy/crr-policy-incomplete`,
    }));

    // Also set up destination account role (needed for replication config)
    const destPolicyDoc = {
        Version: '2012-10-17',
        Statement: [{
            Effect: 'Allow',
            Action: ['s3:ReplicateObject', 's3:ReplicateDelete'],
            Resource: `arn:aws:s3:::${accountDest.bucketName}/*`,
        }],
    };
    await accountDest.iamClient.send(new CreatePolicyCommand({
        PolicyName: 'crr-policy-dest',
        PolicyDocument: JSON.stringify(destPolicyDoc),
    }));
    await accountDest.iamClient.send(new CreateRoleCommand({
        RoleName: ROLE_NAME,
        AssumeRolePolicyDocument: JSON.stringify(trustDoc),
    }));
    await accountDest.iamClient.send(new AttachRolePolicyCommand({
        RoleName: ROLE_NAME,
        PolicyArn: `arn:aws:iam::${accountDest.accountId}:policy/crr-policy-dest`,
    }));

    // Configure bucket replication on source
    await accountSource.s3Client.send(new PutBucketReplicationCommand({
        Bucket: accountSource.bucketName,
        ReplicationConfiguration: {
            Role: `arn:aws:iam::${accountSource.accountId}:role/${ROLE_NAME},`
                + `arn:aws:iam::${accountDest.accountId}:role/${ROLE_NAME}`,
            Rules: [{
                Prefix: '',
                Status: 'Enabled',
                Destination: {
                    Bucket: `arn:aws:s3:::${accountDest.bucketName}`,
                },
            }],
        },
    }));
}

/**
 * Build the missing.json input file matching check script output format.
 */
function buildInputFile(accountSource, accountDest) {
    return {
        results: [{
            bucket: accountSource.bucketName,
            ownerDisplayName: accountSource.accountName,
            sourceRole: `arn:aws:iam::${accountSource.accountId}:role/${ROLE_NAME}`,
            destinationRole: `arn:aws:iam::${accountDest.accountId}:role/${ROLE_NAME}`,
            missingActions: ['s3:ReplicateObject'],
        }],
    };
}

/**
 * Run the fix script as a child process and return { stdout, stderr, exitCode }.
 */
async function runFixScript(args) {
    try {
        const { stdout, stderr } = await execFileAsync('node', [SCRIPT_PATH, ...args], {
            timeout: 30000,
        });
        return { stdout, stderr, exitCode: 0 };
    } catch (err) {
        return {
            stdout: err.stdout || '',
            stderr: err.stderr || '',
            exitCode: err.code || 1,
        };
    }
}

describe('fix-missing-replication-permissions', () => {
    let vaultClient;
    let accountSource;
    let accountDest;
    let tmpDir;
    let adminConfigPath;
    let inputFilePath;
    let outputFilePath;

    beforeEach(async () => {
        vaultClient = new vaultclient.Client(
            iamHost, iamPort,
            false, undefined, undefined, undefined, undefined,
            adminAccessKeyId, adminSecretAccessKey,
        );

        log.info('Creating test accounts');
        accountSource = await createTestAccount(vaultClient);
        accountDest = await createTestAccount(vaultClient);
        log.info('Test accounts created', {
            source: accountSource.accountName,
            dest: accountDest.accountName,
        });

        await configureCrrWithMissingPermission(accountSource, accountDest);

        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fix-repl-test-'));

        adminConfigPath = path.join(tmpDir, 'admin-config.json');
        fs.writeFileSync(adminConfigPath, JSON.stringify({
            accessKey: adminAccessKeyId,
            secretKeyValue: adminSecretAccessKey,
        }));

        inputFilePath = path.join(tmpDir, 'missing.json');
        fs.writeFileSync(inputFilePath, JSON.stringify(buildInputFile(accountSource, accountDest)));

        outputFilePath = path.join(tmpDir, 'output.json');
    }, 60000);

    afterEach(async () => {
        await deleteTestAccount(vaultClient, accountSource);
        await deleteTestAccount(vaultClient, accountDest);
        fs.rmSync(tmpDir, { recursive: true, force: true });
    }, 60000);

    it('--dry-run does not modify IAM state', async () => {
        const { stdout, exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
            '--dry-run',
        ]);

        expect(exitCode).toBe(0);

        const result = JSON.parse(stdout);
        expect(result.fixes[0].status).toBe('dry-run');

        // Verify no policy was attached
        const attached = await accountSource.iamClient.send(
            new ListAttachedRolePoliciesCommand({ RoleName: ROLE_NAME }),
        );
        const auditFixPolicy = (attached.AttachedPolicies || [])
            .find(p => p.PolicyName === `${POLICY_PREFIX}-${accountSource.bucketName}`);
        expect(auditFixPolicy).toBeUndefined();
    }, 30000);

    it('creates policy and attaches it to the role', async () => {
        const { stdout, exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(0);

        const result = JSON.parse(stdout);
        expect(result.fixes[0].status).toBe('success');
        expect(result.metadata.counts.policiesCreated).toBe(1);
        expect(result.metadata.counts.policiesAttached).toBe(1);

        const bucketPolicyName = `${POLICY_PREFIX}-${accountSource.bucketName}`;

        // Verify policy is attached to role
        const attached = await accountSource.iamClient.send(
            new ListAttachedRolePoliciesCommand({ RoleName: ROLE_NAME }),
        );
        const auditFixPolicy = (attached.AttachedPolicies || [])
            .find(p => p.PolicyName === bucketPolicyName);
        expect(auditFixPolicy).toBeDefined();

        // Verify policy document contains s3:ReplicateObject for this bucket
        const policyArn = `arn:aws:iam::${accountSource.accountId}:policy/${bucketPolicyName}`;
        const policyResp = await accountSource.iamClient.send(
            new GetPolicyCommand({ PolicyArn: policyArn }),
        );
        const versionId = policyResp.Policy.DefaultVersionId;

        const versionResp = await accountSource.iamClient.send(
            new GetPolicyVersionCommand({ PolicyArn: policyArn, VersionId: versionId }),
        );
        const policyDoc = JSON.parse(decodeURIComponent(versionResp.PolicyVersion.Document));
        const actions = policyDoc.Statement.flatMap(s => [].concat(s.Action));
        expect(actions).toContain('s3:ReplicateObject');

        const resources = policyDoc.Statement.flatMap(s => [].concat(s.Resource));
        expect(resources).toContain(`arn:aws:s3:::${accountSource.bucketName}/*`);
    }, 30000);

    it('idempotent: re-run does not fail or duplicate', async () => {
        // First run: creates the policy
        const first = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);
        expect(first.exitCode).toBe(0);

        const firstResult = JSON.parse(first.stdout);
        expect(firstResult.metadata.counts.policiesCreated).toBe(1);

        // Second run: policy already exists, should reuse it
        const second = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);
        expect(second.exitCode).toBe(0);

        const secondResult = JSON.parse(second.stdout);
        expect(secondResult.fixes[0].status).toBe('success');
        expect(secondResult.metadata.counts.policiesCreated).toBe(0);
    }, 30000);

    it('temp key cleanup: keysDeleted matches keysCreated', async () => {
        const { stdout, exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(0);

        const result = JSON.parse(stdout);
        expect(result.metadata.counts.keysDeleted).toBe(result.metadata.counts.keysCreated);
    }, 30000);

    it('rejects input without ownerDisplayName', async () => {
        const badInputPath = path.join(tmpDir, 'bad-missing.json');
        fs.writeFileSync(badInputPath, JSON.stringify({
            results: [{
                bucket: accountSource.bucketName,
                // ownerDisplayName deliberately omitted
                sourceRole: `arn:aws:iam::${accountSource.accountId}:role/${ROLE_NAME}`,
                missingActions: ['s3:ReplicateObject'],
            }],
        }));

        const { exitCode } = await runFixScript([
            badInputPath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(1);
    }, 30000);
});

describe('fix-missing-replication-permissions (multi-bucket and multi-role)', () => {
    let vaultClient;
    let accounts;
    let tmpDir;
    let adminConfigPath;
    let outputFilePath;

    beforeEach(() => {
        vaultClient = new vaultclient.Client(
            iamHost, iamPort,
            false, undefined, undefined, undefined, undefined,
            adminAccessKeyId, adminSecretAccessKey,
        );

        accounts = [];
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fix-repl-test-'));
        adminConfigPath = path.join(tmpDir, 'admin-config.json');
        fs.writeFileSync(adminConfigPath, JSON.stringify({
            accessKey: adminAccessKeyId,
            secretKeyValue: adminSecretAccessKey,
        }));
        outputFilePath = path.join(tmpDir, 'output.json');
    });

    afterEach(async () => {
        for (const account of accounts) {
            await deleteTestAccount(vaultClient, account);
        }
        fs.rmSync(tmpDir, { recursive: true, force: true });
    }, 60000);

    it('one role, multiple buckets: one policy per bucket', async () => {
        const accountSource = await createTestAccount(vaultClient);
        const accountDest = await createTestAccount(vaultClient);
        accounts.push(accountSource, accountDest);

        // Create a second bucket in the source account
        const secondBucket = `${accountSource.bucketName}-second`;
        await accountSource.s3Client.send(new CreateBucketCommand({ Bucket: secondBucket }));

        // Enable versioning on all buckets
        await accountSource.s3Client.send(new PutBucketVersioningCommand({
            Bucket: accountSource.bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));
        await accountSource.s3Client.send(new PutBucketVersioningCommand({
            Bucket: secondBucket,
            VersioningConfiguration: { Status: 'Enabled' },
        }));
        await accountDest.s3Client.send(new PutBucketVersioningCommand({
            Bucket: accountDest.bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));

        // Create role in source account
        const trustDoc = {
            Version: '2012-10-17',
            Statement: [{
                Effect: 'Allow',
                Principal: { Service: 'backbeat' },
                Action: 'sts:AssumeRole',
            }],
        };
        await accountSource.iamClient.send(new CreateRoleCommand({
            RoleName: ROLE_NAME,
            AssumeRolePolicyDocument: JSON.stringify(trustDoc),
        }));

        // Write input: same role referenced by two buckets
        const inputFilePath = path.join(tmpDir, 'missing.json');
        const sourceRole = `arn:aws:iam::${accountSource.accountId}:role/${ROLE_NAME}`;
        fs.writeFileSync(inputFilePath, JSON.stringify({
            results: [
                {
                    bucket: accountSource.bucketName,
                    ownerDisplayName: accountSource.accountName,
                    sourceRole,
                    missingActions: ['s3:ReplicateObject'],
                },
                {
                    bucket: secondBucket,
                    ownerDisplayName: accountSource.accountName,
                    sourceRole,
                    missingActions: ['s3:ReplicateObject'],
                },
            ],
        }));

        const { stdout, exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(0);

        const result = JSON.parse(stdout);
        // One fix entry per bucket
        expect(result.fixes).toHaveLength(2);
        expect(result.fixes.map(f => f.bucket)).toEqual(
            expect.arrayContaining([accountSource.bucketName, secondBucket]),
        );
        // One policy per bucket
        expect(result.metadata.counts.policiesCreated).toBe(2);
        expect(result.metadata.counts.totalBucketsFixed).toBe(2);

        // Verify each bucket has its own policy with the correct ARN
        for (const bucketName of [accountSource.bucketName, secondBucket]) {
            const policyName = `${POLICY_PREFIX}-${bucketName}`;
            const policyArn = `arn:aws:iam::${accountSource.accountId}:policy/${policyName}`;
            const policyResp = await accountSource.iamClient.send(
                new GetPolicyCommand({ PolicyArn: policyArn }),
            );
            const versionResp = await accountSource.iamClient.send(
                new GetPolicyVersionCommand({
                    PolicyArn: policyArn,
                    VersionId: policyResp.Policy.DefaultVersionId,
                }),
            );
            const policyDoc = JSON.parse(decodeURIComponent(versionResp.PolicyVersion.Document));
            const resources = policyDoc.Statement.flatMap(s => [].concat(s.Resource));
            expect(resources).toEqual([`arn:aws:s3:::${bucketName}/*`]);
        }
    }, 60000);

    it('two roles in the same account: two policies, one temp key', async () => {
        const accountSource = await createTestAccount(vaultClient);
        const accountDest = await createTestAccount(vaultClient);
        accounts.push(accountSource, accountDest);

        const secondRoleName = 'crr-role-test-2';

        // Create a second bucket
        const secondBucket = `${accountSource.bucketName}-second`;
        await accountSource.s3Client.send(new CreateBucketCommand({ Bucket: secondBucket }));

        // Enable versioning
        await accountSource.s3Client.send(new PutBucketVersioningCommand({
            Bucket: accountSource.bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));
        await accountSource.s3Client.send(new PutBucketVersioningCommand({
            Bucket: secondBucket,
            VersioningConfiguration: { Status: 'Enabled' },
        }));

        // Create two roles in the same source account
        const trustDoc = {
            Version: '2012-10-17',
            Statement: [{
                Effect: 'Allow',
                Principal: { Service: 'backbeat' },
                Action: 'sts:AssumeRole',
            }],
        };
        await accountSource.iamClient.send(new CreateRoleCommand({
            RoleName: ROLE_NAME,
            AssumeRolePolicyDocument: JSON.stringify(trustDoc),
        }));
        await accountSource.iamClient.send(new CreateRoleCommand({
            RoleName: secondRoleName,
            AssumeRolePolicyDocument: JSON.stringify(trustDoc),
        }));

        // Write input: two roles in the same account
        const inputFilePath = path.join(tmpDir, 'missing.json');
        fs.writeFileSync(inputFilePath, JSON.stringify({
            results: [
                {
                    bucket: accountSource.bucketName,
                    ownerDisplayName: accountSource.accountName,
                    sourceRole: `arn:aws:iam::${accountSource.accountId}:role/${ROLE_NAME}`,
                    missingActions: ['s3:ReplicateObject'],
                },
                {
                    bucket: secondBucket,
                    ownerDisplayName: accountSource.accountName,
                    sourceRole: `arn:aws:iam::${accountSource.accountId}:role/${secondRoleName}`,
                    missingActions: ['s3:ReplicateObject'],
                },
            ],
        }));

        const { stdout, exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(0);

        const result = JSON.parse(stdout);
        expect(result.fixes).toHaveLength(2);
        expect(result.metadata.counts.policiesCreated).toBe(2);
        expect(result.metadata.counts.policiesAttached).toBe(2);
        // Only one temp key for the single account
        expect(result.metadata.counts.keysCreated).toBe(1);
        expect(result.metadata.counts.keysDeleted).toBe(1);

        // Verify both policies exist (named by bucket, not role)
        const attached1 = await accountSource.iamClient.send(
            new ListAttachedRolePoliciesCommand({ RoleName: ROLE_NAME }),
        );
        expect((attached1.AttachedPolicies || [])
            .find(p => p.PolicyName === `${POLICY_PREFIX}-${accountSource.bucketName}`)).toBeDefined();

        const attached2 = await accountSource.iamClient.send(
            new ListAttachedRolePoliciesCommand({ RoleName: secondRoleName }),
        );
        expect((attached2.AttachedPolicies || [])
            .find(p => p.PolicyName === `${POLICY_PREFIX}-${secondBucket}`)).toBeDefined();
    }, 60000);

    it('two roles across different accounts: separate policies and keys', async () => {
        const accountSource1 = await createTestAccount(vaultClient);
        const accountSource2 = await createTestAccount(vaultClient);
        const accountDest = await createTestAccount(vaultClient);
        accounts.push(accountSource1, accountSource2, accountDest);

        // Enable versioning
        await accountSource1.s3Client.send(new PutBucketVersioningCommand({
            Bucket: accountSource1.bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));
        await accountSource2.s3Client.send(new PutBucketVersioningCommand({
            Bucket: accountSource2.bucketName,
            VersioningConfiguration: { Status: 'Enabled' },
        }));

        // Create roles in each source account
        const trustDoc = {
            Version: '2012-10-17',
            Statement: [{
                Effect: 'Allow',
                Principal: { Service: 'backbeat' },
                Action: 'sts:AssumeRole',
            }],
        };
        await accountSource1.iamClient.send(new CreateRoleCommand({
            RoleName: ROLE_NAME,
            AssumeRolePolicyDocument: JSON.stringify(trustDoc),
        }));
        await accountSource2.iamClient.send(new CreateRoleCommand({
            RoleName: ROLE_NAME,
            AssumeRolePolicyDocument: JSON.stringify(trustDoc),
        }));

        // Write input: same role name but in two different accounts
        const inputFilePath = path.join(tmpDir, 'missing.json');
        fs.writeFileSync(inputFilePath, JSON.stringify({
            results: [
                {
                    bucket: accountSource1.bucketName,
                    ownerDisplayName: accountSource1.accountName,
                    sourceRole: `arn:aws:iam::${accountSource1.accountId}:role/${ROLE_NAME}`,
                    missingActions: ['s3:ReplicateObject'],
                },
                {
                    bucket: accountSource2.bucketName,
                    ownerDisplayName: accountSource2.accountName,
                    sourceRole: `arn:aws:iam::${accountSource2.accountId}:role/${ROLE_NAME}`,
                    missingActions: ['s3:ReplicateObject'],
                },
            ],
        }));

        const { stdout, exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(0);

        const result = JSON.parse(stdout);
        expect(result.fixes).toHaveLength(2);
        expect(result.fixes.every(f => f.status === 'success')).toBe(true);
        expect(result.metadata.counts.policiesCreated).toBe(2);
        // Two different accounts → two temp keys
        expect(result.metadata.counts.keysCreated).toBe(2);
        expect(result.metadata.counts.keysDeleted).toBe(2);

        // Verify each account has its own policy attached (named by bucket)
        const attached1 = await accountSource1.iamClient.send(
            new ListAttachedRolePoliciesCommand({ RoleName: ROLE_NAME }),
        );
        expect((attached1.AttachedPolicies || [])
            .find(p => p.PolicyName === `${POLICY_PREFIX}-${accountSource1.bucketName}`)).toBeDefined();

        const attached2 = await accountSource2.iamClient.send(
            new ListAttachedRolePoliciesCommand({ RoleName: ROLE_NAME }),
        );
        expect((attached2.AttachedPolicies || [])
            .find(p => p.PolicyName === `${POLICY_PREFIX}-${accountSource2.bucketName}`)).toBeDefined();
    }, 60000);

    it('empty results array exits with code 0', async () => {
        const inputFilePath = path.join(tmpDir, 'empty.json');
        fs.writeFileSync(inputFilePath, JSON.stringify({ results: [] }));

        const { exitCode } = await runFixScript([
            inputFilePath, iamHost, adminConfigPath, outputFilePath,
            '--iam-port', String(iamPort),
        ]);

        expect(exitCode).toBe(0);
    }, 30000);
});
