const { getConfig } = require('../../../MetadataScan/getConfig');

const NEW_SCRIPT_ARGV = ['/usr/bin/node', '/s3utils/metadataScan.js'];
const LEGACY_SCRIPT_ARGV = ['/usr/bin/node', '/s3utils/verifyBucketSproxydKeys.js'];

const MINIMAL_ENV = {
    BUCKETS: 'bucket1',
};

describe('MetadataScan/getConfig', () => {
    test('should return defaults with a minimal environment', () => {
        expect(getConfig({ env: MINIMAL_ENV, argv: NEW_SCRIPT_ARGV })).toEqual({
            bucketdHostport: '127.0.0.1:9000',
            sproxydHostport: '127.0.0.1:8181',
            buckets: ['bucket1'],
            raftSessions: undefined,
            keysFromStdin: false,
            fromUrl: undefined,
            workers: 100,
            verbose: false,
            logProgressInterval: 10,
            listingLimit: 1000,
            mpuOnly: false,
            checkMissingSproxydKeys: false,
            listingDigestsOutputDir: undefined,
            listingDigestsBlockSize: 1000,
        });
    });

    test('should parse all options from the environment', () => {
        const env = {
            BUCKETD_HOSTPORT: 'localhost:9001',
            SPROXYD_HOSTPORT: 'localhost:8182',
            RAFT_SESSIONS: '1,2,3',
            FROM_URL: 's3://bucket1/key',
            WORKERS: '10',
            VERBOSE: '1',
            LOG_PROGRESS_INTERVAL: '5',
            LISTING_LIMIT: '100',
            MPU_ONLY: '1',
            CHECK_MISSING_SPROXYD_KEYS: '1',
            LISTING_DIGESTS_OUTPUT_DIR: '/tmp/digests',
            LISTING_DIGESTS_BLOCK_SIZE: '50',
        };
        expect(getConfig({ env, argv: NEW_SCRIPT_ARGV })).toEqual({
            bucketdHostport: 'localhost:9001',
            sproxydHostport: 'localhost:8182',
            buckets: undefined,
            raftSessions: ['1', '2', '3'],
            keysFromStdin: false,
            fromUrl: 's3://bucket1/key',
            workers: 10,
            verbose: true,
            logProgressInterval: 5,
            listingLimit: 100,
            mpuOnly: true,
            checkMissingSproxydKeys: true,
            listingDigestsOutputDir: '/tmp/digests',
            listingDigestsBlockSize: 50,
        });
    });

    test('should parse command-line options following the script path', () => {
        const argv = [...NEW_SCRIPT_ARGV, '--raft-sessions', '1,2', '--bucketd-hostport=localhost:9001'];
        const config = getConfig({ env: {}, argv });
        expect(config.raftSessions).toEqual(['1', '2']);
        expect(config.bucketdHostport).toBe('localhost:9001');
    });

    describe('checkMissingSproxydKeys', () => {
        test('should be disabled by default when argv is not provided', () => {
            expect(getConfig({ env: MINIMAL_ENV }).checkMissingSproxydKeys).toBe(false);
        });

        test('should be enabled by default when invoked as verifyBucketSproxydKeys.js', () => {
            expect(getConfig({ env: MINIMAL_ENV, argv: LEGACY_SCRIPT_ARGV })
                .checkMissingSproxydKeys).toBe(true);
        });

        test('should be enabled by default when invoked as verifyBucketSproxydKeys without extension', () => {
            const argv = ['/usr/bin/node', './verifyBucketSproxydKeys'];
            expect(getConfig({ env: MINIMAL_ENV, argv }).checkMissingSproxydKeys).toBe(true);
        });

        test('should be disabled with CHECK_MISSING_SPROXYD_KEYS=0 when invoked as verifyBucketSproxydKeys.js', () => {
            const env = { ...MINIMAL_ENV, CHECK_MISSING_SPROXYD_KEYS: '0' };
            expect(getConfig({ env, argv: LEGACY_SCRIPT_ARGV })
                .checkMissingSproxydKeys).toBe(false);
        });

        test('should be disabled with --no-check-missing-sproxyd-keys when invoked as verifyBucketSproxydKeys.js', () => {
            const argv = [...LEGACY_SCRIPT_ARGV, '--no-check-missing-sproxyd-keys'];
            expect(getConfig({ env: MINIMAL_ENV, argv }).checkMissingSproxydKeys).toBe(false);
        });

        test('should be disabled with NO_MISSING_KEY_CHECK when invoked as verifyBucketSproxydKeys.js', () => {
            const env = { ...MINIMAL_ENV, NO_MISSING_KEY_CHECK: '1' };
            expect(getConfig({ env, argv: LEGACY_SCRIPT_ARGV })
                .checkMissingSproxydKeys).toBe(false);
        });

        test('should keep the script name default when NO_MISSING_KEY_CHECK is false', () => {
            const env = { ...MINIMAL_ENV, NO_MISSING_KEY_CHECK: '0' };
            expect(getConfig({ env, argv: NEW_SCRIPT_ARGV })
                .checkMissingSproxydKeys).toBe(false);
            expect(getConfig({ env, argv: LEGACY_SCRIPT_ARGV })
                .checkMissingSproxydKeys).toBe(true);
        });

        test('should accept CHECK_MISSING_SPROXYD_KEYS=0 together with NO_MISSING_KEY_CHECK=1', () => {
            const env = {
                ...MINIMAL_ENV,
                CHECK_MISSING_SPROXYD_KEYS: '0',
                NO_MISSING_KEY_CHECK: '1',
            };
            expect(getConfig({ env, argv: LEGACY_SCRIPT_ARGV })
                .checkMissingSproxydKeys).toBe(false);
        });

        test('should throw if both CHECK_MISSING_SPROXYD_KEYS and NO_MISSING_KEY_CHECK are enabled', () => {
            const env = {
                ...MINIMAL_ENV,
                CHECK_MISSING_SPROXYD_KEYS: '1',
                NO_MISSING_KEY_CHECK: '1',
            };
            expect(() => getConfig({ env })).toThrow('cannot be enabled together');
        });

        test('should throw if both --check-missing-sproxyd-keys and NO_MISSING_KEY_CHECK are enabled', () => {
            const env = { ...MINIMAL_ENV, NO_MISSING_KEY_CHECK: '1' };
            const argv = [...LEGACY_SCRIPT_ARGV, '--check-missing-sproxyd-keys'];
            expect(() => getConfig({ env, argv })).toThrow('cannot be enabled together');
        });
    });

    describe('validation', () => {
        test('should accept KEYS_FROM_STDIN as the only source', () => {
            expect(getConfig({ env: { KEYS_FROM_STDIN: '1' } }).keysFromStdin).toBe(true);
        });

        test('should throw if none of BUCKETS, RAFT_SESSIONS or KEYS_FROM_STDIN is set', () => {
            expect(() => getConfig({ env: {} })).toThrow(
                'one of BUCKETS (--buckets), RAFT_SESSIONS (--raft-sessions), '
                + 'KEYS_FROM_STDIN (--keys-from-stdin) must be defined');
        });

        test('should not count KEYS_FROM_STDIN as a source when false', () => {
            expect(() => getConfig({ env: { KEYS_FROM_STDIN: '0' } })).toThrow('must be defined');
        });

        test('should throw if more than one of BUCKETS, RAFT_SESSIONS or KEYS_FROM_STDIN is set', () => {
            const env = { ...MINIMAL_ENV, RAFT_SESSIONS: '1' };
            expect(() => getConfig({ env })).toThrow('only one of');
        });
    });
});
