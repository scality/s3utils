const path = require('path');

const {
    parseOptions,
    parseBoolean,
    describeOption,
    getOptionsUsage,
} = require('../utils/parseOptions');

// Name of the script before it was renamed to metadataScan.js; it is
// kept as a symlink for backward compatibility, and keeps its original
// default behavior of checking for missing sproxyd keys.
const LEGACY_SCRIPT_NAME = 'verifyBucketSproxydKeys';

/**
 * Default value of the "checkMissingSproxydKeys" option
 *
 * The check is disabled by default, except when the script is invoked
 * with its legacy name "verifyBucketSproxydKeys.js", unless the legacy
 * NO_MISSING_KEY_CHECK environment variable is true.
 *
 * @param {object} context - defaults context
 * @param {string} context.scriptName - invoked script name, without
 * directory and ".js" extension
 * @param {object} context.env - environment variables
 * @return {boolean} - default value
 */
function getCheckMissingSproxydKeysDefault({ scriptName, env }) {
    return scriptName === LEGACY_SCRIPT_NAME
        && parseBoolean(env.NO_MISSING_KEY_CHECK) !== true;
}

/**
 * Configuration options of metadataScan, see utils/parseOptions.js
 * for the format and how environment variable and command-line
 * option names are derived from option names
 */
const OPTIONS = {
    bucketdHostport: {
        description: 'ip:port of bucketd endpoint',
        type: 'string',
        default: '127.0.0.1:9000',
    },
    sproxydHostport: {
        description: 'ip:port of sproxyd endpoint, only used when checking '
            + 'for missing sproxyd keys',
        type: 'string',
        default: '127.0.0.1:8181',
    },
    buckets: {
        description: 'comma-separated list of buckets to scan',
        type: 'list',
        default: undefined,
    },
    raftSessions: {
        description: 'comma-separated list of raft sessions to scan',
        type: 'list',
        default: undefined,
    },
    keysFromStdin: {
        description: 'read objects to scan from stdin, where the input is a '
            + 'stream of JSON objects, each of the form '
            + '{"bucket":"bucketname","key":"objectkey\\0objectversion"} - '
            + 'if "\\0objectversion" is not present, it checks the master key',
        type: 'boolean',
        default: false,
    },
    fromUrl: {
        description: 'URL from which to resume scanning ("s3://bucket[/key]")',
        type: 'string',
        default: undefined,
    },
    workers: {
        description: 'concurrency value for sproxyd requests',
        type: 'int',
        default: 100,
    },
    verbose: {
        description: 'more verbose output (shows one line for every sproxyd key)',
        type: 'boolean',
        default: false,
    },
    logProgressInterval: {
        description: 'interval in seconds between progress update log lines',
        type: 'int',
        default: 10,
    },
    listingLimit: {
        description: 'number of keys to list per listing request',
        type: 'int',
        default: 1000,
    },
    mpuOnly: {
        description: 'only scan objects uploaded with multipart upload method',
        type: 'boolean',
        default: false,
    },
    checkMissingSproxydKeys: {
        description: 'also check that all sproxyd keys referenced by objects '
            + 'exist on the RING: this sends a request to sproxyd for every '
            + 'key, which makes the scan significantly slower than a '
            + 'metadata-only scan',
        type: 'boolean',
        default: getCheckMissingSproxydKeysDefault,
    },
    listingDigestsOutputDir: {
        description: 'output listing digests into the specified directory '
            + '(in the LevelDB format)',
        type: 'string',
        default: undefined,
    },
    listingDigestsBlockSize: {
        description: 'number of keys in each listing digest block',
        type: 'int',
        default: 1000,
    },
};

function validateConfig(config, env) {
    const sourceNames = ['buckets', 'raftSessions', 'keysFromStdin'];
    const sources = sourceNames.filter(name => config[name]);
    const sourcesDesc = sourceNames.map(describeOption).join(', ');
    if (sources.length === 0) {
        throw new Error(`one of ${sourcesDesc} must be defined`);
    }
    if (sources.length > 1) {
        throw new Error(`only one of ${sourcesDesc} can be defined`);
    }
    // the check can only be enabled with NO_MISSING_KEY_CHECK=true
    // if explicitly requested, as its default is then false
    if (config.checkMissingSproxydKeys && parseBoolean(env.NO_MISSING_KEY_CHECK) === true) {
        throw new Error(`${describeOption('checkMissingSproxydKeys')} and NO_MISSING_KEY_CHECK `
                        + 'cannot be enabled together');
    }
}

/**
 * Build the metadataScan configuration from its inputs
 *
 * @param {object} params - configuration inputs
 * @param {object} params.env - environment variables (e.g. process.env)
 * @param {string[]} [params.argv] - command-line arguments
 * (e.g. process.argv): argv[1] is the invoked script path, which
 * keeps the symlink name if invoked through a symlink, and options
 * start at argv[2]
 * @return {object} - mapping of option name to option value
 * @throws {Error} - if the configuration is invalid
 */
function getConfig({ env, argv = [] }) {
    const scriptName = argv[1] ? path.basename(argv[1], '.js') : '';
    const config = parseOptions(OPTIONS, {
        env,
        args: argv.slice(2),
        defaultsContext: { scriptName, env },
    });
    validateConfig(config, env);
    return config;
}

// usage text documenting all options, generated from OPTIONS
const OPTIONS_USAGE = getOptionsUsage(OPTIONS);

module.exports = {
    getConfig,
    OPTIONS_USAGE,
};
