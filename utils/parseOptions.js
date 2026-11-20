const { parseArgs } = require('util');

const TRUE_VALUES = ['1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES'];
const FALSE_VALUES = ['0', 'false', 'False', 'FALSE', 'no', 'No', 'NO'];

/**
 * Parse a boolean value
 *
 * @param {string} [value] - raw value
 * @return {boolean|undefined} - true if value is one of TRUE_VALUES,
 * false if value is one of FALSE_VALUES, undefined otherwise (not set
 * or not a valid boolean)
 */
function parseBoolean(value) {
    if (TRUE_VALUES.includes(value)) {
        return true;
    }
    if (FALSE_VALUES.includes(value)) {
        return false;
    }
    return undefined;
}

/**
 * Parse a positive integer value
 *
 * @param {string} value - raw value
 * @return {number|undefined} - parsed integer, or undefined if value
 * is not made only of digits or is zero
 */
function parsePositiveInt(value) {
    if (!/^[0-9]+$/.test(value)) {
        return undefined;
    }
    return Number.parseInt(value, 10) || undefined;
}

/**
 * Value parsers, by option type
 *
 * - parse: function receiving the raw string value (non-empty) and
 *   returning the parsed value, or undefined if the value is invalid
 * - expected: description of valid values, for error messages
 */
const PARSERS = {
    string: {
        parse: value => value,
        expected: 'a non-empty string',
    },
    list: {
        parse: value => value.split(','),
        expected: 'a comma-separated list',
    },
    boolean: {
        parse: parseBoolean,
        expected: `one of ${[...TRUE_VALUES, ...FALSE_VALUES].join(', ')}`,
    },
    int: {
        parse: parsePositiveInt,
        expected: 'a positive integer',
    },
};

/**
 * Parse a raw option value
 *
 * @param {object} option - option declaration
 * @param {string} rawValue - raw value
 * @param {string} source - where the value comes from, for error
 * messages, e.g. "--workers" or "WORKERS"
 * @return {any} - parsed value
 * @throws {Error} - if the value is empty or invalid
 */
function parseValue(option, rawValue, source) {
    const parser = PARSERS[option.type];
    const value = rawValue === '' ? undefined : parser.parse(rawValue);
    if (value === undefined) {
        throw new Error(`invalid value for ${source}: "${rawValue}" `
                        + `(expected ${parser.expected})`);
    }
    return value;
}

/**
 * Get the environment variable name of an option
 *
 * @param {string} name - option name in camelCase, e.g. "raftSessions"
 * @return {string} - environment variable name, e.g. "RAFT_SESSIONS"
 */
function getEnvName(name) {
    return name.replace(/[A-Z]/g, c => `_${c}`).toUpperCase();
}

/**
 * Get the command-line option name of an option
 *
 * @param {string} name - option name in camelCase, e.g. "raftSessions"
 * @return {string} - command-line option name, e.g. "raft-sessions"
 */
function getCliName(name) {
    return name.replace(/[A-Z]/g, c => `-${c.toLowerCase()}`);
}

/**
 * Describe an option with both its environment variable and
 * command-line option names, for error messages
 *
 * @param {string} name - option name
 * @return {string} - e.g. "RAFT_SESSIONS (--raft-sessions)"
 */
function describeOption(name) {
    return `${getEnvName(name)} (--${getCliName(name)})`;
}

/**
 * Parse command-line options
 *
 * @param {object} options - option declarations (see parseOptions())
 * @param {string[]} args - command-line arguments
 * @return {object} - mapping of option name to the raw value given
 * on the command line (boolean for boolean options, string otherwise)
 * @throws {Error} - on unknown options, positional arguments or
 * missing values
 */
function parseCommandLine(options, args) {
    const parseArgsOptions = {};
    for (const [name, option] of Object.entries(options)) {
        parseArgsOptions[getCliName(name)] = {
            type: option.type === 'boolean' ? 'boolean' : 'string',
        };
    }
    const { values } = parseArgs({
        args,
        options: parseArgsOptions,
        strict: true,
        allowPositionals: false,
        allowNegative: true,
    });
    const cliValues = {};
    for (const name of Object.keys(options)) {
        cliValues[name] = values[getCliName(name)];
    }
    return cliValues;
}

/**
 * Get the value of an option as set by the user, from the command
 * line or else from the environment
 *
 * @param {string} name - option name
 * @param {object} option - option declaration
 * @param {object} cliValues - command-line values from parseCommandLine()
 * @param {object} env - environment variables
 * @return {any} - parsed value, or undefined if not set (an empty
 * environment variable is considered as not set)
 * @throws {Error} - if the value is invalid, including an empty
 * command-line value
 */
function getUserValue(name, option, cliValues, env) {
    const cliValue = cliValues[name];
    if (typeof cliValue === 'boolean') {
        return cliValue;
    }
    if (cliValue !== undefined) {
        return parseValue(option, cliValue, `--${getCliName(name)}`);
    }
    const envName = getEnvName(name);
    const envValue = env[envName];
    return envValue ? parseValue(option, envValue, envName) : undefined;
}

/**
 * Parse options from the command line and environment variables
 *
 * Each option is keyed by its name in camelCase, from which are
 * derived its environment variable name in SNAKE_CASE and its
 * command-line long option name in kebab-case, e.g. "raftSessions"
 * is set by the RAFT_SESSIONS environment variable or the
 * --raft-sessions command-line option.
 *
 * On the command line, boolean options are enabled with --option-name
 * and disabled with --no-option-name, other options take a value with
 * --option-name=value or --option-name value.
 *
 * Command-line options take precedence over environment variables,
 * which take precedence over default values. Invalid values are
 * rejected with an error, while empty environment variables are
 * considered as not set.
 *
 * @param {object} options - option declarations, keyed by option name:
 * - description: human-readable description of the option, for
 *   documentation purpose (not used for parsing)
 * - type: one of "string", "list" (comma-separated), "boolean", "int"
 *   (positive integer)
 * - default: value used when the option is not set, or a function
 *   called with defaultsContext returning that value
 * @param {object} params - inputs
 * @param {object} params.env - environment variables (e.g. process.env)
 * @param {string[]} [params.args] - command-line arguments, without
 * the node executable and script path (e.g. process.argv.slice(2))
 * @param {object} [params.defaultsContext] - argument passed to
 * default value functions
 * @return {object} - mapping of option name to option value
 * @throws {Error} - on invalid command-line arguments or option values
 */
function parseOptions(options, { env, args = [], defaultsContext = {} }) {
    const cliValues = parseCommandLine(options, args);
    const values = {};
    for (const [name, option] of Object.entries(options)) {
        const userValue = getUserValue(name, option, cliValues, env);
        if (userValue !== undefined) {
            values[name] = userValue;
        } else if (typeof option.default === 'function') {
            values[name] = option.default(defaultsContext);
        } else {
            values[name] = option.default;
        }
    }
    return values;
}

const USAGE_WIDTH = 80;
const USAGE_OPTION_INDENT = '  ';
const USAGE_DESCRIPTION_INDENT = '      ';

// value placeholders shown in usage, by option type
const USAGE_PLACEHOLDERS = {
    string: '<value>',
    list: '<list>',
    boolean: '<bool>',
    int: '<number>',
};

/**
 * Wrap text to a maximum line width
 *
 * @param {string} text - text to wrap
 * @param {string} indent - indentation prepended to each line
 * @param {number} width - maximum line width, including indentation
 * @return {string[]} - wrapped lines
 */
function wrapText(text, indent, width) {
    const lines = [];
    let line = '';
    for (const word of text.split(/\s+/)) {
        if (line && indent.length + line.length + 1 + word.length > width) {
            lines.push(`${indent}${line}`);
            line = word;
        } else {
            line = line ? `${line} ${word}` : word;
        }
    }
    if (line) {
        lines.push(`${indent}${line}`);
    }
    return lines;
}

/**
 * Check if help is requested on the command line, with --help or -h
 *
 * This is meant to be checked before parseOptions(), so that help can
 * be shown even if other options are missing or invalid.
 *
 * @param {string[]} args - command-line arguments, without the node
 * executable and script path (e.g. process.argv.slice(2))
 * @return {boolean} - true if help is requested
 */
function isHelpRequested(args) {
    return args.includes('--help') || args.includes('-h');
}

/**
 * Generate the usage text documenting options
 *
 * Each option is shown with its command-line and environment variable
 * forms, followed by its description and its default value. The
 * default value is omitted when undefined, false or computed by a
 * function. The --help option (see isHelpRequested()) is shown first.
 *
 * @param {object} options - option declarations (see parseOptions())
 * @return {string} - usage text, one entry per option
 */
function getOptionsUsage(options) {
    const lines = [
        `${USAGE_OPTION_INDENT}-h, --help`,
        `${USAGE_DESCRIPTION_INDENT}show this help and exit`,
    ];
    for (const [name, option] of Object.entries(options)) {
        const placeholder = USAGE_PLACEHOLDERS[option.type];
        const cliForm = option.type === 'boolean'
            ? `--[no-]${getCliName(name)}`
            : `--${getCliName(name)} ${placeholder}`;
        lines.push(`${USAGE_OPTION_INDENT}${cliForm}, ${getEnvName(name)}=${placeholder}`);
        let { description } = option;
        if (option.default !== undefined
            && option.default !== false
            && typeof option.default !== 'function') {
            description += ` (default: ${option.default})`;
        }
        lines.push(...wrapText(description, USAGE_DESCRIPTION_INDENT, USAGE_WIDTH));
    }
    return lines.join('\n');
}

module.exports = {
    parseOptions,
    isHelpRequested,
    getOptionsUsage,
    parseBoolean,
    getEnvName,
    getCliName,
    describeOption,
};
