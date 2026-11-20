const {
    parseOptions,
    isHelpRequested,
    getOptionsUsage,
    parseBoolean,
    getEnvName,
    getCliName,
    describeOption,
} = require('../../../utils/parseOptions');

const OPTIONS = {
    someString: {
        description: 'some string',
        type: 'string',
        default: 'default-string',
    },
    someList: {
        description: 'some list with a long description that has to be wrapped '
            + 'over multiple lines to fit in the usage width',
        type: 'list',
        default: undefined,
    },
    someBoolean: {
        description: 'some boolean',
        type: 'boolean',
        default: false,
    },
    someInt: {
        description: 'some int',
        type: 'int',
        default: 42,
    },
    someComputed: {
        description: 'some computed',
        type: 'string',
        default: ({ suffix }) => `computed-${suffix}`,
    },
};

const DEFAULTS_CONTEXT = { suffix: 'foo' };

function parse(env, args) {
    return parseOptions(OPTIONS, { env, args, defaultsContext: DEFAULTS_CONTEXT });
}

describe('utils/parseOptions', () => {
    describe('option names', () => {
        test('should derive the environment variable name', () => {
            expect(getEnvName('raftSessions')).toBe('RAFT_SESSIONS');
            expect(getEnvName('listingDigestsOutputDir')).toBe('LISTING_DIGESTS_OUTPUT_DIR');
            expect(getEnvName('workers')).toBe('WORKERS');
        });

        test('should derive the command-line option name', () => {
            expect(getCliName('raftSessions')).toBe('raft-sessions');
            expect(getCliName('listingDigestsOutputDir')).toBe('listing-digests-output-dir');
            expect(getCliName('workers')).toBe('workers');
        });

        test('should describe an option with both names', () => {
            expect(describeOption('raftSessions')).toBe('RAFT_SESSIONS (--raft-sessions)');
        });
    });

    describe('parseBoolean', () => {
        ['1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES'].forEach(value => {
            test(`should parse "${value}" as true`, () => {
                expect(parseBoolean(value)).toBe(true);
            });
        });

        ['0', 'false', 'False', 'FALSE', 'no', 'No', 'NO'].forEach(value => {
            test(`should parse "${value}" as false`, () => {
                expect(parseBoolean(value)).toBe(false);
            });
        });

        [undefined, '', 'tRuE', 'yES', 'on', '2'].forEach(value => {
            test(`should parse ${JSON.stringify(value)} as undefined`, () => {
                expect(parseBoolean(value)).toBeUndefined();
            });
        });
    });

    describe('parseOptions', () => {
        test('should return default values when nothing is set', () => {
            expect(parse({}, [])).toEqual({
                someString: 'default-string',
                someList: undefined,
                someBoolean: false,
                someInt: 42,
                someComputed: 'computed-foo',
            });
        });

        test('should parse values from the environment', () => {
            const env = {
                SOME_STRING: 'env-string',
                SOME_LIST: 'a,b,c',
                SOME_BOOLEAN: 'yes',
                SOME_INT: '10',
                SOME_COMPUTED: 'env-computed',
            };
            expect(parse(env, [])).toEqual({
                someString: 'env-string',
                someList: ['a', 'b', 'c'],
                someBoolean: true,
                someInt: 10,
                someComputed: 'env-computed',
            });
        });

        test('should parse values from the command line', () => {
            const args = [
                '--some-string=cli-string',
                '--some-list', 'a,b',
                '--some-boolean',
                '--some-int', '7',
                '--some-computed', 'cli-computed',
            ];
            expect(parse({}, args)).toEqual({
                someString: 'cli-string',
                someList: ['a', 'b'],
                someBoolean: true,
                someInt: 7,
                someComputed: 'cli-computed',
            });
        });

        test('should give precedence to the command line over the environment', () => {
            const env = { SOME_STRING: 'env-string', SOME_BOOLEAN: '1', SOME_INT: '10' };
            const args = ['--some-string', 'cli-string', '--no-some-boolean', '--some-int=7'];
            const values = parse(env, args);
            expect(values.someString).toBe('cli-string');
            expect(values.someBoolean).toBe(false);
            expect(values.someInt).toBe(7);
        });

        test('should consider empty environment values as not set', () => {
            const env = { SOME_STRING: '', SOME_BOOLEAN: '', SOME_INT: '' };
            const values = parse(env, []);
            expect(values.someString).toBe('default-string');
            expect(values.someBoolean).toBe(false);
            expect(values.someInt).toBe(42);
        });

        test('should throw on invalid boolean environment values', () => {
            expect(() => parse({ SOME_BOOLEAN: 'maybe' }, [])).toThrow(
                'invalid value for SOME_BOOLEAN: "maybe" (expected one of 1, true, ');
        });

        ['abc', '2O', '0', '-1', '1.5', ' 10'].forEach(value => {
            test(`should throw on invalid integer environment value "${value}"`, () => {
                expect(() => parse({ SOME_INT: value }, [])).toThrow(
                    `invalid value for SOME_INT: "${value}" (expected a positive integer)`);
            });
        });

        test('should throw on invalid command-line values, even if set in the environment', () => {
            const env = { SOME_INT: '10' };
            expect(() => parse(env, ['--some-int=2O'])).toThrow(
                'invalid value for --some-int: "2O" (expected a positive integer)');
        });

        test('should throw on empty command-line values', () => {
            const env = { SOME_STRING: 'env-string' };
            expect(() => parse(env, ['--some-string='])).toThrow(
                'invalid value for --some-string: ""');
        });

        test('should throw on unknown command-line options', () => {
            expect(() => parse({}, ['--unknown'])).toThrow("Unknown option '--unknown'");
        });

        test('should throw on positional command-line arguments', () => {
            expect(() => parse({}, ['positional'])).toThrow('Unexpected argument');
        });

        test('should throw on missing command-line option values', () => {
            expect(() => parse({}, ['--some-string'])).toThrow('argument missing');
        });

        test('should throw when a value is given to a boolean command-line option', () => {
            expect(() => parse({}, ['--some-boolean=false'])).toThrow('does not take an argument');
        });
    });
    describe('isHelpRequested', () => {
        test('should detect --help and -h', () => {
            expect(isHelpRequested(['--some-int', '7', '--help'])).toBe(true);
            expect(isHelpRequested(['-h'])).toBe(true);
        });

        test('should not detect help when not requested', () => {
            expect(isHelpRequested([])).toBe(false);
            expect(isHelpRequested(['--some-boolean'])).toBe(false);
        });
    });

    describe('getOptionsUsage', () => {
        test('should document each option with its names, description and default', () => {
            expect(getOptionsUsage(OPTIONS)).toBe([
                '  -h, --help',
                '      show this help and exit',
                '  --some-string <value>, SOME_STRING=<value>',
                '      some string (default: default-string)',
                '  --some-list <list>, SOME_LIST=<list>',
                '      some list with a long description that has to be wrapped over multiple',
                '      lines to fit in the usage width',
                '  --[no-]some-boolean, SOME_BOOLEAN=<bool>',
                '      some boolean',
                '  --some-int <number>, SOME_INT=<number>',
                '      some int (default: 42)',
                '  --some-computed <value>, SOME_COMPUTED=<value>',
                '      some computed',
            ].join('\n'));
        });
    });
});
