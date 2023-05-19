const async = require('async');
const stream = require('stream');

const KeyValueStreamDiff = require('../../../CompareRaftMembers/KeyValueStreamDiff');

class MockKeyValueStream extends stream.Readable {
    constructor(listingToSend) {
        super({ objectMode: true });

        this.listingToSend = listingToSend;
    }

    _read() {
        process.nextTick(() => {
            if (this.listingToSend.length === 0) {
                this.push(null);
            } else {
                const item = this.listingToSend.shift();
                this.push(item);
            }
        });
    }
}

describe('KeyValueStreamDiff', () => {
    describe('should output differences between streams of { key, value } items', () => {
        [
            {
                desc: 'with empty streams',
                streamEntries: [
                    [],
                    [],
                ],
                expectedOutput: [],
            },
            {
                desc: 'with respectively zero and one entry in each stream',
                streamEntries: [
                    [],
                    [
                        { key: 'bucket/key1', value: '{}' },
                    ],
                ],
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key1', value: '{}' },
                    ],
                ],
            },
            {
                // symmetric check of the previous one
                desc: 'with respectively one and zero entry in each stream',
                streamEntries: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                    ],
                    [],
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                        null,
                    ],
                ],
            },
            {
                desc: 'with a single identical entry in both streams',
                streamEntries: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                    ],
                    [
                        { key: 'bucket/key1', value: '{}' },
                    ],
                ],
                expectedOutput: [],
            },
            {
                desc: 'with a single entry in each stream with a different key',
                streamEntries: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                    ],
                    [
                        { key: 'bucket/key2', value: '{}' },
                    ],
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                        null,
                    ],
                    [
                        null,
                        { key: 'bucket/key2', value: '{}' },
                    ],
                ],
            },
            {
                desc: 'with a single entry with same key but different value in each stream',
                streamEntries: [
                    [
                        { key: 'bucket/key1', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key1', value: '{"foo":"qux"}' },
                    ],
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket/key1', value: '{"foo":"qux"}' },
                    ],
                ],
            },
            {
                desc: 'with respectively one and two entries in each stream',
                streamEntries: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                    ],
                    [
                        { key: 'bucket/key1', value: '{}' },
                        { key: 'bucket/key2', value: '{"foo":"bar"}' },
                    ],
                ],
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key2', value: '{"foo":"bar"}' },
                    ],
                ],
            },
            {
                desc: 'with two identical entries in each stream',
                streamEntries: [
                    [
                        { key: 'bucket1/key1', value: '{}' },
                        { key: 'bucket2/key1', value: '{}' },
                    ],
                    [
                        { key: 'bucket1/key1', value: '{}' },
                        { key: 'bucket2/key1', value: '{}' },
                    ],
                ],
                expectedOutput: [],
            },
            {
                desc: 'with two different entries in each stream',
                streamEntries: [
                    [
                        { key: 'bucket1/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket2/key1', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket1/key1', value: '{"foo":"qux"}' },
                        { key: 'bucket2/key1', value: '{"foo":"qux"}' },
                    ],
                ],
                expectedOutput: [
                    [
                        { key: 'bucket1/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket1/key1', value: '{"foo":"qux"}' },
                    ],
                    [
                        { key: 'bucket2/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket2/key1', value: '{"foo":"qux"}' },
                    ],
                ],
            },
            {
                desc: 'with 7777 identical entries in both streams',
                get streamEntries() {
                    const streamEntries = [[], []];
                    for (let s = 0; s < 2; ++s) {
                        for (let i = 0; i < 7777; ++i) {
                            const paddedI = `000000${i}`.slice(-6);
                            streamEntries[s].push({
                                key: `bucket/key-${paddedI}`,
                                value: '{"foo":"bar"}',
                            });
                        }
                    }
                    return streamEntries;
                },
                expectedOutput: [],
            },
            {
                desc: 'with respectively 7777 and zero entries in each stream',
                get streamEntries() {
                    const streamEntries = [[], []];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        streamEntries[0].push({
                            key: `bucket/key-${paddedI}`,
                            value: '{"foo":"bar"}',
                        });
                    }
                    return streamEntries;
                },
                get expectedOutput() {
                    const expectedDiff = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        expectedDiff.push([
                            { key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' },
                            null,
                        ]);
                    }
                    return expectedDiff;
                },
            },
            {
                desc: 'with 7777 entries in each stream with a few differences',
                get streamEntries() {
                    const streamEntries = [[], []];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        let value1;
                        if (i === 3333) {
                            value1 = '{"foo":"qux"}';
                        } else {
                            value1 = '{"foo":"bar"}';
                        }
                        const value2 = '{"foo":"bar"}';
                        if (i !== 2222) {
                            streamEntries[0].push({
                                key: `bucket/key-${paddedI}`,
                                value: value1,
                            });
                        }
                        if (i !== 4444) {
                            streamEntries[1].push({
                                key: `bucket/key-${paddedI}`,
                                value: value2,
                            });
                        }
                    }
                    return streamEntries;
                },
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key-002222', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key-003333', value: '{"foo":"qux"}' },
                        { key: 'bucket/key-003333', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key-004444', value: '{"foo":"bar"}' },
                        null,
                    ],
                ],
            },
            {
                desc: 'with respectively two and 7777 entries in each stream',
                get streamEntries() {
                    const streamEntries = [[], []];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        if ([1234, 6667].includes(i)) {
                            streamEntries[0].push({
                                key: `bucket/key-${paddedI}`,
                                value: '{"foo":"bar"}',
                            });
                        }
                        streamEntries[1].push({
                            key: `bucket/key-${paddedI}`,
                            value: '{"foo":"bar"}',
                        });
                    }
                    return streamEntries;
                },
                get expectedOutput() {
                    const expectedDiff = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (![1234, 6667].includes(i)) {
                            const paddedI = `000000${i}`.slice(-6);
                            expectedDiff.push([
                                null,
                                { key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' },
                            ]);
                        }
                    }
                    return expectedDiff;
                },
            },
        ].forEach(testCase => {
            const { expectedOutput } = testCase;
            test(`${testCase.desc} yielding ${expectedOutput.length} diff entries`, done => {
                const streams = testCase.streamEntries.map(
                    streamEntries => new MockKeyValueStream(streamEntries),
                );
                const output = [];
                const diffStream = new KeyValueStreamDiff(streams[0], streams[1]);
                diffStream
                    .on('data', data => {
                        output.push(data);
                    })
                    .on('end', () => {
                        expect(output).toEqual(expectedOutput);
                        done();
                    })
                    .on('error', done);
            });
        });
    });
});
