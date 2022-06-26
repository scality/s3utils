const BlockDigestsStream = require('../../../CompareRaftMembers/BlockDigestsStream');

describe('BlockDigestsStream', () => {
    describe('should transform a stream of { key, value } items', () => {
        [
            {
                desc: 'no item',
                blockSize: 4,
                input: [],
                output: [],
            },
            {
                desc: 'one item',
                blockSize: 4,
                input: [
                    { key: 'key1', value: 'value1' },
                ],
                output: [
                    { size: 1, lastKey: 'key1', digest: 'd4e3b2f94f165f3310fb12cf714146c4' },
                ],
            },
            {
                desc: '4 items',
                blockSize: 4,
                input: [
                    { key: 'key1', value: 'value1' },
                    { key: 'key2', value: 'value2' },
                    { key: 'key3', value: 'value3' },
                    { key: 'key4', value: 'value4' },
                ],
                output: [
                    { size: 4, lastKey: 'key4', digest: 'c082e6a395e8977b97cd5fa04dc18149' },
                ],
            },
            {
                desc: '5 items',
                blockSize: 4,
                input: [
                    { key: 'key1', value: 'value1' },
                    { key: 'key2', value: 'value2' },
                    { key: 'key3', value: 'value3' },
                    { key: 'key4', value: 'value4' },
                    { key: 'key5', value: 'value5' },
                ],
                output: [
                    { size: 4, lastKey: 'key4', digest: 'c082e6a395e8977b97cd5fa04dc18149' },
                    { size: 1, lastKey: 'key5', digest: '93556b596ad97575fc774a187cc7936b' },
                ],
            },
            {
                desc: '5 items with forced flush',
                blockSize: 4,
                input: [
                    { key: 'key1', value: 'value1' },
                    { key: 'key2', value: 'value2' },
                    { key: 'key3', value: 'value3' },
                    { key: 'key4', value: 'value4' },
                    { key: 'key5', value: 'value5' },
                ],
                flushAtKeys: ['key2'],
                output: [
                    { size: 2, lastKey: 'key2', digest: '5292c0746ab27bd83c8d720c2e99a25c' },
                    { size: 3, lastKey: 'key5', digest: 'd7097c3ab32f536666cd3f514c15727e' },
                ],
            },
            {
                desc: '5000 items',
                blockSize: 1000,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        input.push({ key: `key${i}`, value: `value${i}` });
                    }
                    return input;
                },
                output: [
                    { size: 1000, lastKey: 'key999', digest: '155c72c54ecde7bc2a2f7ea8ec96d3c6' },
                    { size: 1000, lastKey: 'key1999', digest: '5f3b32472d104e3aec532f168b2e7397' },
                    { size: 1000, lastKey: 'key2999', digest: '1ac3a4575a2441a7ceeb1fb62fbf1ad8' },
                    { size: 1000, lastKey: 'key3999', digest: 'b9784c796d36790c76de60972136ee36' },
                    { size: 1000, lastKey: 'key4999', digest: '538d55b502fe656665cf782d4e62d170' },
                ],
            },
            {
                desc: '5000 items, baseline with one value-modified item',
                blockSize: 1000,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        if (i === 3333) {
                            input.push({ key: `key${i}`, value: `value${i}-mod` });
                        } else {
                            input.push({ key: `key${i}`, value: `value${i}` });
                        }
                    }
                    return input;
                },
                output: [
                    { size: 1000, lastKey: 'key999', digest: '155c72c54ecde7bc2a2f7ea8ec96d3c6' },
                    { size: 1000, lastKey: 'key1999', digest: '5f3b32472d104e3aec532f168b2e7397' },
                    { size: 1000, lastKey: 'key2999', digest: '1ac3a4575a2441a7ceeb1fb62fbf1ad8' },
                    { size: 1000, lastKey: 'key3999', digest: '3b664fe0d980a517dae6202ce8674f60' },
                    { size: 1000, lastKey: 'key4999', digest: '538d55b502fe656665cf782d4e62d170' },
                ],
            },
            {
                desc: '5000 items, no automatic block boundary',
                blockSize: 0,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        input.push({ key: `key${i}`, value: `value${i}` });
                    }
                    return input;
                },
                output: [
                    { size: 5000, lastKey: 'key4999', digest: 'b3c7724f5128a72483c5e6e2b1b27a0c' },
                ],
            },
        ].forEach(testCase => {
            let input;
            if (typeof testCase.input === 'function') {
                input = testCase.input();
            } else {
                input = testCase.input;
            }
            test(`with input of ${testCase.desc} yielding ${testCase.output.length} `
            + `blocks with blockSize=${testCase.blockSize}`, done => {
                const output = [];
                const rbs = new BlockDigestsStream({
                    blockSize: testCase.blockSize,
                    hashAlgorithm: 'md5',
                });
                rbs
                    .on('data', data => {
                        output.push(data);
                    })
                    .on('end', () => {
                        expect(output).toEqual(testCase.output);
                        done();
                    })
                    .on('error', done);
                input.forEach(item => {
                    rbs.write(item);
                    if (testCase.flushAtKeys
                        && testCase.flushAtKeys.includes(item.key)) {
                        rbs.flush();
                    }
                });
                rbs.end();
            });
        });
    });
});
