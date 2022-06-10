const BlockDigestsStream = require('../../../CompareRaftMembers/BlockDigestsStream');

describe('BlockDigestsStream', () => {
    describe('BlockDigestsStream._countLeadingZeros', () => {
        [
            { input: [], output: 0 },
            { input: [0], output: 8 },
            { input: [1], output: 7 },
            { input: [7], output: 5 },
            { input: [8], output: 4 },
            { input: [15], output: 4 },
            { input: [16], output: 3 },
            { input: [75], output: 1 },
            { input: [127], output: 1 },
            { input: [128], output: 0 },
            { input: [0, 0], output: 16 },
            { input: [0, 1], output: 15 },
            { input: [0, 127], output: 9 },
            { input: [0, 128], output: 8 },
            { input: [1, 1], output: 7 },
            { input: [75, 0], output: 1 },
            { input: [0, 0, 5, 0, 2], output: 21 },
            // MD5 buffers
            {
                input: [
                    0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04,
                    0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e,
                ],
                output: 0,
            },
            {
                input: [
                    0x00, 0x05, 0xf2, 0xc3, 0x25, 0x18, 0x33, 0xaa,
                    0x92, 0xca, 0xc3, 0x31, 0x29, 0x8d, 0xf6, 0x0b,
                ],
                output: 13,
            },
        ].forEach(testCase => {
            test(`${JSON.stringify(testCase.input)} => ${testCase.output}`, () => {
                const output = BlockDigestsStream._countLeadingZeros(Buffer.from(testCase.input));
                expect(output).toEqual(testCase.output);
            });
        });
    });

    describe('should transform a stream of { key, value } items', () => {
        [
            {
                desc: 'no item',
                blockSize: 4,
                input: [],
                output: [],
            },
            {
                desc: 'one non-boundary item',
                blockSize: 4,
                input: [
                    { key: 'key1', value: 'value1' },
                ],
                output: [
                    { size: 1, lastKey: 'key1', digest: 'd4e3b2f94f165f3310fb12cf714146c4' },
                ],
            },
            {
                desc: 'one boundary item',
                blockSize: 4,
                input: [
                    { key: 'key3', value: 'value3' },
                ],
                output: [
                    { size: 1, lastKey: 'key3', digest: '11b10f3bfe737735c22afc89e6bb6b5e' },
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
                    { size: 3, lastKey: 'key3', digest: '379dc3627570493a4c682cc7d8bbd6af' },
                    { size: 1, lastKey: 'key4', digest: '312539b024aae950100532ebeace7f4e' },
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
                    { size: 3, lastKey: 'key3', digest: '379dc3627570493a4c682cc7d8bbd6af' },
                    { size: 2, lastKey: 'key5', digest: 'da94242cf17a2aad0d2e7c9d49527c46' },
                ],
            },
            {
                desc: '5 items, reversed',
                blockSize: 4,
                input: [
                    { key: 'key5', value: 'value5' },
                    { key: 'key4', value: 'value4' },
                    { key: 'key3', value: 'value3' },
                    { key: 'key2', value: 'value2' },
                    { key: 'key1', value: 'value1' },
                ],
                output: [
                    // the order and hashes are different from
                    // previous test but we still see key3 and key5
                    // used as boundaries
                    { size: 1, lastKey: 'key5', digest: '93556b596ad97575fc774a187cc7936b' },
                    { size: 2, lastKey: 'key3', digest: 'f34ebbcb351c1a5bac23af185d188f6d' },
                    { size: 2, lastKey: 'key1', digest: '408012ca8923341d71f925440b77e087' },
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
                flushAtKeys: ['key2', 'key5'],
                output: [
                    { size: 2, lastKey: 'key2', digest: '5292c0746ab27bd83c8d720c2e99a25c' },
                    { size: 1, lastKey: 'key3', digest: '11b10f3bfe737735c22afc89e6bb6b5e' },
                    { size: 2, lastKey: 'key5', digest: 'da94242cf17a2aad0d2e7c9d49527c46' },
                ],
            },
            {
                desc: '5000 items, baseline',
                blockSize: 1024,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        input.push({ key: `key${i}`, value: `value${i}` });
                    }
                    return input;
                },
                output: [
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    { size: 153, lastKey: 'key3058', digest: 'e9e40a4afc031186ff5882967e6b50b6' },
                    { size: 399, lastKey: 'key3457', digest: 'd2fbef46362341694a8a6fda9c2ae760' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
                ],
            },
            {
                desc: '4999 items, baseline with one non-boundary removed',
                blockSize: 1024,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        if (i !== 3333) {
                            input.push({ key: `key${i}`, value: `value${i}` });
                        }
                    }
                    return input;
                },
                output: [
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    { size: 153, lastKey: 'key3058', digest: 'e9e40a4afc031186ff5882967e6b50b6' },
                    // digest of this block differs from baseline
                    { size: 398, lastKey: 'key3457', digest: 'b0658d3aba7d89f5fc1c8032c841346e' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
                ],
            },
            {
                desc: '4999 items, baseline with one boundary removed',
                blockSize: 1024,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        if (i !== 3058) {
                            input.push({ key: `key${i}`, value: `value${i}` });
                        }
                    }
                    return input;
                },
                output: [
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    // two baseline blocks merged into one due to removal of boundary
                    { size: 551, lastKey: 'key3457', digest: '3f4516a623b687d6f2e26d8a7e8564f1' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
                ],
            },
            {
                desc: '5001 items, baseline with one extra non-boundary inserted item',
                blockSize: 1024,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        if (i === 3333) {
                            input.push({ key: `key${i}-2`, value: `value${i}-2` });
                        }
                        input.push({ key: `key${i}`, value: `value${i}` });
                    }
                    return input;
                },
                output: [
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    // digest of this block differs from baseline
                    { size: 153, lastKey: 'key3058', digest: 'e9e40a4afc031186ff5882967e6b50b6' },
                    { size: 400, lastKey: 'key3457', digest: '8e1fa497eca463dc1ca392ba967885cb' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
                ],
            },
            {
                desc: '5001 items, baseline with one extra boundary inserted item',
                blockSize: 1024,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        // hash(key-2911-2) gives a boundary with blockSize=1024
                        if (i === 2911) {
                            input.push({ key: `key${i}-2`, value: `value${i}-2` });
                        }
                        input.push({ key: `key${i}`, value: `value${i}` });
                    }
                    return input;
                },
                output: [
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    // baseline block has been split into two by the new boundary
                    { size: 6, lastKey: 'key2911-2', digest: '3b43df9e65175315f346f63d73a5fc98' },
                    { size: 148, lastKey: 'key3058', digest: '77ba0e8d5abe55fbf4ceb1e0116a2ecd' },
                    { size: 399, lastKey: 'key3457', digest: 'd2fbef46362341694a8a6fda9c2ae760' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
                ],
            },
            {
                desc: '5000 items, baseline with one non-boundary value-modified item',
                blockSize: 1024,
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
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    { size: 153, lastKey: 'key3058', digest: 'e9e40a4afc031186ff5882967e6b50b6' },
                    // digest of this block differs from baseline
                    { size: 399, lastKey: 'key3457', digest: 'e3db0a5a8ef8af464d8008c1a1cf61ad' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
                ],
            },
            {
                desc: '5000 items, baseline with one boundary value-modified item',
                blockSize: 1024,
                input: () => {
                    const input = [];
                    for (let i = 0; i < 5000; ++i) {
                        if (i === 3058) {
                            input.push({ key: `key${i}`, value: `value${i}-mod` });
                        } else {
                            input.push({ key: `key${i}`, value: `value${i}` });
                        }
                    }
                    return input;
                },
                output: [
                    { size: 1494, lastKey: 'key1493', digest: 'd59d5a8c7088052645300508835ecf26' },
                    { size: 260, lastKey: 'key1753', digest: '43d9ae329250d448f3ceefddf51f949e' },
                    { size: 444, lastKey: 'key2197', digest: '546e67ec04e6f3e66e8544df324b9b73' },
                    { size: 602, lastKey: 'key2799', digest: 'bb6ca4fc7ec8c8ef4de88f552a23e493' },
                    { size: 106, lastKey: 'key2905', digest: 'bb075520ea97a904b0e59ee512854015' },
                    { size: 153, lastKey: 'key3058', digest: 'aa149b61c9242aef386f70948ea83c75' },
                    { size: 399, lastKey: 'key3457', digest: 'd2fbef46362341694a8a6fda9c2ae760' },
                    { size: 171, lastKey: 'key3628', digest: '2ba0356aae76b5a7e766c537bc75c36d' },
                    { size: 507, lastKey: 'key4135', digest: 'd24a657e9a8e71d5c11298687f423674' },
                    { size: 864, lastKey: 'key4999', digest: 'acd4525a142ce02f705032b35675485d' },
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
