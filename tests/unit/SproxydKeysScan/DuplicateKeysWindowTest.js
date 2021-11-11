const { BoundedMap, MultiMap, SproxydKeysProcessor } = require('../../../SproxydKeysScan/DuplicateKeysWindow');
const randomize = require('randomatic');
const range = require('lodash/range');

// EE46F879EB689D97C28532E120D5FB5940D7C420
describe.only('DuplicateKeysWindow', () => {
    describe('BoundedMap', () => {
        test('grows in size up to the limit with each unique key', () => {
            const boundedMap = new BoundedMap(20);

            range(20).forEach(i => {
                boundedMap.setAndUpdate(randomize('A0', 40), `someMasterKey-${i}`);
                expect(boundedMap.size).toEqual(i + 1);
            });
        });

        test('does not grow past the size limit and removes old keys', () => {
            const maxSize = 10;
            const boundedMap = new BoundedMap(maxSize);
            range(40).forEach(i => {
                const removedKey = boundedMap.setAndUpdate(randomize('A0', 40), `someMasterKey-${i}`);
                expect(boundedMap.size).toBeLessThanOrEqual(maxSize);
                if (i < maxSize) {
                    expect(removedKey).toBe(null);
                } else {
                    expect(removedKey).not.toBe(null);
                }
            });
        });

        test('removes oldest key each time new unique key is inserted past the size limit', () => {
            const insertionOrder = [];
            const maxSize = 10;
            const boundedMap = new BoundedMap(maxSize);

            range(40).forEach(i => {
                const sproxydKey = randomize('A0', 40);
                insertionOrder.push(sproxydKey);
                const removedKey = boundedMap.setAndUpdate(sproxydKey, `someMasterKey-${i}`);
                expect(boundedMap.size).toBeLessThanOrEqual(maxSize);

                if (removedKey) {
                    const targetKey = insertionOrder[i - maxSize];
                    expect(targetKey).toEqual(removedKey);
                }
            });
        });
    });

    describe('MultiMap', () => {
        test('sets multiple values for each key', () => {

        });
    });

    describe('SproxydKeyProcessor', () => {
        test('sets and updates keys without when all unique keys are inserted', () => {

        });

        test('calls duplicateSproxydKeyFound handler when duplicate is found', () => {

        });
    });
});
