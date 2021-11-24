const { BoundedMap, MultiMap, SproxydKeysProcessor } = require('../../../SproxydKeysScan/DuplicateKeysWindow');
const { DuplicateSproxydKeyFoundHandler } = require('../../../SproxydKeysScan/SproxydKeysSubscribers');

const randomize = require('randomatic');
const range = require('lodash/range');

describe('DuplicateKeysWindow', () => {
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
            const handler1 = () => {};
            const handler2 = () => {};

            const multiMap = new MultiMap();
            multiMap.set('event_1', handler1);
            multiMap.set('event_1', handler2);

            expect(multiMap.get('event_1')).toBeInstanceOf(Array);
            expect(multiMap.get('event_1').length).toEqual(2);
        });
    });

    describe('SproxydKeyProcessor', () => {
        const windowSize = 10;

        const setupProcessor = windowSize => {
            const subscribers = new MultiMap();
            const duplicateHandler = new DuplicateSproxydKeyFoundHandler();
            duplicateHandler._repairObject = jest.fn().mockReturnValue((err, res) => [err, res]);
            subscribers.set('duplicateSproxydKeyFound', duplicateHandler);

            const processor = new SproxydKeysProcessor(windowSize, subscribers);
            return [processor, duplicateHandler];
        };

        test('sets and updates keys when all unique keys are inserted', () => {
            const [processor, duplicateHandler] = setupProcessor();

            const objectKey = 'objectKey-1';
            const sproxydKeys = range(windowSize).map(() => randomize('A0', 40));
            processor.insert(objectKey, sproxydKeys);
            expect(duplicateHandler._repairObject).not.toHaveBeenCalled();
            expect(processor.sproxydKeys.size).toEqual(windowSize);
        });

        test('calls duplicateSproxydKeyFound handler when duplicate is found', () => {
            const [processor, duplicateHandler] = setupProcessor();

            const objectKey1 = 'objectKey-1';
            const sproxydKeys = range(windowSize).map(() => randomize('A0', 40));
            const objectKey2 = 'objectKey-2';

            processor.insert(objectKey1, sproxydKeys);
            processor.insert(objectKey2, sproxydKeys);

            expect(duplicateHandler._repairObject).toHaveBeenCalledTimes(windowSize);
            expect(processor.sproxydKeys.size).toEqual(windowSize);
        });
    });
});
