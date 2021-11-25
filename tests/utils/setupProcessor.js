const { MultiMap, SproxydKeysProcessor } = require('../../SproxydKeysScan/DuplicateKeysWindow');
const { DuplicateSproxydKeyFoundHandler } = require('../../SproxydKeysScan/SproxydKeysSubscribers');

const setupProcessor = windowSize => {
    const subscribers = new MultiMap();
    const duplicateHandler = new DuplicateSproxydKeyFoundHandler();
    duplicateHandler._repairObject = jest.fn().mockReturnValue((err, res) => [err, res]);
    subscribers.set('duplicateSproxydKeyFound', duplicateHandler);

    const processor = new SproxydKeysProcessor(windowSize, subscribers);
    return [processor, duplicateHandler];
};

module.exports = { setupProcessor };
