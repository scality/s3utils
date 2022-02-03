const async = require('async');
const { MultiMap, SproxydKeysProcessor } = require('../../ObjectRepair/DuplicateKeysWindow');
const { DuplicateSproxydKeyFoundHandler } = require('../../ObjectRepair/SproxydKeysSubscribers');

const setupProcessor = windowSize => {
    async.wrapASync = jest.fn().mockImplementation(fn => fn);
    const subscribers = new MultiMap();
    const duplicateHandler = new DuplicateSproxydKeyFoundHandler();
    duplicateHandler._repairObject = jest.fn().mockReturnValue((err, res) => [err, res]);
    duplicateHandler.queue.push = jest.fn().mockImplementation(data => data);
    subscribers.set('duplicateSproxydKeyFound', duplicateHandler);

    const processor = new SproxydKeysProcessor(windowSize, subscribers);
    return [processor, duplicateHandler];
};

module.exports = { setupProcessor };
