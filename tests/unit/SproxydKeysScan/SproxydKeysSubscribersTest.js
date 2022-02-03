const randomize = require('randomatic');
const range = require('lodash/range');
const { setupProcessor } = require('../../utils/setupProcessor');

describe('SproxydKeysSubscribers', () => {
    describe('DuplicateSproxydKeyFoundHandler', () => {
        const [processor, duplicateHandler] = setupProcessor(10);

        const key = 'test-2021-11-24T19-41-08-494Z/2\u000098362217123979999997RG001 ';
        const version = () => `${randomize('0', 3)}.${randomize('0', 3)}`;

        test('the higher (older) version is always repaired and then the lower (newer) version is set in map', () => {
            range(100).forEach(idx => {
                const params = {
                    objectKey: key + version(),
                    existingObjectKey: key + version(),
                    sproxydKey: '75FB916EF57176F3C28532E120D5FB59BDCF8020',
                    context: processor,
                    bucket: 'bucket-name',
                };
                const [newerVersion, olderVersion] = [params.objectKey, params.existingObjectKey].sort();

                const [olderVersionURL, newerVersionURL] = [olderVersion, newerVersion]
                    .map(obj => duplicateHandler._getObjectURL(params.bucket, obj));

                duplicateHandler.handle(params);
                if (idx === 0) {
                    expect(duplicateHandler.queue.push).toHaveBeenCalledWith(
                        { objectUrl: olderVersionURL, objectUrl2: newerVersionURL }, expect.anything()
                  );
                } else {
                    expect(duplicateHandler.queue.push).not.toHaveBeenCalledWith();
                }

                expect(processor.sproxydKeys.get(params.sproxydKey)).toEqual(newerVersion);
            });
            expect(duplicateHandler.visitedObjects.size).toEqual(100);
        });
    });
});
