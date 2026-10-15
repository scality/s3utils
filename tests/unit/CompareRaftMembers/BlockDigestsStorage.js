const BlockDigestsStorage = require('../../../CompareRaftMembers/BlockDigestsStorage');

describe('BlockDigestsStorage', () => {
    [0, 1, 5000].forEach(nKeysToWrite => {
        test(`should write ${nKeysToWrite} digest keys`, done => {
            const keysRemainingToWrite = {};
            let closeDone = false;
            const dbMock = {
                batch: array => new Promise(resolve => {
                    setTimeout(() => {
                        array.forEach(item => {
                            expect(item.type).toEqual('put');
                            expect(item.value).toEqual(keysRemainingToWrite[item.key]);
                            delete keysRemainingToWrite[item.key];
                        });
                        resolve();
                    }, 10);
                }),
                close: () => new Promise(resolve => {
                    setTimeout(() => {
                        closeDone = true;
                        resolve();
                    }, 10);
                }),
            };
            const levelStream = new BlockDigestsStorage({ db: dbMock });
            for (let i = 0; i < nKeysToWrite; ++i) {
                const key = `000000${i}`.slice(-6);
                const digest = `DIGEST${i}`;
                const value = JSON.stringify({ size: 100 + i, digest });
                keysRemainingToWrite[key] = value;
                levelStream.write({ size: 100 + i, lastKey: key, digest });
            }
            levelStream.end();
            levelStream.on('finish', () => {
                // db should have been closed
                expect(closeDone).toBeTruthy();
                // all keys should have been written to leveldb after 'finish' is emitted
                expect(keysRemainingToWrite).toEqual({});
                done();
            });
        });
    });
});
