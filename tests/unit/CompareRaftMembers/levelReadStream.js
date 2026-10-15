const fs = require('fs');
const os = require('os');
const path = require('path');
const { ClassicLevel } = require('classic-level');

const BlockDigestsStorage = require('../../../CompareRaftMembers/BlockDigestsStorage');
const levelReadStream = require('../../../CompareRaftMembers/levelReadStream');

function collect(readable) {
    return new Promise((resolve, reject) => {
        const entries = [];
        readable
            .on('data', entry => entries.push(entry))
            .on('end', () => resolve(entries))
            .on('error', reject);
    });
}

describe('levelReadStream', () => {
    let tmpDir;
    let levelPath;

    beforeAll(done => {
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'levelReadStream-'));
        levelPath = path.join(tmpDir, 'digests');
        const storage = new BlockDigestsStorage({ levelPath });
        ['bucket/a', 'bucket/b', 'bucket/c', 'bucket10/a'].forEach((lastKey, i) => {
            storage.write({ size: i, digest: `digest${i}`, lastKey });
        });
        storage.end();
        storage.on('finish', done);
    });

    afterAll(() => {
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });

    test('should stream the digests written by BlockDigestsStorage', async () => {
        const db = new ClassicLevel(levelPath, { createIfMissing: false });
        const entries = await collect(levelReadStream(db));
        await db.close();
        expect(entries).toEqual([
            { key: 'bucket/a', value: '{"size":0,"digest":"digest0"}' },
            { key: 'bucket/b', value: '{"size":1,"digest":"digest1"}' },
            { key: 'bucket/c', value: '{"size":2,"digest":"digest2"}' },
            { key: 'bucket10/a', value: '{"size":3,"digest":"digest3"}' },
        ]);
    });

    test('should honor range options', async () => {
        const db = new ClassicLevel(levelPath, { createIfMissing: false });
        const entries = await collect(levelReadStream(db, { gte: 'bucket/b', lt: 'bucket0' }));
        await db.close();
        expect(entries.map(({ key }) => key)).toEqual(['bucket/b', 'bucket/c']);
    });

    test('should not end and release the iterator when destroyed from a data handler', async () => {
        const db = new ClassicLevel(levelPath, { createIfMissing: false });
        const readable = levelReadStream(db);
        const onEnd = jest.fn();
        readable.on('end', onEnd);
        await new Promise(resolve => {
            readable.once('data', () => {
                readable.destroy();
                readable.on('close', resolve);
            });
        });
        expect(onEnd).not.toHaveBeenCalled();
        // closing the database waits for open iterators to be closed
        await db.close();
        expect(db.status).toEqual('closed');
    });

    test('should drop an entry read after being destroyed', async () => {
        let resolveNext;
        const iterator = {
            next: () => new Promise(resolve => {
                resolveNext = resolve;
            }),
            close: jest.fn(async () => {}),
        };
        const readable = levelReadStream({ iterator: () => iterator });
        const onData = jest.fn();
        readable.on('data', onData);
        await new Promise(resolve => setImmediate(resolve));
        readable.destroy();
        resolveNext(['bucket/a', 'value']);
        await new Promise(resolve => readable.on('close', resolve));
        expect(onData).not.toHaveBeenCalled();
        expect(iterator.close).toHaveBeenCalled();
    });

    test('should emit iterator errors and close the iterator', async () => {
        const iterator = {
            next: async () => {
                throw new Error('read failure');
            },
            close: jest.fn(async () => {}),
        };
        const readable = levelReadStream({ iterator: () => iterator });
        await expect(collect(readable)).rejects.toThrow('read failure');
        expect(iterator.close).toHaveBeenCalled();
    });

    test('should report a failure to close the iterator', async () => {
        const iterator = {
            next: () => new Promise(() => {}),
            close: async () => {
                throw new Error('close failure');
            },
        };
        const readable = levelReadStream({ iterator: () => iterator });
        const error = new Promise(resolve => readable.on('error', resolve));
        readable.destroy();
        await expect(error).resolves.toThrow('close failure');
    });
});
