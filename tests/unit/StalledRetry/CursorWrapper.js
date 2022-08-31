const { PassThrough } = require('stream');
const werelogs = require('werelogs');
const {
    RateLimitingCursor,
} = require('../../../StalledRetry/CursorWrapper');

const {
    generateObjectMD,
    generateModifiedDateString,
} = require('../../utils');

const loggerConfig = {
    level: 'debug',
    dump: 'error',
};

werelogs.configure(loggerConfig);

const log = new werelogs.Logger('S3Utils::StalledRetry::RateLimitingCursor');

const cmpDate = new Date('2020-01-01T00:00:00.000Z');

function writeItemsTo(writable, items) {
    items.forEach(item => {
        writable.write(item);
    });
}

describe('RateLimitingCursor', () => {
    let mockCursor;

    beforeEach(() => {
        mockCursor = new PassThrough({
            objectMode: true,
            highWaterMark: 1000, // 1000 objects read/write
        });
    });

    afterEach(() => {
        mockCursor.destroy();
    });

    test('should pause source stream if queue limit has been reached', done => {
        const wrapper = new RateLimitingCursor(
            mockCursor,
            {
                queueLimit: 1,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            },
        );

        writeItemsTo(mockCursor, [
            ...generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2',
            ),
            ...generateObjectMD(
                'testobject-1',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2',
            ),
        ]);

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(false);
            expect(wrapper.cursor.isPaused()).toEqual(true);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.buffer.length).toEqual(2);
            done();
        }, 100);
    });

    test('should ignore non-stalled items', done => {
        const wrapper = new RateLimitingCursor(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            },
        );

        writeItemsTo(mockCursor, [
            ...generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, +24),
                'us-east-1,us-east-2',
            ),
        ]);

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(false);
            expect(wrapper.cursor.isPaused()).toEqual(false);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.buffer.length).toEqual(0);
            done();
        }, 100);
    });

    test('should queue stalled items', done => {
        const wrapper = new RateLimitingCursor(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            },
        );

        writeItemsTo(mockCursor, [
            ...generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2',
            ),
        ]);

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(false);
            expect(wrapper.cursor.isPaused()).toEqual(false);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.buffer.length).toEqual(2);
            done();
        }, 100);
    });

    test('should handle cursor error', done => {
        const wrapper = new RateLimitingCursor(
            mockCursor,
            {
                queueLimit: 1,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            },
        );

        mockCursor.emit('error', new Error('test error'));

        setTimeout(() => {
            expect(wrapper.cursorErr).toEqual(new Error('test error'));
            expect(wrapper.cursorEnd).toEqual(true);
            expect(wrapper.completed).toEqual(true);
            done();
        }, 100);
    });

    test('should handle cursor end with items in queue', done => {
        const wrapper = new RateLimitingCursor(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            },
        );

        writeItemsTo(mockCursor, [
            ...generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2',
            ),
        ]);
        mockCursor.end();

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(true);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.buffer.length).toEqual(2);
            done();
        }, 100);
    });

    test('should handle cursor end with 0 items in queue', done => {
        const wrapper = new RateLimitingCursor(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            },
        );

        mockCursor.end();

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(true);
            expect(wrapper.completed).toEqual(true);
            done();
        }, 100);
    });

    describe('::getBatch', () => {
        test('should add getBatch request to queue', done => {
            const wrapper = new RateLimitingCursor(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                },
            );

            wrapper.getBatch(1, () => {});

            setTimeout(() => {
                expect(wrapper.getBatchCallbacks).toHaveLength(1);
                done();
            }, 100);
        });

        test('should call queued callback when size is met', done => {
            const wrapper = new RateLimitingCursor(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                },
            );

            wrapper.getBatch(5, (err, batch) => {
                expect(err).toBeNull();
                expect(batch.length).toEqual(5);
                expect(wrapper.buffer.length).toEqual(1);
                done();
            });

            setTimeout(() => {
                writeItemsTo(mockCursor, [
                    ...generateObjectMD(
                        'testobject-0',
                        generateModifiedDateString(cmpDate, -24),
                        'us-east-1,us-east-2,us-east-3',
                    ),
                    ...generateObjectMD(
                        'testobject-1',
                        generateModifiedDateString(cmpDate, -24),
                        'us-east-1,us-east-2,us-east-3',
                    ),
                ]);
            }, 100);
        });

        test('should destroy source on completion', done => {
            const wrapper = new RateLimitingCursor(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                },
            );

            writeItemsTo(mockCursor, [
                ...generateObjectMD(
                    'testobject-0',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1',
                ),
            ]);
            mockCursor.end();

            setTimeout(() => {
                wrapper.getBatch(1, err => {
                    expect(err).toBeNull();
                    expect(wrapper.cursorEnd).toEqual(true);
                    expect(wrapper.completed).toEqual(true);
                    expect(wrapper.cursor).toEqual(null);
                    done();
                });
            }, 1000);
        });

        test('should return error', done => {
            const wrapper = new RateLimitingCursor(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                },
            );

            wrapper.getBatch(1, (err, batch) => {
                expect(err).toEqual(new Error('test error'));
                expect(batch).toBeNull();
                done();
            });

            setTimeout(() => {
                mockCursor.emit('error', new Error('test error'));
            }, 100);
        });

        test('should call queued getBatch requests on source end', done => {
            const wrapper = new RateLimitingCursor(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                },
            );

            writeItemsTo(mockCursor, [
                ...generateObjectMD(
                    'testobject-0',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1,us-east-2,us-east-3',
                ),
                ...generateObjectMD(
                    'testobject-1',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1,us-east-2,us-east-3',
                ),
            ]);

            wrapper.getBatch(3, (err, batch) => {
                expect(err).toBeNull();
                expect(batch.length).toEqual(3);
                expect(wrapper.completed).toEqual(false);
            });

            wrapper.getBatch(2, (err, batch) => {
                expect(err).toBeNull();
                expect(batch.length).toEqual(2);
                expect(wrapper.completed).toEqual(false);
            });

            wrapper.getBatch(1, (err, batch) => {
                expect(err).toBeNull();
                expect(batch.length).toEqual(1);
                expect(wrapper.completed).toEqual(true);
                done();
            });

            mockCursor.end();
        });

        test('should resume paused source', done => {
            const wrapper = new RateLimitingCursor(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                },
            );

            mockCursor.pause();
            writeItemsTo(mockCursor, [
                ...generateObjectMD(
                    'testobject-0',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1,us-east-2,us-east-3',
                ),
                ...generateObjectMD(
                    'testobject-1',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1,us-east-2,us-east-3',
                ),
            ]);

            setTimeout(() => {
                expect(wrapper.cursor.isPaused()).toEqual(true);
                wrapper.getBatch(1, (err, batch) => {
                    expect(err).toBeNull();
                    expect(batch.length).toEqual(1);
                    expect(wrapper.completed).toEqual(false);
                    expect(wrapper.cursorEnd).toEqual(false);
                    expect(wrapper.cursor.isPaused()).toEqual(false);
                    done();
                });
            }, 1000);
        });
    });
});
