const { PassThrough } = require('stream');
const werelogs = require('werelogs');
const {
    StalledCursorWrapper,
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

const log = new werelogs.Logger('S3Utils::StalledRetry::CursorWrapper');

const cmpDate = new Date('2020-01-01T00:00:00.000Z');

function writeItemsTo(writable, items) {
    items.forEach(item => {
        writable.write(item);
    });
}

describe('StalledCursorWrapper', () => {
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
        const wrapper = new StalledCursorWrapper(
            mockCursor,
            {
                queueLimit: 1,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            }
        );

        writeItemsTo(mockCursor, [
            generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2'
            ),
            generateObjectMD(
                'testobject-1',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2'
            ),
        ]);

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(false);
            expect(wrapper.cursor.isPaused()).toEqual(true);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.queue.length).toEqual(2);
            done();
        }, 100);
    });

    test('should ignore non-stalled items', done => {
        const wrapper = new StalledCursorWrapper(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            }
        );

        writeItemsTo(mockCursor, [
            generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, +24),
                'us-east-1,us-east-2'
            ),
        ]);

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(false);
            expect(wrapper.cursor.isPaused()).toEqual(false);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.queue.length).toEqual(0);
            done();
        }, 100);
    });

    test('should queue stalled items', done => {
        const wrapper = new StalledCursorWrapper(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            }
        );

        writeItemsTo(mockCursor, [
            generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2'
            ),
        ]);

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(false);
            expect(wrapper.cursor.isPaused()).toEqual(false);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.queue.length).toEqual(2);
            done();
        }, 100);
    });

    test('should handle cursor error', done => {
        const wrapper = new StalledCursorWrapper(
            mockCursor,
            {
                queueLimit: 1,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            }
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
        const wrapper = new StalledCursorWrapper(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            }
        );

        writeItemsTo(mockCursor, [
            generateObjectMD(
                'testobject-0',
                generateModifiedDateString(cmpDate, -24),
                'us-east-1,us-east-2'
            ),
        ]);
        mockCursor.end();

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(true);
            expect(wrapper.completed).toEqual(false);
            // expect 2 entries to be queued from the first item
            expect(wrapper.queue.length).toEqual(2);
            done();
        }, 100);
    });

    test('should handle cursor end with 0 items in queue', done => {
        const wrapper = new StalledCursorWrapper(
            mockCursor,
            {
                queueLimit: 1000,
                bucketName: 'test-bucket',
                cmpDate,
                log,
            }
        );

        mockCursor.end();

        setTimeout(() => {
            expect(wrapper.cursorErr).toBeNull();
            expect(wrapper.cursorEnd).toEqual(true);
            expect(wrapper.completed).toEqual(false);
            done();
        }, 100);
    });

    describe('::getBatch', () => {
        test('should wait on source for data', done => {
            const wrapper = new StalledCursorWrapper(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                }
            );

            writeItemsTo(mockCursor, [
                generateObjectMD(
                    'testobject-0',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1,us-east-2'
                ),
                generateObjectMD(
                    'testobject-1',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1,us-east-2'
                ),
            ]);

            wrapper.getBatch(1, (err, batch) => {
                expect(err).toBeNull();
                expect(batch.length).toEqual(1);
                expect(wrapper.queue.length).toEqual(3);
                done();
            });
        });

        test('should destroy source on completion', done => {
            const wrapper = new StalledCursorWrapper(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                }
            );

            writeItemsTo(mockCursor, [
                generateObjectMD(
                    'testobject-0',
                    generateModifiedDateString(cmpDate, -24),
                    'us-east-1'
                ),
            ]);
            mockCursor.end();

            setTimeout(() => {
                wrapper.getBatch(1, err => {
                    expect(err).toBeNull();
                    expect(wrapper.cursorEnd).toEqual(true);
                    expect(wrapper.cursor).toEqual(null);
                    done();
                });
            }, 1000);
        });

        test('should return error', done => {
            const wrapper = new StalledCursorWrapper(
                mockCursor,
                {
                    queueLimit: 1000,
                    bucketName: 'test-bucket',
                    cmpDate,
                    log,
                }
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
    });
});
