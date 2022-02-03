jest.mock('node-uuid', () => require('../../mocks/uuid'));

const uuid = require('node-uuid');
const Subprocess = require('../../mocks/subprocess');

const CountWorkerObj = require('../../../CountItems/CountWorkerObj');

const createWorker = () => {
    const p = new Subprocess();
    const w = new CountWorkerObj('worker-id', p);
    return { p, w };
};

describe('CountItems::CountWorkerObj', () => {
    beforeEach(() => {
        jest.resetModules();
        uuid.v4.mockReset();
    });

    test('should add callback object to list', () => {
        const { w } = createWorker();
        expect(w.callbacks.size).toEqual(0);
        w._addCallback('test-id', 'test-type', () => {});
        expect(w.callbacks.size).toEqual(1);
    });

    test('should retrieve and remove callback object from list', () => {
        const { w } = createWorker();
        w._addCallback('test-id', 'test-type', () => {});
        expect(w.callbacks.size).toEqual(1);
        const cb = w._getCallback('test-id');
        expect(w.callbacks.size).toEqual(0);
        expect(typeof cb).toBe('function');
    });

    test('should set ready once the "online" event has been triggered', () => {
        const { p, w } = createWorker();
        p.emit('online');
        expect(w.ready).toBeTruthy();
    });

    test('should add readyCheck callback to list', () => {
        const { w } = createWorker();
        const wAddCallbackFn = jest.spyOn(w, '_addCallback');
        w.init(() => {});
        expect(wAddCallbackFn).toHaveBeenCalledTimes(1);
        expect(w.callbacks.has('readyCheck')).toBeTruthy();
    });

    test('should call "readyCheck" callback if it exists', () => {
        const { p, w } = createWorker();
        const wAddCallbackFn = jest.spyOn(w, '_addCallback');
        const wGetCallbackFn = jest.spyOn(w, '_getCallback');
        w.init(() => {
            expect(wAddCallbackFn).toHaveBeenCalledTimes(1);
            expect(wGetCallbackFn).toHaveBeenCalledTimes(1);
            expect(w.ready).toBeTruthy();
        });
        p.emit('online');
    });

    test('should invoke callback if ready was set before init call', done => {
        const { p, w } = createWorker();
        const wAddCallbackFn = jest.spyOn(w, '_addCallback');
        const wGetCallbackFn = jest.spyOn(w, '_getCallback');
        p.emit('online');
        setTimeout(() => {
            w.init(() => {
                expect(wAddCallbackFn).toHaveBeenCalledTimes(0);
                expect(wGetCallbackFn).toHaveBeenCalledTimes(1);
                expect(w.ready).toBeTruthy();
                done();
            });
        }, 100);
    });

    const tests = [
        [
            'should send/receive setup messages',
            {
                id: 0,
                connected: true,
                args: [],
                type: 'setup',
                status: 'passed',
                error: null,
            },
        ],
        [
            'should send/receive setup messages (error)',
            {
                id: 0,
                connected: true,
                args: [],
                type: 'setup',
                status: 'failed',
                error: 'setupFailed',
            },
        ],
        [
            'should send/receive teardown messages',
            {
                id: 0,
                connected: true,
                args: [],
                type: 'teardown',
                status: 'passed',
                error: null,
            },
        ],
        [
            'should send/receive teardown messages (error)',
            {
                id: 0,
                connected: true,
                args: [],
                type: 'teardown',
                status: 'failed',
                error: 'teardownFailed',
            },
        ],
        [
            'should send/receive count messages',
            {
                id: 0,
                connected: true,
                args: [{ name: 'test-bucket' }],
                type: 'count',
                status: 'passed',
                error: null,
            },
        ],
        [
            'should send/receive count messages (error)',
            {
                id: 0,
                connected: true,
                args: [{ name: 'test-bucket' }],
                type: 'count',
                status: 'failed',
                error: 'countFailed',
            },
        ],
    ];
    test.each(tests)('%s', (msg, testCase, done) => {
        const { p, w } = createWorker();
        w.clientConnected = testCase.connected;
        const pSendFn = jest.spyOn(p, 'send');
        uuid.v4.mockImplementationOnce(() => testCase.id);
        w[testCase.type](...testCase.args, err => {
            if (testCase.error) {
                expect(err).toEqual(new Error(testCase.error));
            } else {
                expect(err).toBeFalsy();
            }
            if (testCase.type === 'count') {
                expect(pSendFn).toHaveBeenCalledWith({
                    id: testCase.id,
                    owner: 'scality',
                    type: 'count',
                    bucketInfo: testCase.args[0],
                });
            } else {
                expect(pSendFn).toHaveBeenCalledWith({
                    id: testCase.id,
                    owner: 'scality',
                    type: testCase.type,
                });
            }
            done();
        });
        p.emit('message', {
            id: testCase.id,
            owner: 'scality',
            type: testCase.type,
            status: testCase.status,
            error: testCase.error,
        });
    });

    test('should invoke all stored callbacks if process exits', done => {
        const { p, w } = createWorker();
        w.clientConnected = true;
        const testCallbackFn = jest.fn();
        let id = 0;
        uuid.v4.mockImplementation(() => id++);
        w.init(testCallbackFn);
        w.setup(testCallbackFn);
        w.teardown(testCallbackFn);
        w.count({ name: 'test-bucket' }, testCallbackFn);
        expect(w.callbacks.size).toEqual(4);
        p.emit('exit');
        setTimeout(() => {
            expect(testCallbackFn).toHaveBeenCalledTimes(4);
            done();
        }, 500);
    });
});
