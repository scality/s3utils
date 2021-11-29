const { AggregateLogger, ProxyLoggerCreator } = require('../../../SproxydKeysScan/Logging');
const { Logger } = require('werelogs');
describe.only('SproxydKeysScan:Logging', () => {
    describe('AggregateLogger', () => {
        let aggregateLogger = null;

        beforeAll(() => {
            aggregateLogger = new AggregateLogger(10);
            aggregateLogger.log.debug = jest.fn(message => message);
        });

        afterAll(() => {
            AggregateLogger._instance = null;
            jest.clearAllMocks();
        });
        test('it is a singleton', () => {
            const debug = aggregateLogger.log.debug;
            expect(AggregateLogger.hasOwnProperty('_instance')).toBe(true);
            const anotherAggregateLogger = new AggregateLogger(10);
            expect(debug).toHaveBeenCalled();
            const message = 'AggregateLogger is a singleton and has been instatiated. This instantiation is ignored';
            expect(debug).toHaveBeenCalledWith(message);
            expect(anotherAggregateLogger).toStrictEqual(aggregateLogger);
            expect(AggregateLogger._instance).toStrictEqual(aggregateLogger);
        });

        test('correctly logs aggregate events', () => {
            aggregateLogger.log.info = jest.fn((message, data) => [message, data]);
            const events = ['success', 'success', 'failure', 'failure', 'failure', 'in progress', 'in progress'];
            for (const event of events) {
                aggregateLogger.update(event);
            }
            const expectedSummary = { 'success': 2, 'failure': 3, 'in progress': 2 };

            const data = {
                beginTime: expect.anything(),
                endTime: expect.anything(),
                summary: expectedSummary,
            };

            aggregateLogger.logInterval();

            expect(aggregateLogger.log.info).toHaveBeenCalledTimes(1);
            expect(aggregateLogger.log.info).toHaveBeenCalledWith('Summary update:', data);
        });
    });

    describe('ProxyLoggerCreator', () => {
        let proxyLogger = null;
        let aggregateLogger = null;
        let logger = null;

        beforeAll(() => {
            logger = new Logger('ProxyLoggerCreator test');
            logger.info = jest.fn((message, data) => [message, data]);
            logger.warn = jest.fn((message, data) => [message, data]);
            aggregateLogger = new AggregateLogger(10);
            aggregateLogger.log.debug = jest.fn(message => message);
            proxyLogger = new ProxyLoggerCreator(logger);
        });

        afterAll(() => {
            AggregateLogger._instance = null;
            jest.clearAllMocks();
        });

        test('uses existing aggregateLogger', () => {
            expect(aggregateLogger.log.debug).toHaveBeenCalled();
            expect(aggregateLogger.log.debug).toHaveBeenCalledTimes(1);
        });

        test('calls to proxyLogger are intercepted by aggregateLogger', () => {
            proxyLogger.info('message', { some: 'data', eventMessage: 'proxy success' });
            expect(aggregateLogger.statistics.has('proxy success')).toBe(true);
        });

        test('calls to werelogs.Logger are made correctly', () => {
            const data = { some: 'data', eventMessage: 'proxy warning' };
            proxyLogger.warn('message', data);
            expect(logger.warn).toHaveBeenCalled();
            expect(logger.warn).toHaveBeenCalledTimes(1);
            expect(logger.warn).toHaveBeenCalledWith('message', data);
        });
    });
});
