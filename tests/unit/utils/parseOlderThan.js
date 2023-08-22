const FakeTimers = require('@sinonjs/fake-timers');
const parseOlderThan = require('../../../utils/parseOlderThan');

describe('parseOlderThan', () => {
    let clock;
    beforeAll(() => {
        clock = FakeTimers.install({
            now: new Date('2022-07-14T00:00:00Z'),
        });
    });
    afterAll(() => {
        clock.restore();
    });

    test('as a date given in ISO format', () => {
        const d = parseOlderThan('2022-02-02T02:02:02Z');
        expect(d.toISOString()).toEqual('2022-02-02T02:02:02.000Z');
    });
    test('as one day past the current date', () => {
        const d = parseOlderThan('1 day');
        expect(d.toISOString()).toEqual('2022-07-13T00:00:00.000Z');
    });
    test('as 30 days past the current date', () => {
        const d = parseOlderThan('30 days');
        expect(d.toISOString()).toEqual('2022-06-14T00:00:00.000Z');
    });
    test('as 30 seconds past the current date', () => {
        const d = parseOlderThan('30 seconds');
        expect(d.toISOString()).toEqual('2022-07-13T23:59:30.000Z');
    });
});
