const getObjectURL = require('../../MetadataScan/getObjectURL');
const getBucketdURL = require('../../MetadataScan/getBucketdURL');
const FindDuplicateSproxydKeys = require('../../MetadataScan/FindDuplicateSproxydKeys');
const validateLocations = require('../../MetadataScan/validateLocations');

describe('metadataScan', () => {
    test('getObjectURL', () => {
        expect(getObjectURL()).toEqual('s3://');
        expect(getObjectURL('foobucket')).toEqual('s3://foobucket');
        expect(getObjectURL('foobucket', 'fooobject'))
            .toEqual('s3://foobucket/fooobject');
    });

    test('getBucketdURL', () => {
        expect(getBucketdURL('bucketd:9000', {
            Bucket: 'bucket',
            Key: 'key',
        })).toEqual('http://bucketd:9000/default/bucket/bucket/key');
        expect(getBucketdURL('bucketd:9000', {
            Bucket: 'bucket',
            Key: 'key&$@=;:+ ,?\u0000/bar',
        })).toEqual('http://bucketd:9000/default/bucket/bucket/key%26%24%40%3D%3B%3A%2B%20%2C%3F%00%2Fbar');
        expect(getBucketdURL('bucketd:9000', {
            Bucket: 'bucket',
            MaxKeys: 1000,
            KeyMarker: 'key&$@=;:+ ,?\u0000/bar',
        })).toEqual('http://bucketd:9000/default/bucket/bucket?maxKeys=1000'
                    + '&marker=key%26%24%40%3D%3B%3A%2B%20%2C%3F%00%2Fbar');
    });

    test('FindDuplicateSproxydKeys', () => {
        const finder = new FindDuplicateSproxydKeys(5);
        expect(finder.insertVersion('obj1', ['k1', 'k2'])).toEqual(null);

        expect(finder.sproxydKeys).toEqual({ k1: 'obj1', k2: 'obj1' });
        expect(finder.versionsWindow).toEqual({ 0: ['k1', 'k2'] });

        expect(finder.insertVersion('obj2', ['k3'])).toEqual(null);
        expect(finder.insertVersion('obj3', ['k2']))
            .toEqual({ objectId: 'obj1', key: 'k2' });
        expect(finder.insertVersion('obj4', ['k4'])).toEqual(null);
        expect(finder.insertVersion('obj5', ['k1']))
            .toEqual({ objectId: 'obj1', key: 'k1' });
        // "obj1.k1" is now out of the window of 5 objects, so returns null now
        expect(finder.insertVersion('obj6', ['k1'])).toEqual(null);
        // "obj1.k2" is still out of the window
        expect(finder.insertVersion('obj7', ['k2'])).toEqual(null);
        // "obj4.k4" is still in the window, so detected as duplicate
        expect(finder.insertVersion('obj8', ['k5', 'k6', 'k4', 'k7', 'k8']))
            .toEqual({ objectId: 'obj4', key: 'k4' });

        expect(finder.sproxydKeys)
            .toEqual({
                k1: 'obj6',
                k5: 'obj8',
                k6: 'obj8',
                k7: 'obj8',
                k8: 'obj8',
            });
        expect(finder.versionsWindow)
            .toEqual({
                4: ['k1'],
                5: ['k1'],
                6: ['k2'],
                7: ['k5', 'k6', 'k4', 'k7', 'k8'],
            });

        // skipVersion() updates the window...
        expect(finder.skipVersion()).toEqual(null);
        expect(finder.skipVersion()).toEqual(null);
        expect(finder.skipVersion()).toEqual(null);
        expect(finder.insertVersion('obj9', ['k7']))
            .toEqual({ objectId: 'obj8', key: 'k7' });
        // ...hence "obj9.k7" is now out of the window
        expect(finder.insertVersion('obj9', ['k7'])).toEqual(null);

        // only obj9.k7 is now present in the map
        expect(finder.sproxydKeys).toEqual({ k7: 'obj9' });
        expect(finder.versionsWindow).toEqual({ 11: ['k7'], 12: ['k7'] });
    });

    describe('validateLocations', () => {
        const key1 = {
            key: '8df148c188a7369ef6b632b08e9a2a867c065761',
            size: 2097,
            start: 0,
            dataStoreName: 'us-east-1',
            dataStoreType: 'scality',
            dataStoreETag: '1:35bf6d36c0c721deceda5d15fa642a18',
        };
        const key2 = {
            key: '8df148c188a7369ef6b632b08e9a2a867c065762',
            size: 2098,
            start: 0,
            dataStoreName: 'us-east-1',
            dataStoreType: 'scality',
            dataStoreETag: '1:35bf6d36c0c721deceda5d15fa642a19',
        };
        let status;
        let findDuplicateSproxydKeys;

        beforeEach(() => {
            status = { objectsScanned: 0, objectsWithBrokenMetadata: 0 };
            findDuplicateSproxydKeys = { skipVersion: jest.fn() };
        });

        test('should return true for valid locations array', () => {
            const result = validateLocations('s3://bucket/key', [key1], status, findDuplicateSproxydKeys);

            expect(result).toEqual(true);
            expect(status.objectsScanned).toEqual(0);
            expect(status.objectsWithBrokenMetadata).toEqual(0);
            expect(findDuplicateSproxydKeys.skipVersion).not.toHaveBeenCalled();
        });

        test('should return true for multiple valid locations', () => {
            const result = validateLocations('s3://bucket/valid-key', [key1, key2], status, findDuplicateSproxydKeys);

            expect(result).toEqual(true);
            expect(status.objectsScanned).toEqual(0);
            expect(status.objectsWithBrokenMetadata).toEqual(0);
            expect(findDuplicateSproxydKeys.skipVersion).not.toHaveBeenCalled();
        });

        test('should return false for undefined locations', () => {
            const result = validateLocations('s3://bucket/broken-key', undefined, status, findDuplicateSproxydKeys);

            expect(result).toEqual(false);
            expect(status.objectsScanned).toEqual(1);
            expect(status.objectsWithBrokenMetadata).toEqual(1);
            expect(findDuplicateSproxydKeys.skipVersion).toHaveBeenCalledTimes(1);
        });

        test('should return false for null locations', () => {
            const result = validateLocations('s3://bucket/null-key', null, status, findDuplicateSproxydKeys);

            expect(result).toEqual(false);
            expect(status.objectsScanned).toEqual(1);
            expect(status.objectsWithBrokenMetadata).toEqual(1);
            expect(findDuplicateSproxydKeys.skipVersion).toHaveBeenCalledTimes(1);
        });

        test('should return false for empty array locations', () => {
            const result = validateLocations('s3://bucket/empty-key', [], status, findDuplicateSproxydKeys);

            expect(result).toEqual(false);
            expect(status.objectsScanned).toEqual(1);
            expect(status.objectsWithBrokenMetadata).toEqual(1);
            expect(findDuplicateSproxydKeys.skipVersion).toHaveBeenCalledTimes(1);
        });

        test('should return false for non-array locations', () => {
            const result = validateLocations('s3://bucket/string-key', 'not-an-array', status, findDuplicateSproxydKeys);

            expect(result).toEqual(false);
            expect(status.objectsScanned).toEqual(1);
            expect(status.objectsWithBrokenMetadata).toEqual(1);
            expect(findDuplicateSproxydKeys.skipVersion).toHaveBeenCalledTimes(1);
        });

        test('should return false for object instead of array', () => {
            const result = validateLocations('s3://bucket/object-key', { key: 'sproxyd-key' }, status, findDuplicateSproxydKeys);

            expect(result).toEqual(false);
            expect(status.objectsScanned).toEqual(1);
            expect(status.objectsWithBrokenMetadata).toEqual(1);
            expect(findDuplicateSproxydKeys.skipVersion).toHaveBeenCalledTimes(1);
        });
    });
});

// Regression test for the bug where `logProgress` read `status.objectErrors`
// (typo — missing the 's') instead of `status.objectsErrors`. Because
// JSON.stringify drops undefined values, the `errors` field was silently
// missing from the summary log line even when non-404 sproxyd errors had
// been counted. A customer scan with ~27,000 HTTP 422s looked clean as a
// result. The test below sets the counter, calls logProgress, and checks
// the emitted summary contains the expected `errors` value.

const { createScanStatus, logProgress } = require('../../metadataScan');

describe('metadataScan — summary line emits errors count (S3UTILS-236)', () => {
    let ctx;

    beforeEach(() => {
        // Each test gets its own scan context with counters starting
        // from zero, and a mock logger to inspect what logProgress emits.
        ctx = {
            log: { info: jest.fn() },
            config: {},
            status: createScanStatus(),
        };
    });

    test('should emit errors count from status.objectsErrors when non-404 sproxyd errors have been counted', () => {
        // Use the customer's actual numbers from the bug report: 59,011
        // objects scanned, 27,152 sproxyd errors.
        ctx.status.objectsScanned = 59011;
        ctx.status.objectsErrors = 27152;

        logProgress(ctx, 'completed scan');

        expect(ctx.log.info).toHaveBeenCalledWith('completed scan', expect.objectContaining({
            scanned: 59011,
            errors: 27152,
        }));
    });

    test('should emit errors: 0 (key present, not undefined) when no errors have been counted', () => {
        // A clean scan should still emit `errors: 0` in the summary. If the
        // read were misspelled, the value would be undefined and the key
        // would vanish from the output entirely — so we first assert the
        // key is present, then assert the value is 0.
        ctx.status.objectsScanned = 100;
        ctx.status.objectsErrors = 0;

        logProgress(ctx, 'completed scan');

        const emitted = ctx.log.info.mock.calls[0][1];
        expect('errors' in emitted).toBe(true);
        expect(emitted.errors).toBe(0);
    });
});
