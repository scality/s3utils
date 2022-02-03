const getObjectURL = require('../../VerifyBucketSproxydKeys/getObjectURL');
const getBucketdURL = require('../../VerifyBucketSproxydKeys/getBucketdURL');
const FindDuplicateSproxydKeys = require('../../VerifyBucketSproxydKeys/FindDuplicateSproxydKeys');

describe('verifyBucketSproxydKeys', () => {
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
});
