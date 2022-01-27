const assert = require('assert');
const symDiff = require('../../../compareBuckets/ArrayUtils').symDiff;

describe('ArrayUtils', () => {
    it('shall find symmetric difference', () => {
        const arr1 = [2, 4, 5, 7, 8, 10, 12, 15];
        const arr2 = [5, 8, 11, 12, 14, 15];
        const arr3 = [];
        symDiff(arr1, arr2, arr1, arr2, x => arr3.push(x));
        assert.deepEqual(arr3, [2, 4, 7, 10, 11, 14]);
    });
});
