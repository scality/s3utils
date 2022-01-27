const assert = require('assert');
const SortedSet = require('../../../compareBuckets/SortedSet');

describe('SortedSet', () => {
    it('basic', () => {
        const set = new SortedSet();
        set.set('foo', 'bar');
        assert(set.isSet('foo'));
        assert(!set.isSet('foo2'));
        assert(set.get('foo') === 'bar');
        set.set('foo', 'bar2');
        assert(set.get('foo') === 'bar2');
        set.del('foo');
        assert(!set.isSet('foo'));
    });
});
