const assert = require('assert');
const utils = require('../../../compareBuckets/utils');

const listBucketMasterKeys = jest.spyOn(utils, 'listBucketMasterKeys');

const DiffManager = require('../../../compareBuckets/compareBuckets');
const DummyLogger = require('../../mocks/DummyLogger');

const log = new DummyLogger();

const newEntry = key => ({ key, value: '{}' });

describe('compareBuckets', () => {
    let status;
    let params;
    let dstStack;
    let srcStack;

    beforeEach(() => {
        jest.resetModules();
        listBucketMasterKeys.mockReset();

        status = {
            srcProcessedCount: 0,
            dstProcessedCount: 0,
            missingInSrcCount: 0,
            missingInDstCount: 0,
            dstBucketInProgress: null,
            srcBucketInProgress: null,
            srcKeyMarker: '',
            dstKeyMarker: '',
        };

        params = {
            bucketdSrcParams: {
                bucket: 'src',
                marker: '',
                hostPort: '',
            },
            bucketdDstParams: {
                bucket: 'dst',
                marker: '',
                hostPort: '',
            },
            statusObj: status,
        };

        dstStack = [];
        srcStack = [];

        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, ...srcStack.pop());
            }
            return cb(null, ...dstStack.pop());
        });
    });

    it('should complete successfully when listings are empty', done => {
        dstStack.push([false, '', []]);
        srcStack.push([false, '', []]);

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('');
            expect(status.dstKeyMarker).toEqual('');
            expect(status.srcProcessedCount).toEqual(0);
            expect(status.dstProcessedCount).toEqual(0);
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(0);
            return done();
        });
    });

    it('should complete successfully with only source listings', done => {
        srcStack.push(
            [false, '3', [newEntry('1'), newEntry('2'), newEntry('3')]]
        );
        dstStack.push([false, '', []]);

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('3');
            expect(status.dstKeyMarker).toEqual('');
            expect(status.srcProcessedCount).toEqual(3);
            expect(status.dstProcessedCount).toEqual(0);
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(3);
            return done();
        });
    });

    it('should complete successfully with only destination listings', done => {
        srcStack.push([false, '', []]);
        dstStack.push(
            [false, '3', [newEntry('1'), newEntry('2'), newEntry('3')]]
        );

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('');
            expect(status.dstKeyMarker).toEqual('3');
            expect(status.srcProcessedCount).toEqual(0);
            expect(status.dstProcessedCount).toEqual(3);
            expect(status.missingInSrcCount).toEqual(3);
            expect(status.missingInDstCount).toEqual(0);
            return done();
        });
    });

    it('should successfully perform compare (single listing call)', done => {
        srcStack.push(
            [false, '3', [newEntry('1'), newEntry('2'), newEntry('3')]]
        );
        dstStack.push(
            [false, '6', [newEntry('4'), newEntry('5'), newEntry('6')]]
        );

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('3');
            expect(status.dstKeyMarker).toEqual('6');
            expect(status.srcProcessedCount).toEqual(3);
            expect(status.dstProcessedCount).toEqual(3);
            expect(status.missingInSrcCount).toEqual(3);
            expect(status.missingInDstCount).toEqual(3);
            return done();
        });
    });

    it('should successfully perform compare (with multiple listing calls)', done => {
        srcStack.push(
            [false, '3', [newEntry('3')]],
            [true, '2', [newEntry('2')]],
            [true, '1', [newEntry('1')]]
        );

        dstStack.push(
            [false, '6', [newEntry('6')]],
            [true, '5', [newEntry('5')]],
            [true, '4', [newEntry('4')]]
        );

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('3');
            expect(status.dstKeyMarker).toEqual('6');
            expect(status.srcProcessedCount).toEqual(3);
            expect(status.dstProcessedCount).toEqual(3);
            expect(status.missingInSrcCount).toEqual(3);
            expect(status.missingInDstCount).toEqual(3);
            return done();
        });
    });

    it('should successfully perform compare (with variable-sized and matching lists)', done => {
        srcStack.push(
            [false, '9', [newEntry('8'), newEntry('9')]],
            [true, '7', [newEntry('6'), newEntry('7')]],
            [true, '5', [newEntry('3'), newEntry('4'), newEntry('5')]],
            [true, '2', [newEntry('1'), newEntry('2')]],
            [true, '0', [newEntry('0')]]
        );

        dstStack.push(
            [false, '9', [newEntry('8'), newEntry('9')]],
            [true, '7', [newEntry('6'), newEntry('7')]],
            [true, '5', [newEntry('5')]],
            [true, '4', [newEntry('3'), newEntry('4')]],
            [true, '2', [newEntry('0'), newEntry('1'), newEntry('2')]]
        );

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('9');
            expect(status.dstKeyMarker).toEqual('9');
            expect(status.srcProcessedCount).toEqual(10);
            expect(status.dstProcessedCount).toEqual(10);
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(0);
            return done();
        });
    });

    it('should successfully perform compare (with variable-sized and mismatched lists)', done => {
        srcStack.push(
            [false, '9', [newEntry('8'), newEntry('9')]],
            [true, '7', [newEntry('6'), newEntry('7')]],
            [true, '5', [newEntry('3'), newEntry('4'), newEntry('5')]],
            [true, '2', [newEntry('1'), newEntry('2')]],
            [true, '0', [newEntry('0')]]
        );

        dstStack.push(
            [false, '19', [newEntry('18'), newEntry('19')]],
            [true, '17', [newEntry('16'), newEntry('17')]],
            [true, '15', [newEntry('15')]],
            [true, '14', [newEntry('13'), newEntry('14')]],
            [true, '12', [newEntry('10'), newEntry('11'), newEntry('12')]]
        );

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('9');
            expect(status.dstKeyMarker).toEqual('19');
            expect(status.srcProcessedCount).toEqual(10);
            expect(status.dstProcessedCount).toEqual(10);
            expect(status.missingInSrcCount).toEqual(10);
            expect(status.missingInDstCount).toEqual(10);
            return done();
        });
    });

    it('should successfully perform compare (with variable-sized and partially matching lists)', done => {
        srcStack.push(
            [false, '9', [newEntry('8'), newEntry('9')]],
            [true, '7', [newEntry('6'), newEntry('7')]],
            [true, '5', [newEntry('3'), newEntry('4'), newEntry('5')]],
            [true, '2', [newEntry('1'), newEntry('2')]],
            [true, '0', [newEntry('0')]]
        );

        dstStack.push(
            [false, '9', [newEntry('8'), newEntry('9')]],
            [true, '6', [newEntry('6')]],
            [true, '1', [newEntry('1'), newEntry('2')]],
            [true, '0', [newEntry('0')]]
        );

        const diffMgr = new DiffManager(params, log);
        diffMgr.compareBuckets(err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('9');
            expect(status.dstKeyMarker).toEqual('9');
            expect(status.srcProcessedCount).toEqual(10);
            expect(status.dstProcessedCount).toEqual(6);
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(4);
            return done();
        });
    });
});

describe('commpareObjectsReports', () => {
    const objectMDVersion1Size200 = JSON.stringify({
        'versionId': '1',
        'content-length': 200,
        'content-md5': 'bbb',
    });

    const objectMDVersion1Size100 = JSON.stringify({
        'versionId': '1',
        'content-length': 100,
        'content-md5': 'aaa',
    });

    const objectMDVersion2Size100 = JSON.stringify({
        'versionId': '2',
        'content-length': 100,
        'content-md5': 'aaa',
    });

    it('should return null if compare options are not set', () => {
        const diffMgr = new DiffManager({ statusObj: {} }, log);
        const report = diffMgr.compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            'destinationBucket',
            { key: 'key1', value: objectMDVersion1Size200 },
            {}
        );

        expect(report).toBeNull();
    });

    it('should return report if content-length does match', () => {
        const diffMgr = new DiffManager({ statusObj: {} }, log);
        const report = diffMgr.compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            'destinationBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            { compareObjectSize: true }
        );

        expect(report).toEqual({
            sourceObject: {
                bucket: 'sourceBucket',
                key: 'key1',
                versionId: '1',
                size: 100,
                contentMD5: 'aaa',
            },
            destinationObject: {
                bucket: 'destinationBucket',
                key: 'key1',
                versionId: '1',
                size: 100,
                contentMD5: 'aaa',
            },
        });
    });

    it('should return report if content-length does not match', () => {
        const diffMgr = new DiffManager({ statusObj: {} }, log);
        const report = diffMgr.compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            'destinationBucket',
            { key: 'key1', value: objectMDVersion1Size200 },
            { compareObjectSize: true }
        );

        expect(report).toEqual({
            sourceObject: {
                bucket: 'sourceBucket',
                key: 'key1',
                versionId: '1',
                size: 100,
                contentMD5: 'aaa',
            },
            destinationObject: {
                bucket: 'destinationBucket',
                key: 'key1',
                versionId: '1',
                size: 200,
                contentMD5: 'bbb',
            },
            error: 'destination object size does not match source object',
        });
    });

    it('should return report if version-id does not match', () => {
        const diffMgr = new DiffManager({ statusObj: {} }, log);
        const report = diffMgr.compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            'destinationBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            { compareVersionId: true }
        );

        expect(report).toEqual({
            sourceObject: {
                bucket: 'sourceBucket',
                key: 'key1',
                versionId: '1',
                size: 100,
                contentMD5: 'aaa',
            },
            destinationObject: {
                bucket: 'destinationBucket',
                key: 'key1',
                versionId: '1',
                size: 100,
                contentMD5: 'aaa',
            },
        });
    });

    it('should return report if version-id does not match', () => {
        const diffMgr = new DiffManager({ statusObj: {} }, log);
        const report = diffMgr.compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: objectMDVersion1Size100 },
            'destinationBucket',
            { key: 'key1', value: objectMDVersion2Size100 },
            { compareVersionId: true }
        );

        expect(report).toEqual({
            sourceObject: {
                bucket: 'sourceBucket',
                key: 'key1',
                versionId: '1',
                size: 100,
                contentMD5: 'aaa',
            },
            destinationObject: {
                bucket: 'destinationBucket',
                key: 'key1',
                versionId: '2',
                size: 100,
                contentMD5: 'aaa',
            },
            error: 'destination object version-id does not match source object',
        });
    });
});

describe('genericUpdate', () => {
    const status = {
        srcProcessedCount: 0,
        dstProcessedCount: 0,
        missingInSrcCount: 0,
        missingInDstCount: 0,
        dstBucketInProgress: null,
        srcBucketInProgress: null,
        srcKeyMarker: '',
        dstKeyMarker: '',
    };

    const params = {
        bucketdSrcParams: {
            bucket: 'src',
            marker: '',
            hostPort: '',
        },
        bucketdDstParams: {
            bucket: 'dst',
            marker: '',
            hostPort: '',
        },
        statusObj: status,
    };

    // for add events
    const obj1 = {
        key: 'obj1',
        value: {
            versionId: 'a4f22157b6ad554d0e35fd5a7bb2c560',
            size: 46573,
            md5: '2df22eba370784b0af011f06a633ce8a',
        },
    };

    // for delete events
    const _obj1 = {
        key: 'obj1',
        value: {
            versionId: 'a4f22157b6ad554d0e35fd5a7bb2c560',
            // size and md5 are note available in bucketd
        },
    };

    it('a new object on source shall be added on target', () => {
        const addQueue = [];
        const deleteQueue = [];
        const diffMgr = new DiffManager(params, log);
        params.statusObj.missingInDstCount = 0;
        params.statusObj.missingInSrcCount = 0;
        addQueue.push(obj1);
        diffMgr.genericUpdate(
            diffMgr.IdxSrc,
            diffMgr.IdxDst,
            addQueue,
            deleteQueue);
        assert(diffMgr.state[diffMgr.IdxSrc].getSize() === 0);
        assert(diffMgr.state[diffMgr.IdxDst].getSize() === 1);
        assert(diffMgr.state[diffMgr.IdxDst].get(obj1.key).mismatch === diffMgr.MismatchNoExist);
        assert(diffMgr.state[diffMgr.IdxDst].get(obj1.key).versionId === obj1.value.versionId);
        assert(diffMgr.params.statusObj.missingInSrcCount === 0);
        assert(diffMgr.params.statusObj.missingInDstCount === 1);
    });

    it('a missing object on target shall be removed if received identical in target', () => {
        const addQueue = [];
        const deleteQueue = [];
        const diffMgr = new DiffManager(params, log);
        diffMgr._add(
            diffMgr.IdxDst,
            obj1,
            diffMgr.MismatchNoExist);
        params.statusObj.missingInDstCount = 1;
        params.statusObj.missingInSrcCount = 0;
        addQueue.push(obj1);
        diffMgr.genericUpdate(
            diffMgr.IdxDst,
            diffMgr.IdxSrc,
            addQueue,
            deleteQueue);
        assert(diffMgr.state[diffMgr.IdxSrc].getSize() === 0);
        assert(diffMgr.state[diffMgr.IdxDst].getSize() === 0);
        assert(diffMgr.params.statusObj.missingInSrcCount === 0);
        assert(diffMgr.params.statusObj.missingInDstCount === 0);
    });

    it('a delete in target oplog add it to source', () => {
        const addQueue = [];
        const deleteQueue = [];
        const diffMgr = new DiffManager(params, log);
        params.statusObj.missingInDstCount = 0;
        params.statusObj.missingInSrcCount = 0;
        deleteQueue.push(_obj1);
        diffMgr.genericUpdate(
            diffMgr.IdxDst,
            diffMgr.IdxSrc,
            addQueue,
            deleteQueue);
        assert(diffMgr.state[diffMgr.IdxSrc].getSize() === 0);
        assert(diffMgr.state[diffMgr.IdxDst].getSize() === 1);
        assert(diffMgr.state[diffMgr.IdxDst].get(_obj1.key).mismatch === diffMgr.MismatchNoExist);
        assert(diffMgr.state[diffMgr.IdxDst].get(_obj1.key).versionId === _obj1.value.versionId);
        assert(diffMgr.params.statusObj.missingInSrcCount === 0);
        assert(diffMgr.params.statusObj.missingInDstCount === 1);
    });

    it('a missing object on source shall be removed if del received in target oplog', () => {
        const addQueue = [];
        const deleteQueue = [];
        const diffMgr = new DiffManager(params, log);
        diffMgr._add(
            diffMgr.IdxSrc,
            obj1,
            diffMgr.MismatchNoExist);
        params.statusObj.missingInDstCount = 0;
        params.statusObj.missingInSrcCount = 1;
        deleteQueue.push(_obj1);
        diffMgr.genericUpdate(
            diffMgr.IdxDst,
            diffMgr.IdxSrc,
            addQueue,
            deleteQueue);
        assert(diffMgr.state[diffMgr.IdxSrc].getSize() === 0);
        assert(diffMgr.state[diffMgr.IdxDst].getSize() === 0);
        assert(diffMgr.params.statusObj.missingInSrcCount === 0);
        assert(diffMgr.params.statusObj.missingInDstCount === 0);
    });
});
