const utils = require('../../../compareBuckets/utils');

const listBucketMasterKeys = jest.spyOn(utils, 'listBucketMasterKeys');

const {
    compareBuckets,
    compareObjectsReport,
} = require('../../../compareBuckets/compareBuckets');
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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

        compareBuckets(params, log, err => {
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
    const mismatchedContentLength1 = JSON.stringify({
        'versionId': '1',
        'content-length': 100,
        'content-md5': 'aaa',
    });

    const mismatchedContentLength2 = JSON.stringify({
        'versionId': '1',
        'content-length': 200,
        'content-md5': 'bbb',
    });

    const mismatchedVersionId1 = JSON.stringify({
        'versionId': '1',
        'content-length': 100,
        'content-md5': 'aaa',
    });

    const mismatchedVersionId2 = JSON.stringify({
        'versionId': '2',
        'content-length': 100,
        'content-md5': 'aaa',
    });

    it('should return null if verbose is not set', () => {
        const report = compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: mismatchedContentLength1 },
            'destinationBucket',
            { key: 'key1', value: mismatchedContentLength2 },
            false
        );

        expect(report).toBeNull();
    });

    it('should return report if content-length does not match', () => {
        const report = compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: mismatchedContentLength1 },
            'destinationBucket',
            { key: 'key1', value: mismatchedContentLength2 },
            true
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
            error: [
                { msg: 'destination object size does not match source object' },
            ],
        });
    });

    it('should return report if version-id does not match', () => {
        const report = compareObjectsReport(
            'sourceBucket',
            { key: 'key1', value: mismatchedVersionId1 },
            'destinationBucket',
            { key: 'key1', value: mismatchedVersionId2 },
            true
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
            error: [
                { msg: 'destination object version-id does not match source object' },
            ],
        });
    });
});
