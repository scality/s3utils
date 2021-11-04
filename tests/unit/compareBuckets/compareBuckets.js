const utils = require('../../../compareBuckets/utils');

const listBucketMasterKeys = jest.spyOn(utils, 'listBucketMasterKeys');

const { compareBuckets } = require('../../../compareBuckets/compareBuckets');
const DummyLogger = require('../../mocks/DummyLogger');

const log = new DummyLogger();

describe('compareBuckets', () => {
    jest.setTimeout(10000);

    beforeEach(() => {
        jest.resetModules();
        listBucketMasterKeys.mockReset();
    });

    it('should complete successfully when listings are empty', done => {
        listBucketMasterKeys.mockImplementation((params, cb) => cb(null, false, '', []));

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            return done();
        });
    });

    it('should complete successfully with only source listings', done => {
        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, false, '3', [
                    { key: '1' },
                    { key: '2' },
                    { key: '3' },
                ]);
            }
            return cb(null, false, '', []);
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('3');
            expect(status.dstKeyMarker).toEqual('');
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(3);
            return done();
        });
    });

    it('should complete successfully with only destination listings', done => {
        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'dst') {
                return cb(null, false, '3', [
                    { key: '1' },
                    { key: '2' },
                    { key: '3' },
                ]);
            }
            return cb(null, false, '', []);
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('');
            expect(status.dstKeyMarker).toEqual('3');
            expect(status.missingInSrcCount).toEqual(3);
            expect(status.missingInDstCount).toEqual(0);
            return done();
        });
    });

    it('should succesufully perform compare (single listing call)', done => {
        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, false, '3', [
                    { key: '1' },
                    { key: '2' },
                    { key: '3' },
                ]);
            }
            return cb(null, false, '6', [
                { key: '4' },
                { key: '5' },
                { key: '6' },
            ]);
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('3');
            expect(status.dstKeyMarker).toEqual('6');
            expect(status.missingInSrcCount).toEqual(3);
            expect(status.missingInDstCount).toEqual(3);
            return done();
        });
    });

    it('should successfully perform compare (with multiple listing calls)', done => {
        const srcStack = [
            [false, '3', [{ key: '3' }]],
            [true, '2', [{ key: '2' }]],
            [true, '1', [{ key: '1' }]],
        ];

        const dstStack = [
            [false, '6', [{ key: '6' }]],
            [true, '5', [{ key: '5' }]],
            [true, '4', [{ key: '4' }]],
        ];

        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, ...srcStack.pop());
            }
            return cb(null, ...dstStack.pop());
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('3');
            expect(status.dstKeyMarker).toEqual('6');
            expect(status.missingInSrcCount).toEqual(3);
            expect(status.missingInDstCount).toEqual(3);
            return done();
        });
    });

    it('should successfully perform compare (with variable-sized and matching lists)', done => {
        const srcStack = [
            [false, '9', [{ key: '8' }, { key: '9' }]],
            [true, '7', [{ key: '6' }, { key: '7' }]],
            [true, '5', [{ key: '3' }, { key: '4' }, { key: '5' }]],
            [true, '2', [{ key: '1' }, { key: '2' }]],
            [true, '0', [{ key: '0' }]],
        ];

        const dstStack = [
            [false, '9', [{ key: '8' }, { key: '9' }]],
            [true, '7', [{ key: '6' }, { key: '7' }]],
            [true, '5', [{ key: '5' }]],
            [true, '4', [{ key: '3' }, { key: '4' }]],
            [true, '2', [{ key: '0' }, { key: '1' }, { key: '2' }]],
        ];

        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, ...srcStack.pop());
            }
            return cb(null, ...dstStack.pop());
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('9');
            expect(status.dstKeyMarker).toEqual('9');
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(0);
            return done();
        });
    });

    it('should successfully perform compare (with variable-sized and mismatched lists)', done => {
        const srcStack = [
            [false, '9', [{ key: '8' }, { key: '9' }]],
            [true, '7', [{ key: '6' }, { key: '7' }]],
            [true, '5', [{ key: '3' }, { key: '4' }, { key: '5' }]],
            [true, '2', [{ key: '1' }, { key: '2' }]],
            [true, '0', [{ key: '0' }]],
        ];

        const dstStack = [
            [false, '19', [{ key: '18' }, { key: '19' }]],
            [true, '17', [{ key: '16' }, { key: '17' }]],
            [true, '15', [{ key: '15' }]],
            [true, '14', [{ key: '13' }, { key: '14' }]],
            [true, '12', [{ key: '10' }, { key: '11' }, { key: '12' }]],
        ];

        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, ...srcStack.pop());
            }
            return cb(null, ...dstStack.pop());
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('9');
            expect(status.dstKeyMarker).toEqual('19');
            expect(status.missingInSrcCount).toEqual(10);
            expect(status.missingInDstCount).toEqual(10);
            return done();
        });
    });

    it('should successfully perform compare (with variable-sized and partially matching lists)', done => {
        const srcStack = [
            [false, '9', [{ key: '8' }, { key: '9' }]],
            [true, '7', [{ key: '6' }, { key: '7' }]],
            [true, '5', [{ key: '3' }, { key: '4' }, { key: '5' }]],
            [true, '2', [{ key: '1' }, { key: '2' }]],
            [true, '0', [{ key: '0' }]],
        ];

        const dstStack = [
            [false, '9', [{ key: '8' }, { key: '9' }]],
            [true, '6', [{ key: '6' }]],
            [true, '1', [{ key: '1' }, { key: '2' }]],
            [true, '0', [{ key: '0' }]],
        ];

        listBucketMasterKeys.mockImplementation((params, cb) => {
            if (params.bucket === 'src') {
                return cb(null, ...srcStack.pop());
            }
            return cb(null, ...dstStack.pop());
        });

        const status = {
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

        compareBuckets(params, log, err => {
            expect(err).toBeNull();
            expect(status.srcKeyMarker).toEqual('9');
            expect(status.dstKeyMarker).toEqual('9');
            expect(status.missingInSrcCount).toEqual(0);
            expect(status.missingInDstCount).toEqual(4);
            return done();
        });
    });
});
