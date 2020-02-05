module.exports = {
    client: {
        isConnected: jest.fn(),
    },
    setup: jest.fn(),
    close: jest.fn(),
    _getIsTransient: jest.fn(),
    getObjectMDStats: jest.fn(),
    getBucketInfos: jest.fn(),
    updateCountItems: jest.fn(),
};
