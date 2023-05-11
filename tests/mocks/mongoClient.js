module.exports = {
    client: {
        isConnected: jest.fn(),
    },
    setup: jest.fn(),
    close: jest.fn(),
    _getIsTransient: jest.fn(),
    getObjectMDStats: jest.fn(),
    getBucketInfos: jest.fn(),
    updateStorageConsumptionMetrics: jest.fn(),
    getUsersBucketCreationDate: jest.fn(),
};
