module.exports = {
    client: {
        isConnected: jest.fn(),
    },
    setup: jest.fn(),
    close: jest.fn(),
    getObjectMDStats: jest.fn(),
    isLocationTransient: jest.fn(),
    getObject: jest.fn(),
    getBucketInfos: jest.fn(),
    updateStorageConsumptionMetrics: jest.fn(),
    getUsersBucketCreationDate: jest.fn(),
};
