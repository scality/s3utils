module.exports = {
    defaults: {
        LOG_PROGRESS_INTERVAL: 10,
        LISTING_WORKERS: 10,
        LISTING_LIMIT: 1000,
        DESTINATION_MD_REQUEST_WORKERS: 100,
        BUCKET_MATCH: 0,
        COMPARE_OBJECT_SIZE: 0,
        COMPARE_OBJECT_ALL_VERSIONS: 0,
        AWS_SDK_REQUEST_RETRIES: 3,
        AWS_SDK_REQUEST_INITIAL_DELAY_MS: 30,
        AWS_SDK_REQUEST_TIMEOUT: 0, // aws default is 2 minutes, 2 * 60 * 1000
        AWS_REGION: 'us-east-1',
        STORAGE_TYPE: 'aws_s3',
        SUPPORTED_STORAGE_TYPES: ['aws_s3'],
    },
    mandatoryVars: [
        'SRC_ENDPOINT',
        'SRC_BUCKET',
        'SRC_ACCESS_KEY',
        'SRC_SECRET_KEY',
        'DST_ACCESS_KEY',
        'DST_SECRET_KEY',
        'DST_BUCKET',
    ],
};
