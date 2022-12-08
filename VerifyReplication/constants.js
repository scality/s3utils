module.exports = {
    defaults: {
        LOG_PROGRESS_INTERVAL: 10,
        LISTING_WORKERS: 10,
        LISTING_LIMIT: 1000,
        DESTINATION_MD_REQUEST_WORKERS: 100,
        BUCKET_MATCH: 0,
        COMPARE_OBJECT_SIZE: 0,
        COMPARE_OBJECT_ALL_VERSIONS: 0,
        AWS_SDK_REQUEST_RETRIES: 100,
        AWS_SDK_REQUEST_INITIAL_DELAY_MS: 30,
        AWS_SDK_REQUEST_TIMEOUT: 0, // 0 is infinity, aws default is 2 minutes
        AWS_SDK_REQUEST_MAX_BACKOFF_LIMIT_MS: 60000, // 60 seconds
        AWS_REGION: 'us-east-1',
        STORAGE_TYPE: 'aws_s3',
        SUPPORTED_STORAGE_TYPES: ['aws_s3'],
        SHOW_CLIENT_LOGS_IF_AVAILABLE: 0,
        DELIMITER: '/',
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
