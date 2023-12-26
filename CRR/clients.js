const AWS = require('aws-sdk');
const http = require('http');
const BackbeatClient = require('../BackbeatClient');

const AWS_SDK_REQUEST_DELAY_MS = 30;
const AWS_SDK_REQUEST_RETRIES = 100;
const LOG_PROGRESS_INTERVAL_MS = 10000;

/**
 * Sets up and configures AWS S3 and Backbeat clients.
 *
 * This function initializes and configures clients for S3 and Backbeat services,
 * using provided access credentials and endpoint configurations. It includes a custom
 * backoff strategy for AWS SDK requests to handle retries in case of errors.
 * The clients are set with specific options, such as maximum retries and custom
 * backoff strategies for S3 requests.
 *
 * @param {Object} config - The configuration object for the clients.
 * @param {string} config.accessKey - The access key for AWS services.
 * @param {string} config.secretKey - The secret key for AWS services.
 * @param {string} config.endpoint - The endpoint URL for the AWS services.
 * @param {Function} log - The logging function for error logging.
 * @returns {Object} An object containing initialized S3 and Backbeat clients.
 */
function setupClients({
    accessKey,
    secretKey,
    endpoint,
}, log) {
    const awsConfig = {
        accessKeyId: accessKey,
        secretAccessKey: secretKey,
        endpoint,
        region: 'us-east-1',
        sslEnabled: false,
        s3ForcePathStyle: true,
        apiVersions: { s3: '2006-03-01' },
        signatureVersion: 'v4',
        signatureCache: false,
        httpOptions: {
            timeout: 0,
            agent: new http.Agent({ keepAlive: true }),
        },
    };

    /**
     * Custom backoff strategy for AWS SDK requests.
     * @param {number} retryCount - The current retry attempt.
     * @param {Error} error - The error that caused the retry.
     * @returns {number} The delay in milliseconds before the next retry.
     */
    function customBackoffStrategy(retryCount, error) {
        this.log.error('aws sdk request error', { error, retryCount });
        // The delay is not truly exponential; it resets to the minimum after every 10 calls,
        // with a maximum delay of 15 seconds.
        return AWS_SDK_REQUEST_DELAY_MS * (2 ** (retryCount % 10));
    }

    // Specific options for S3 requests
    const s3SpecificOptions = {
        maxRetries: AWS_SDK_REQUEST_RETRIES,
        customBackoff: customBackoffStrategy,
    };

    // Create an S3 client instance
    const s3 = new AWS.S3({ ...awsConfig, ...s3SpecificOptions });

    // Create a BackbeatClient instance
    const bb = new BackbeatClient(awsConfig);

    return { s3, bb };
}

module.exports = { setupClients };
