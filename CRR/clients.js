const { S3Client } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const { ConfiguredRetryStrategy } = require('@smithy/util-retry');
const http = require('http');
const https = require('https');
const BackbeatClient = require('../BackbeatClient');

const AWS_SDK_REQUEST_DELAY_MS = 30;
const AWS_SDK_REQUEST_RETRIES = 100;

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
    const awsv3Config = {
        region: 'us-east-1',
        credentials: {
            accessKeyId: accessKey,
            secretAccessKey: secretKey,
        },
        endpoint,
        forcePathStyle: true,
        requestHandler: new NodeHttpHandler({
            httpAgent: new http.Agent({ keepAlive: true }),
            httpsAgent: new https.Agent({ 
                keepAlive: true,
                // With SDK v2, SSL was disabled. Here we added this
                // to be able to run the script locally on a lab for testing,
                // so https will work but there will be no security enforced
                rejectUnauthorized: false
            }),
            requestTimeout: 60000,
        }),
        retryStrategy: new ConfiguredRetryStrategy(
            AWS_SDK_REQUEST_RETRIES, // maxAttempts
            attempt => {
                log.error('aws sdk request error', { retryCount: attempt });
                // The delay is not truly exponential; it resets to the minimum after every 10 calls,
                // with a maximum delay of 15 seconds.
                return AWS_SDK_REQUEST_DELAY_MS * (2 ** (attempt % 10));
            }
        )
    };

    const awsv2Config = {
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
            timeout: 60000,
            agent: new http.Agent({ keepAlive: true }),
        },
    };

    return {
        s3: new S3Client(awsv3Config),
        bb: new BackbeatClient(awsv2Config),
    };
}

module.exports = { setupClients };
