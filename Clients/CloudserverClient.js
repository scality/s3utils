const { 
    CloudserverClient,
    GetMetadataCommand,
    PutMetadataCommand,
} = require('@scality/cloudserverclient');
const { http: httpArsn } = require('httpagent');
const https = require('https');

class Client {
    constructor(endpoint, accessKey, secretKey, agent) {
        const isHttps = endpoint.startsWith('https:');
        let httpAgent = agent;
        
        if (!httpAgent) {
            if (isHttps) {
                httpAgent = new https.Agent({
                    keepAlive: true,
                    rejectUnauthorized: false,
                    timeout: 60000,                    
                });
            } else {
                httpAgent = new httpArsn.Agent({
                    keepAlive: true,
                    timeout: 60000,
                });
            }
        }

        const requestHandler = {
            connectionTimeout: 60000,
            requestTimeout: 60000,
        };
        if (isHttps) {
            requestHandler.httpsAgent = httpAgent;
        } else {
            requestHandler.httpAgent = httpAgent;
        }
        
        const config = {
            endpoint,
            credentials: {
                accessKeyId: accessKey,
                secretAccessKey: secretKey,
            },
            region: 'us-east-1',
            maxAttempts: 1,
            requestHandler,
        };

        this.client = new CloudserverClient(config);
    }

    getMetadata(params, callback) {
        const command = new GetMetadataCommand(params);
        this.client.send(command)
            .then(data => callback(null, data))
            .catch(err => callback(err));
    }

    putMetadata(params, callback) {
        const command = new PutMetadataCommand(params);
        this.client.send(command)
            .then(data => callback(null, data))
            .catch(err => callback(err));
    }
}

module.exports = Client;
