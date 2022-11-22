#!/usr/bin/env node

const os = require('os');
const { jsutil } = require('arsenal');

const { startServer } = require('./server');
const log = require('./log');
const env = require('./env');

// Log the random gen key at startup if one is not set
if (process.env.SIDECAR_API_KEY) {
    log.info('using API key from environment');
} else {
    log.info('using random API key', { key: env.apiKey });
}


function setupSignalHandlers(cleanUpFunc) {
    ['SIGINT', 'SIGQUIT', 'SIGTERM'].forEach(signal => process.on(signal, () => {
        log.info('received signal', { signal });
        cleanUpFunc();
    }));

    process.on('uncaughtException', error => {
        log.error('uncaught exception', { error, stack: error.stack.split(os.EOL) });
        cleanUpFunc();
    });
}

startServer(server => {
    log.info(`server listening on ${env.host}:${env.port}`);

    if (env.tls.enabled) {
        log.info('tls enabled', { apiServer: true, vault: env.vaultTls, bucketd: env.bucketdTls });
    }

    const cleanup = jsutil.once(() => {
        log.info('server exiting');
        server.close();
    });

    setupSignalHandlers(cleanup);
});
