const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const { envNamespace, truthy } = require('./constants');

function _splitTrim(char, text) {
    return text.split(char).map(v => v.trim());
}

function _parseServer(text) {
    assert.notStrictEqual(text.indexOf(':'), -1);
    const [host, port] = _splitTrim(':', text);
    return {
        host,
        port: Number.parseInt(port, 10),
    };
}

function _parseNode(text) {
    assert.notStrictEqual(text.indexOf('@'), -1);
    const [nodeId, hostname] = _splitTrim('@', text);
    return {
        nodeId,
        ..._parseServer(hostname),
    };
}

const typeCasts = {
    bool: val => truthy.has(val.toLowerCase()),
    int: val => parseInt(val, 10),
    float: val => parseFloat(val),
    server: _parseServer,
    node: _parseNode,
};

function loadFromEnv(key, defaultValue, type) {
    const envKey = `${envNamespace}_${key}`;
    const value = process.env[envKey];
    if (value !== undefined) {
        if (type !== undefined) {
            return type(value);
        }
        return value;
    }
    return defaultValue;
}

function loadCertificates(tlsConfig) {
    const { key, cert, ca } = tlsConfig;

    if (!key && !cert) {
        return { enabled: false };
    }

    if (!(key && cert)) {
        throw new Error('You must specify a path for both a key and certificate');
    }

    const basePath = path.join(__dirname, '../');
    const keyPath = path.isAbsolute(key) ? key : path.join(basePath, key);
    const certPath = path.isAbsolute(cert) ? cert : path.join(basePath, cert);

    const certs = {
        cert: fs.readFileSync(certPath, { encoding: 'ascii' }),
        key: fs.readFileSync(keyPath, { encoding: 'ascii' }),
    };

    if (ca) {
        const caPath = path.isAbsolute(ca) ? ca : path.join(basePath, ca);
        certs.ca = fs.readFileSync(caPath, { encoding: 'ascii' });
    }

    return { enabled: true, certs };
}

const defaults = {
    // Generate a random API per start
    apiKey: crypto.randomBytes(24).toString('hex'),
    logLevel: 'info',
    host: '0.0.0.0',
    port: 24742,
    scaleFactor: 1.0,
    concurrencyLimit: 10,
    retryLimit: 5,
    bucketd: 'localhost:9000',
    bucketdTls: false,
    warp10: {
        readToken: 'readTokenStatic',
        node: {
            host: '127.0.0.1',
            port: 4802,
            nodeId: 'single_node',
        },
    },
    vaultTls: false,
    vault: {
        host: 'localhost',
        port: 8500,
    },
};

module.exports = {
    // Fallback to the random key if one is not specified
    apiKey: loadFromEnv('API_KEY', defaults.apiKey),
    host: loadFromEnv('HOST', defaults.host),
    port: loadFromEnv('PORT', defaults.port, typeCasts.int),
    logLevel: loadFromEnv('LOG_LEVEL', defaults.logLevel),
    scaleFactor: loadFromEnv('SCALE_FACTOR', defaults.scaleFactor, typeCasts.float),
    tls: loadCertificates({
        cert: loadFromEnv('TLS_CERT_PATH', null),
        key: loadFromEnv('TLS_KEY_PATH', null),
        ca: loadFromEnv('TLS_CA_PATH', null),
    }),
    concurrencyLimit: loadFromEnv('CONCURRENCY_LIMIT', defaults.concurrencyLimit, typeCasts.int),
    retryLimit: loadFromEnv('RETRY_LIMIT', defaults.retryLimit, typeCasts.int),
    bucketd: loadFromEnv('BUCKETD_BOOTSTRAP', defaults.bucketd),
    bucketdTls: loadFromEnv('BUCKETD_ENABLE_TLS', defaults.bucketdTls, typeCasts.bool),
    vault: loadFromEnv('VAULT_ENDPOINT', defaults.vault, typeCasts.server),
    vaultTls: loadFromEnv('VAULT_ENABLE_TLS', defaults.vaultTls, typeCasts.bool),
    warp10: {
        readToken: loadFromEnv('WARP10_READ_TOKEN', defaults.warp10.readToken),
        ...loadFromEnv('WARP10_NODE', defaults.warp10.node, typeCasts.node),
    },
};
