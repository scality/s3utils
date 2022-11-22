const vaultclient = require('vaultclient');

const env = require('./env');
const utils = require('./utils');

const params = [env.vault.host, env.vault.port, env.vaultTls];
if (env.vaultTls) {
    params.push(env.tls.certs.key, env.tls.certs.cert, env.tls.certs.ca);
}

const vault = new vaultclient.Client(...params);

function _getAccountIdForCanonicalId(canonicalId, log, cb) {
    vault.getAccounts(
        null,
        null,
        [canonicalId],
        { reqUid: log.getSerializedUids(), logger: log },
        (error, res) => {
            if (error) {
                log.error('error during request to vault', { error });
                cb(error);
                return;
            }
            cb(null, res[0]);
        },
    );
}

const getAccountIdForCanonicalId = utils.retryable(_getAccountIdForCanonicalId);

module.exports = { getAccountIdForCanonicalId };
