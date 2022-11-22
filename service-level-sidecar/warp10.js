const { Warp10 } = require('@senx/warp10');

const log = require('./log');
const env = require('./env');

class Warp10Client {
    constructor(config) {
        this._writeToken = (config && config.writeToken) || 'writeTokenStatic';
        this._readToken = (config && config.readToken) || 'readTokenStatic';
        this.nodeId = (config && config.nodeId);
        const proto = (config && config.tls) ? 'https' : 'http';
        this._requestTimeout = (config && config.requestTimeout) || 30000;
        this._connectTimeout = (config && config.connectTimeout) || 30000;
        const host = (config && config.host) || 'localhost';
        const port = (config && config.port) || 4802;
        this._client = new Warp10(`${proto}://${host}:${port}`, this._requestTimeout, this._connectTimeout);
    }

    _buildScriptEntry(params) {
        const authInfo = {
            read: this._readToken,
            write: this._writeToken,
        };
        return `'${JSON.stringify(authInfo)}' '${JSON.stringify(params)}'`;
    }

    _buildExecPayload(params) {
        const payload = [this._buildScriptEntry(params.params)];
        if (params.macro) {
            payload.push(`@${params.macro}\n`);
        }
        if (params.script) {
            payload.push(params.script);
        }
        return payload.join('\n');
    }

    async exec(params) {
        const payload = this._buildExecPayload(params);
        const resp = await this._client.exec(payload);
        log.debug('warpscript executed', { stats: resp.meta });
        return resp;
    }
}


module.exports = new Warp10Client(env.warp10);
