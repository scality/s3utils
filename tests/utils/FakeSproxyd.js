const http = require('http');

/**
 * Minimal fake sproxyd server, implementing what is needed to check
 * the existence of sproxyd keys:
 *
 * - GET /.conf returns the ring driver alias
 *
 * - HEAD /<alias>/<key> returns 200, unless the key is registered as
 *   missing (404) or as failing (500)
 */
class FakeSproxyd {
    constructor() {
        this.alias = 'arc';
        this.missingKeys = new Set();
        this.failingKeys = new Set();
        this.confStatus = 200;
        this.server = http.createServer((req, res) => this._handleRequest(req, res));
    }

    _handleRequest(req, res) {
        if (req.method === 'GET' && req.url === '/.conf') {
            res.writeHead(this.confStatus, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ 'ring_driver:0': { alias: this.alias } }));
        }
        const prefix = `/${this.alias}/`;
        if (req.method === 'HEAD' && req.url.startsWith(prefix)) {
            const key = req.url.slice(prefix.length);
            if (this.missingKeys.has(key)) {
                res.writeHead(404);
            } else if (this.failingKeys.has(key)) {
                res.writeHead(500);
            } else {
                res.writeHead(200);
            }
            return res.end();
        }
        res.writeHead(400);
        return res.end();
    }

    /**
     * Reset the fake behavior: all keys exist, and /.conf succeeds
     *
     * @return {undefined}
     */
    reset() {
        this.missingKeys.clear();
        this.failingKeys.clear();
        this.confStatus = 200;
    }

    /**
     * Start listening on a random port
     *
     * @return {Promise<string>} - resolves with the "host:port" the
     * server is listening on
     */
    start() {
        return new Promise(resolve => {
            this.server.listen(0, '127.0.0.1', () => {
                resolve(`127.0.0.1:${this.server.address().port}`);
            });
        });
    }

    /**
     * Stop the server
     *
     * @return {Promise<undefined>} - resolves when stopped
     */
    stop() {
        this.server.closeAllConnections();
        return new Promise(resolve => this.server.close(() => resolve()));
    }
}

module.exports = FakeSproxyd;
