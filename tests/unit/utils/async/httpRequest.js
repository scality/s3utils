const http = require('http');
const httpRequest = require('../../../../utils/async/httpRequest');

// Spin up a minimal HTTP server, run the callback, then close it.
async function withServer(handler, fn) {
    const server = http.createServer(handler);
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address();
    try {
        return await fn(port);
    } finally {
        await new Promise(resolve => server.close(resolve));
    }
}

describe('httpRequest', () => {
    test('GET returns status code and buffered body', () =>
        withServer((req, res) => {
            res.writeHead(200);
            res.end(`${req.method} hello world`);
        }, async port => {
            const res = await httpRequest('GET', `http://127.0.0.1:${port}/`);
            expect(res.statusCode).toBe(200);
            expect(res.body).toBe('GET hello world');
        }));

    test('GET passes query string and path to the server', () =>
        withServer((req, res) => {
            res.writeHead(200);
            res.end(`${req.method} ${req.url}`);
        }, async port => {
            const res = await httpRequest('GET', `http://127.0.0.1:${port}/foo?bar=baz`);
            expect(res.statusCode).toBe(200);
            expect(res.body).toBe('GET /foo?bar=baz');
        }));

    test('GET resolves on 404 (non-5xx is not an error)', () =>
        withServer((req, res) => {
            res.writeHead(404);
            res.end(`${req.method} not found`);
        }, async port => {
            const res = await httpRequest('GET', `http://127.0.0.1:${port}/`);
            expect(res.statusCode).toBe(404);
            expect(res.body).toBe('GET not found');
        }));

    test('DELETE sends the correct method to the server', () =>
        withServer((req, res) => {
            res.writeHead(200);
            res.end(`${req.method} ok`);
        }, async port => {
            const res = await httpRequest('DELETE', `http://127.0.0.1:${port}/resource`);
            expect(res.statusCode).toBe(200);
            expect(res.body).toBe('DELETE ok');
        }));

    test('GET rejects on 5xx response (no retries)', () => {
        let calls = 0;
        withServer((req, res) => {
            calls += 1;
            res.writeHead(500);
            res.end(`${req.method} oops`);
        }, async port => {
            await expect(
                httpRequest('GET', `http://127.0.0.1:${port}/`)
            ).rejects.toThrow('500');
            expect(calls).toBe(1);
        });
    });

    test('GET retries on 5xx when retryParams provided', () => {
        let calls = 0;
        return withServer((req, res) => {
            calls += 1;
            if (calls < 3) {
                res.writeHead(500);
                res.end(`${req.method} not yet`);
            } else {
                res.writeHead(200);
                res.end(`${req.method} ok`);
            }
        }, async port => {
            const res = await httpRequest(
                'GET',
                `http://127.0.0.1:${port}/`,
                { times: 5, interval: 0 },
            );
            expect(res.statusCode).toBe(200);
            expect(res.body).toBe('GET ok');
            expect(calls).toBe(3);
        });
    });

    test('GET gives up after retries are exhausted', () => {
        let calls = 0;
        return withServer((req, res) => {
            calls += 1;
            res.writeHead(500);
            res.end(`${req.method} oops`);
        }, async port => {
            await expect(
                httpRequest('GET', `http://127.0.0.1:${port}/`, { times: 3, interval: 0 })
            ).rejects.toThrow('500');
            expect(calls).toBe(3);
        });
    });
});
