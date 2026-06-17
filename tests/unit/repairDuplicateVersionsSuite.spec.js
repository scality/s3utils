'use strict';

const http = require('http');
const { httpRequest } = require('../../repairDuplicateVersionsSuite');

describe('repairDuplicateVersionsSuite::httpRequest', () => {
    let server;
    let serverPort;
    let lastRequest;

    beforeAll(done => {
        server = http.createServer((req, res) => {
            lastRequest = {
                method: req.method,
                path: req.url,
                headers: req.headers,
                body: '',
            };
            req.on('data', chunk => { lastRequest.body += chunk.toString('utf8'); });
            req.on('end', () => {
                res.writeHead(200, { 'content-type': 'application/json' });
                res.end(JSON.stringify({ ok: true }));
            });
        });
        server.listen(0, '127.0.0.1', () => {
            serverPort = server.address().port;
            done();
        });
    });

    afterAll(done => {
        server.close(done);
    });

    beforeEach(() => {
        lastRequest = null;
    });

    test('GET returns response with body attached', done => {
        const url = `http://127.0.0.1:${serverPort}/test-path`;
        httpRequest('GET', url, null, (err, res) => {
            expect(err).toBeNull();
            expect(res.statusCode).toBe(200);
            expect(JSON.parse(res.body)).toEqual({ ok: true });
            expect(lastRequest.method).toBe('GET');
            expect(lastRequest.path).toBe('/test-path');
            done();
        });
    });

    test('POST with multi-byte UTF-8 body sets content-length in bytes, not characters', done => {
        // String length (13 chars) differs from byte length (19 bytes) due to
        // multi-byte UTF-8 encoding of the Japanese characters and emoji.
        const body = '{"key":"日本語🎉"}';
        const expectedByteLength = Buffer.byteLength(body, 'utf8');
        expect(expectedByteLength).toBeGreaterThan(body.length);

        const url = `http://127.0.0.1:${serverPort}/write-path`;
        httpRequest('POST', url, body, (err, res) => {
            expect(err).toBeNull();
            expect(res.statusCode).toBe(200);
            expect(lastRequest.method).toBe('POST');
            expect(lastRequest.headers['content-type']).toBe('application/json');
            expect(parseInt(lastRequest.headers['content-length'], 10)).toBe(expectedByteLength);
            expect(lastRequest.body).toBe(body);
            done();
        });
    });
});
