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

describe('repairDuplicateVersionsSuite::putObjectMetadata', () => {
    let putObjectMetadata;
    let server;
    let serverPort;
    let lastRequest;
    let responseStatus;

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
                res.writeHead(responseStatus);
                res.end();
            });
        });
        server.listen(0, '127.0.0.1', () => {
            serverPort = server.address().port;
            // OBJECT_REPAIR_BUCKETD_HOSTPORT is captured at module load time, so
            // set it before requiring the module via isolateModules.
            process.env.OBJECT_REPAIR_BUCKETD_HOSTPORT = `127.0.0.1:${serverPort}`;
            jest.isolateModules(() => {
                ({ putObjectMetadata } = require('../../repairDuplicateVersionsSuite'));
            });
            done();
        });
    });

    afterAll(done => {
        server.close(done);
    });

    beforeEach(() => {
        lastRequest = null;
        responseStatus = 200;
    });

    test('POSTs JSON-serialised metadata with multi-byte UTF-8 characters to the correct bucketd path', done => {
        const md = {
            'content-type': 'text/plain',
            'size': 42,
            'key': '日本語🎉',
        };
        putObjectMetadata('s3://mybucket/mykey', md, err => {
            expect(err).toBeFalsy();
            expect(lastRequest.method).toBe('POST');
            expect(lastRequest.path).toBe('/default/bucket/mybucket/mykey');
            expect(JSON.parse(lastRequest.body)).toEqual(md);
            done();
        });
    });

    test('calls back with error immediately when objectUrl does not start with "s3://"', done => {
        putObjectMetadata('http://mybucket/mykey', {}, err => {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toMatch(/malformed object URL/);
            expect(lastRequest).toBeNull(); // no HTTP request made
            done();
        });
    });

    test('calls back with error when bucketd returns a non-200 status', done => {
        responseStatus = 500;
        putObjectMetadata('s3://mybucket/mykey', {}, err => {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toMatch(/500/);
            done();
        });
    });
});
