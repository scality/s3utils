const { RaftJournalReader } = require('../../../SproxydKeysScan/DuplicateKeysIngestion');
const { subscribers } = require('../../../SproxydKeysScan/SproxydKeysSubscribers');
const fs = require('fs');

function getMockResponse(mockStatusCode) {
    const mockBody = fs.readFileSync(`${__dirname}/RaftJournalTestData.json`, 'utf8');
    const mockResponse = {
        body: mockBody,
        statusCode: mockStatusCode,
    };
    return mockResponse;
}

const mockHttpRequest = (err, res) => jest.fn((method, requestUrl, requestBody, callback) => callback(err, res));

function setupJournalReader() {
    const raftJournalReader = new RaftJournalReader(1, 10, 1, subscribers);
    raftJournalReader.processor.insert = jest.fn().mockReturnValue(true);
    return raftJournalReader;
}

describe.only('RaftJournalReader', () => {
    describe('::getBatch', () => {
        const raftJournalReader = setupJournalReader(getMockResponse(200));
        afterEach(() => {
            raftJournalReader._httpRequest.mockReset();
        });

        const returnedError = (err, body) => {
            expect(raftJournalReader._httpRequest).toHaveBeenCalled();
            expect(raftJournalReader._httpRequest).toHaveBeenCalledWith(
                'GET',
                expect.anything(),
                null,
                expect.anything()
            );
            expect(err).not.toBe(null);
            expect(body).toBe(undefined);
        };

        test('should correctly read mocked data', () => {
            raftJournalReader._httpRequest = mockHttpRequest(null, getMockResponse(200));
            raftJournalReader.getBatch((err, body) => {
                expect(raftJournalReader._httpRequest).toHaveBeenCalled();
                expect(raftJournalReader._httpRequest).toHaveBeenCalledWith(
                    'GET',
                    expect.anything(),
                    null,
                    expect.anything()
                );
                expect(err).toBe(null);
                expect(body).not.toBe(null);
                expect(body.log).not.toBe(null);
            });
        });

        test('should return error with a none-200 status code', () => {
            raftJournalReader._httpRequest = mockHttpRequest(null, getMockResponse(500));
            raftJournalReader.getBatch((err, body) => {
                returnedError(err, body);
            });
        });

        test('should return error with a missing body', () => {
            raftJournalReader._httpRequest = mockHttpRequest(null, { statusCode: 200 });
            raftJournalReader.getBatch((err, body) => {
                returnedError(err, body);
            });
        });

        test('should return error with null response', () => {
            raftJournalReader._httpRequest = mockHttpRequest(null, null);
            raftJournalReader.getBatch((err, body) => {
                returnedError(err, body);
            });
        });
    });

    describe('::processBatch', () => {
        const raftJournalReader = setupJournalReader(getMockResponse(200));
        raftJournalReader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        afterAll(() => raftJournalReader._httpRequest.mockReset());

        raftJournalReader.getBatch((err, body) => {
            raftJournalReader.processBatch(body, (err, res) => {
                test('processes logs into a list of { objectKey, sproxydKeys }', () => {
                    expect(err).toBe(null);
                    expect(res).not.toBe(null);
                    expect(res).toBeInstanceOf(Array);
                    expect(res.length).toBeGreaterThan(1);
                });

                const obj = res[0];
                test('has correct objectKeys', () => {
                    expect(obj.objectKey).not.toBe(null);
                    expect(typeof obj.objectKey).toBe('string');
                    expect(obj.objectKey).toEqual('crr_testing/small-object-8\u000098364713705553999999RG001  1.26.11');
                });

                test('has correct sproxydKeys', () => {
                    expect(obj.sproxydKeys).not.toBe(null);
                    expect(obj.sproxydKeys).toBeInstanceOf(Array);
                    expect(obj.sproxydKeys.length).toBeGreaterThan(0);

                    const sproxydKey = obj.sproxydKeys[0];
                    expect(sproxydKey).toEqual('5D11385B6915C1ADC28532E120D5FB59964F4F20');
                });
            });
        });
    });

    describe('::updateStatus', () => {
        const raftJournalReader = setupJournalReader(getMockResponse(200));
        raftJournalReader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        afterAll(() => raftJournalReader._httpRequest.mockReset());

        raftJournalReader.getBatch((err, body) => {
            raftJournalReader.processBatch(body, (err, extractedKeys) => {
                const oldBegin = raftJournalReader.begin;
                const insert = raftJournalReader.processor.insert;

                raftJournalReader.updateStatus(extractedKeys, () => {
                    test('updates sproxydKeysMap with processed keys', () => {
                        expect(raftJournalReader._httpRequest).toHaveBeenCalled();
                        expect(insert).toHaveBeenCalled();
                        expect(insert).toHaveBeenCalledWith(
                            expect.anything(),
                            expect.anything(),
                            'source-bucket'
                        );
                    });

                    test('updates begin property in RaftJournalReader instance', () => {
                        const newBegin = raftJournalReader.begin;
                        const limit = raftJournalReader.limit;
                        expect(newBegin).toEqual(Math.min(raftJournalReader.cseq + 1, oldBegin + limit));
                    });
                });
            });
        });
    });

    describe('::runOnce', () => {
        const raftJournalReader = setupJournalReader(getMockResponse(200));
        raftJournalReader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        afterEach(() => raftJournalReader._httpRequest.mockReset());

        test('inserts correct sproxyd key data into SproxydKeyProcessor', () => {
            expect(raftJournalReader.begin).toEqual(1);
            raftJournalReader.runOnce((err, timeout) => {
                expect(err).toBe(null);
                expect(timeout).toBe(0);
                expect(raftJournalReader._httpRequest).toHaveBeenCalled();
                expect(raftJournalReader.processor.insert).toHaveBeenCalled();
                expect(raftJournalReader.begin).toBeGreaterThan(1);
            });
        });

        const waitsFiveSeconds = (err, timeout) => {
            expect(err).not.toBe(null);
            expect(timeout).toBe(5000);
            expect(raftJournalReader._httpRequest).toHaveBeenCalled();
            expect(raftJournalReader.processor.insert).not.toHaveBeenCalled();
        };

        test('should set a timeout of 5000 milliseconds when there is any error during ingestion', () => {
            raftJournalReader._httpRequest = mockHttpRequest(null, getMockResponse(500));
            raftJournalReader.processor.insert.mockReset();
            raftJournalReader.runOnce((err, timeout) => {
                waitsFiveSeconds(err, timeout);
            });
        });

        test('should set a timeout of 5000 milliseconds when there is no new data from raft journal', () => {
            raftJournalReader._httpRequest = mockHttpRequest(null, null);
            raftJournalReader.processor.insert.mockReset();
            raftJournalReader.runOnce((err, timeout) => {
                waitsFiveSeconds(err, timeout);
            });
        });
    });

    describe.skip('::run', () => {
    // TODO: should return a timeout of 0 milliseconds for first iteration, followed by 5000 for each iteration onwards
    });
});
