const { RaftJournalReader } = require('../../../ObjectRepair/DuplicateKeysIngestion');
const { subscribers } = require('../../../ObjectRepair/SproxydKeysSubscribers');
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
    const reader = new RaftJournalReader(1, 10, 1, subscribers);
    reader.processor.insert = jest.fn().mockReturnValue(true);
    return reader;
}

describe('RaftJournalReader', () => {
    describe('::setBegin', () => {
        const reader = setupJournalReader();

        beforeEach(() => {
            reader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        });

        afterEach(() => {
            reader._httpRequest.mockReset();
        });

        test('when begin is undefined, begin = latest cseq - lookBack', () => {
            reader.begin = undefined;
            reader.lookBack = 5;

            reader.setBegin(err => {
                expect(err).toBe(undefined);
                expect(reader.begin).toEqual(reader.cseq - reader.lookBack);
            });
        });

        test('begin is at minimum 1, even with lookback > latest cseq', () => {
            reader.begin = undefined;
            reader.lookBack = Infinity;

            reader.setBegin(err => {
                expect(err).toBe(undefined);
                expect(reader.begin).toEqual(1);
            });
        });

        test('if begin is set from a previous call, use the existing begin and ignore lookBack', () => {
            reader.begin = 3;
            reader.lookBack = Infinity;

            reader.setBegin(err => {
                expect(err).toBe(undefined);
                expect(reader._httpRequest).not.toHaveBeenCalled();
                expect(reader.begin).toEqual(3);
            });
        });
    });
    describe('::getBatch', () => {
        const reader = setupJournalReader(getMockResponse(200));
        afterEach(() => {
            reader._httpRequest.mockReset();
        });

        const returnedError = (err, body) => {
            expect(reader._httpRequest).toHaveBeenCalled();
            expect(reader._httpRequest).toHaveBeenCalledWith(
                'GET',
                expect.anything(),
                null,
                expect.anything()
            );
            expect(err).not.toBe(null);
            expect(body).toBe(undefined);
        };

        test('should correctly read mocked data', () => {
            reader._httpRequest = mockHttpRequest(null, getMockResponse(200));
            reader.getBatch((err, body) => {
                expect(reader._httpRequest).toHaveBeenCalled();
                expect(reader._httpRequest).toHaveBeenCalledWith(
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
            reader._httpRequest = mockHttpRequest(null, getMockResponse(500));
            reader.getBatch((err, body) => {
                returnedError(err, body);
            });
        });

        test('should return error with a missing body', () => {
            reader._httpRequest = mockHttpRequest(null, { statusCode: 200 });
            reader.getBatch((err, body) => {
                returnedError(err, body);
            });
        });

        test('should return error with null response', () => {
            reader._httpRequest = mockHttpRequest(null, null);
            reader.getBatch((err, body) => {
                returnedError(err, body);
            });
        });
    });

    describe('::processBatch', () => {
        const reader = setupJournalReader(getMockResponse(200));
        reader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        afterAll(() => reader._httpRequest.mockReset());

        reader.getBatch((err, body) => {
            reader.processBatch(body, (err, res) => {
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
        const reader = setupJournalReader(getMockResponse(200));
        reader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        afterAll(() => reader._httpRequest.mockReset());

        reader.getBatch((err, body) => {
            reader.processBatch(body, (err, extractedKeys) => {
                const oldBegin = reader.begin;
                const insert = reader.processor.insert;

                reader.updateStatus(extractedKeys, () => {
                    test('updates sproxydKeysMap with processed keys', () => {
                        expect(reader._httpRequest).toHaveBeenCalled();
                        expect(insert).toHaveBeenCalled();
                        expect(insert).toHaveBeenCalledWith(
                            expect.anything(),
                            expect.anything(),
                            'source-bucket'
                        );
                    });

                    test('updates begin property in RaftJournalReader instance', () => {
                        const newBegin = reader.begin;
                        const limit = reader.limit;
                        expect(newBegin).toEqual(Math.min(reader.cseq + 1, oldBegin + limit));
                    });
                });
            });
        });
    });

    describe('::runOnce', () => {
        const reader = setupJournalReader(getMockResponse(200));
        reader._httpRequest = mockHttpRequest(null, getMockResponse(200));
        afterEach(() => reader._httpRequest.mockReset());

        test('inserts correct sproxyd key data into SproxydKeyProcessor', () => {
            expect(reader.begin).toEqual(1);
            reader.runOnce((err, timeout) => {
                expect(err).toBe(null);
                expect(timeout).toBe(0);
                expect(reader._httpRequest).toHaveBeenCalled();
                expect(reader.processor.insert).toHaveBeenCalled();
                expect(reader.begin).toBeGreaterThan(1);
            });
        });

        const waitsFiveSeconds = (err, timeout) => {
            expect(err).not.toBe(null);
            expect(timeout).toBe(5000);
            expect(reader._httpRequest).toHaveBeenCalled();
            expect(reader.processor.insert).not.toHaveBeenCalled();
        };

        test('should set a timeout of 5000 milliseconds when there is any error during ingestion', () => {
            reader._httpRequest = mockHttpRequest(null, getMockResponse(500));
            reader.processor.insert.mockReset();
            reader.runOnce((err, timeout) => {
                waitsFiveSeconds(err, timeout);
            });
        });

        test('should set a timeout of 5000 milliseconds when there is no new data from raft journal', () => {
            reader._httpRequest = mockHttpRequest(null, null);
            reader.processor.insert.mockReset();
            reader.runOnce((err, timeout) => {
                waitsFiveSeconds(err, timeout);
            });
        });
    });
});
