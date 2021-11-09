process.env.BUCKETD_HOSTPORT = 'localhost:9000';
process.env.SPROXYD_HOSTPORT = 'localhost';

const { RaftJournalReader } = require('../../../SproxydKeysScan/DuplicateKeysIngestion');
const { subscribers } = require('../../../SproxydKeysScan/sproxydKeysSubscribers');
const fs = require('fs');

describe('RaftJournalReader', () => {
    let raftJournalReader = null;
    raftJournalReader = new RaftJournalReader(1, 10, 1, subscribers);
    const data = fs.readFileSync(`${__dirname}/RaftJournalTestData.json`, 'utf8');
    const body = JSON.parse(data);
    raftJournalReader.getBatch = jest.fn(cb => cb(null, body));

    beforeEach(() => {
    // todo
    });

    afterEach(() => {
    // todo
    });

    describe('::getBatch', () => {
        test('should correctly read mocked data', () => {
            raftJournalReader.getBatch((err, body) => {
                expect(err).toBe(null);
                expect(body).not.toBe(null);
                expect(body.log).not.toBe(null);
            });
        });
    });

    describe('::processBatch', () => {
        raftJournalReader.getBatch((err, body) => {
            raftJournalReader.processBatch(body, (err, res) => {
                test('processes logs into a list of { masterKey, sproxydKeys }', () => {
                    expect(err).toBe(null);
                    expect(res).not.toBe(null);
                    expect(res).toBeInstanceOf(Array);
                    expect(res.length).toBeGreaterThan(1);
                });

                const obj = res[0];
                test('has correct masterKeys', () => {
                    expect(obj.masterKey).not.toBe(null);
                    expect(typeof obj.masterKey).toBe('string');
                    expect(obj.masterKey).toEqual('crr_testing/small-object-8');
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

    describe.only('::updateStatus', () => {
        raftJournalReader.getBatch((err, body) => {
            raftJournalReader.processBatch(body, (err, extractedKeys) => {
                const oldBegin = raftJournalReader.begin;
                const insert = raftJournalReader.processor.insert = jest.fn().mockReturnValue(true);

                raftJournalReader.updateStatus(extractedKeys, () => {
                    test('updates sproxydKeysMap with processed keys', () => {
                        expect(insert).toHaveBeenCalled();
                    });

                    test('updates begin property in RaftJournalReader instance', () => {
                        const newBegin = raftJournalReader.begin;
                        const limit = raftJournalReader.limit;
                        expect(newBegin).toEqual(oldBegin + limit);
                    });
                });
            });
        });
    });
});
