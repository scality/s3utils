process.env.BUCKETD_HOSTPORT = 'localhost:9000';
process.env.SPROXYD_HOSTPORT = 'localhost';

const { RaftJournalReader } = require('../../../SproxydKeysScan/DuplicateKeysIngestion');
const { subscribers } = require('../../../SproxydKeysScan/sproxydKeysSubscribers');
const fs = require('fs');

describe('RaftJournalReader', () => {
    let raftJournalReaderMock = null;
    raftJournalReaderMock = new RaftJournalReader(1, 10, 1, subscribers);
    const data = fs.readFileSync(`${__dirname}/RaftJournalTestData.json`, 'utf8');
    const body = JSON.parse(data);
    raftJournalReaderMock.getBatch = jest.fn(cb => cb(null, body));

    beforeEach(() => {
    // todo
    });

    afterEach(() => {
    // todo
    });

    describe.only('::getBatch', () => {
        test('should correctly read mocked data', () => {
            raftJournalReaderMock.getBatch((err, body) => {
                expect(err).toBe(null);
                expect(body).not.toBe(null);
                expect(body.log).not.toBe(null);
            });
        });
    });
});
