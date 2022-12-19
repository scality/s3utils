
const mongoMemoryServerParams = {
    debug: true,
    instanceOpts: [
        { port: 27018 },
    ],
    replSet: {
        name: 'customSetName',
        count: 1,
        dbName: 'metadata',
        storageEngine: 'ephemeralForTest',
    },
};

function createMongoParamsFromMongoMemoryRepl(repl) {
    const {
        ip, port, dbName, replSet,
    } = repl.servers[0].instanceInfo;
    return {
        replicaSetHosts:
            `${ip}:${port}`,
        writeConcern: 'majority',
        replicaSet: replSet,
        readPreference: 'primary',
        database: dbName,
        replicationGroupId: 'GR001',
    };
}

module.exports = {
    createMongoParamsFromMongoMemoryRepl,
    mongoMemoryServerParams,
};
