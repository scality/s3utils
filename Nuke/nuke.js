const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');

const { argv } = require('yargs')
    .option('bucket', {
        alias: 'b',
        describe: 'Name of the bucket',
    })
    .option('accessKey', {
        alias: 'a',
        describe: 'Access Key',
    })
    .option('secretKey', {
        alias: 's',
        describe: 'Secret Key',
    })
    .option('endpoint', {
        alias: 'e',
        describe: 'S3 endpoint',
    })
    .option('delete-latest', {
        describe: 'Deletes all versions',
    })
    .demandOption(['bucket', 'accessKey', 'secretKey',
        'endpoint'], 'Please provide all required arguments to work with this tool')
    .help()
    .argv;

const BUCKET = argv.bucket;
const LISTING_LIMIT = 30000000;
const ACCESSKEY = argv.accessKey;
const SECRETKEY = argv.secretKey;
const ENDPOINT = argv.endpoint;
const QUIET_MODE = true;


AWS.config.update({
    accessKeyId: ACCESSKEY,
    secretAccessKey: SECRETKEY,
    region: 'us-west-1',
    sslEnabled: false,
    endpoint: ENDPOINT,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
});
const s3 = new AWS.S3({
    httpOptions: {
        maxRetries: 0,
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
});
// list object versions
function _listObjectVersions(VersionIdMarker, KeyMarker, cb) {
    s3.listObjectVersions({ Bucket: BUCKET, MaxKeys: LISTING_LIMIT, VersionIdMarker, KeyMarker }, cb);
}
// return object with key and version_id
function _getKeys(keys) {
    return keys.map(v => ({
        Key: v.Key,
        VersionId: v.VersionId,
    }));
}
// delete all versions of an object
function _deleteVersions(objectsToDelete, cb) {
    // multi object delete can delete max 1000 objects
    function _batchDelete(Objects, done) {
        s3.deleteObjects({ Bucket: BUCKET, Delete: { Objects, Quiet: QUIET_MODE } }, (err, res) => {
            if (err) {
                console.log('batch delete err', err);
                return done(err);
            }
            Objects.forEach(v => console.log('deleted key: ' + v.Key));
            return done();
        });
    }
    const fns = [];
    while(Object.keys(objectsToDelete).length > 0) {
        const Objects = objectsToDelete.splice(0, 999);
        fns.push(done => _batchDelete(Objects, done));
    }
    async.parallel(fns, cb);
}

function nukeObjects(cb) {
    let VersionIdMarker = null;
    let KeyMarker = null;
    const nonCurrent = [];
    async.doWhilst(
        done => _listObjectVersions(VersionIdMarker, KeyMarker, (err, data) => {
            if (err) {
                return done(err);
            }
            // const filteredData = data.Versions.filter(item => item.IsLatest !== true);
            if (argv.deleteLatest === 'true') {
                keysToDelete = _getKeys(data.Versions);
            } else {
                Object.keys(data.Versions).forEach(function eachKey(key) {
                    if (!data.Versions[key].IsLatest) {
                        nonCurrent.push(data.Versions[key]);
                    }
                });
                keysToDelete = _getKeys(nonCurrent);
            }
            VersionIdMarker = data.NextVersionIdMarker;
            KeyMarker = data.NextKeyMarker;
            const markersToDelete = _getKeys(data.DeleteMarkers);
            _deleteVersions(keysToDelete.concat(markersToDelete), done);
        }),
        () => {
            if (VersionIdMarker || KeyMarker) {
                return true;
            }
            return false;
        },
        cb,
    );
}
nukeObjects((err, res) => {
    if (err) {
        return console.log('error occured deleting objects', err);
    }
    return console.log('completed deletion');
});
