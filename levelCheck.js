const async = require('async');
const werelogs = require('werelogs');
const level = require('level');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';
const loggerConfig = {
    level: logLevel,
    dump: 'error',
};
werelogs.configure(loggerConfig);
const log = new werelogs.Logger('s3utils::levelCheck');

const _pageSize = process.env.PAGE_SIZE;
let pageSize;
if (_pageSize !== undefined) {
    pageSize = parseInt(_pageSize, 10);
} else {
    pageSize = 1000;
}

let lastKey = process.env.LAST_KEY;

const myArgs = process.argv.slice(2);

if (myArgs.length < 2) {
    log.error(
        'usage: node levelCheck.js db1 db2 ... dbn');
    process.exit(1);
}

const db0 = level(myArgs[0]);
const dbs = [];
let i;
for (i = 0; i < myArgs.length - 1; i++) {
    dbs[i] = level(myArgs[i + 1]);
}

function getPage(db, gt, limit, cb) {
    log.debug(`startKey ${gt} limit=${limit}`);
    const buffer = [];
    db.createReadStream({
        gt,
        limit,
    })
        .on('data', data => buffer.push(data))
        .on('error', err => cb(err))
        .on('end', () => log.debug('getPage done'))
        .on('close', () => cb(null, buffer));
}

let count = 0;
let stop = false;
async.until(() => stop, cb => {
    getPage(db0, lastKey, pageSize, (err, buffer0) => {
        if (buffer0.length === 0) {
            stop = true;
            return cb();
        }
        async.each(dbs, (db, next) => {
            getPage(db, lastKey, pageSize, (err, buffer) => {
                if (err) {
                    log.error('getPage error', { error: err.message });
                    return next(err);
                }
                let i;
                for (i = 0; i < buffer0.length; i++) {
                    if (buffer0[i].key !== buffer[i].key) {
                        log.error('Keys differs', {
                            offset: count + i,
                            refKey: buffer0[i].key,
                            key: buffer[i].key,
                        });
                    }
                    if (buffer0[i].value !== buffer[i].value) {
                        log.error('Entries differs', {
                            offset: count + i,
                            refKey: buffer0[i].key,
                            refValue: buffer0[i].value,
                            value: buffer[i].value,
                        });
                    }
                }
                return next();
            });
        }, err => {
            if (err) {
                log.error('dbs error', { error: err.message });
                process.exit(1);
            }
            count += buffer0.length;
            process.stdout.write(`${count}\r`);
            lastKey = buffer0[buffer0.length - 1].key;
            return cb();
        });
        return undefined;
    }, err => {
        if (err) {
            log.error('paging error', { error: err.message });
            process.exit(1);
        }
        log.info('DONE');
        process.exit(0);
    });
});
