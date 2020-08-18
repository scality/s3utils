const http = require('http');
const async = require('async');
const md5 = require('md5');

let verbose = process.env.VERBOSE;
if (verbose === undefined) {
    verbose = false;
}

let pageSize = process.env.PAGE_SIZE;
if (pageSize === undefined) {
    pageSize = 1000;
}

let myArgs = process.argv.slice(2);

if (myArgs.length != 3) {
    console.log('usage: node raftLogCheck.js bucketdHost bucketdPort raftSession');
    process.exit(1);
}

let bucketdHost = myArgs[0];
let bucketdPort = myArgs[1];
let raftSession = myArgs[2];

function getUrl(url, cb) {
    if (verbose) {
	console.log('url', url);
    }
    http.get(url, res => {
	let body = "";
	
	res.on("data", (chunk) => {
            body += chunk;
	});
	
	res.on("end", () => {
            try {
		let json = JSON.parse(body);
		return cb(null, json);
            } catch (error) {
		return cb(error);
            };
	});
	
    }).on("error", (error) => {
	return cb(error);
    });
}

getUrl(
    `http://${bucketdHost}:${bucketdPort}/_/raft_sessions/${raftSession}/info`,
    (err, info) => {
	if (err) {
	    console.log(err.message);
	    process.exit(1);
	}
	if (verbose) {
	    console.log('info:', info);
	}
	// get raft info on leader
	const leaderHost = info.leader.host;
	const leaderPort = info.leader.adminPort;
	const connected = info.connected;
	getUrl(
	    `http://${leaderHost}:${leaderPort}/_/raft/state`,
	    (err, state) => {
		if (err) {
		    console.log(err.message);
		    process.exit(1);
		}
		console.log('state:', state);
		const _bseq = state.backups * 10000 + 1;
		const _vseq = state.committed;
		let seq = _bseq;
		let limit = pageSize;
		async.until(() => {
		    if (verbose) {
			console.log('test', seq, _vseq);
		    }
		    return seq >= _vseq;
		}, cb => {
		    console.log('step', seq, limit);
		    getUrl(
			`http://${leaderHost}:${leaderPort}/_/raft/log?begin=${seq}&limit=${limit}`,
			(err, leaderLog) => {
			    if (err) {
				console.log(err.message);
				process.exit(1);
			    }
			    if (verbose) {
				console.log('leaderLog:', leaderLog);
			    }
			    async.each(connected, (member, cb) => {
				getUrl(
				    `http://${leaderHost}:${leaderPort}/_/raft/log?begin=${seq}&limit=${limit}`,
				    (err, memberLog) => {
					if (err) {
					    return cb(err);
					}
					if (verbose) {
					    console.log('memberLog:', memberLog);
					}
					let i, j;
					for (i = 0;i < limit;i++) {
					    if (leaderLog.log !== undefined && 
						leaderLog.log[i] !== undefined &&
						leaderLog.log[i].entries != undefined) {
						for (j = 0;j < leaderLog.log[i].entries.length;j++) {
						    if (memberLog.log === undefined ||
							memberLog.log[i] === undefined ||
							memberLog.log[i].entries === undefined) {
							console.log('undefined entries at', seq + i, j);
							continue ;
						    }
						    if (leaderLog.log[i].entries[j].key === undefined) {
							console.log('undefined leader key at', seq + i, j);
							continue ;
						    }
						    if (leaderLog.log[i].entries[j].value === undefined) {
							console.log('undefined leader value at', seq + i, j);
							continue ;
						    }
						    if (memberLog.log[i].entries[j].key === undefined) {
							console.log('undefined member key at', seq + i, j);
							continue ;
						    }
						    if (memberLog.log[i].entries[j].value === undefined) {
							console.log('undefined member value at', seq + i, j);
							continue ;
						    }
						    if (md5(memberLog.log[i].entries[j].key) !=
							md5(leaderLog.log[i].entries[j].key)) {
							console.log('key mismatch at',
								    seq + i, j, memberLog.log[i].entries[j].key);
						    }
						    if (md5(memberLog.log[i].entries[j].value) !=
							md5(leaderLog.log[i].entries[j].value)) {
							console.log('value mismatch at', 
								    seq + i, j, memberLog.log[i].entries[j].value);
						    }
						}
					    }
					}
					return cb();
				    });
			    }, err => {
				if (err) {
				    console.log('step error', err.message);
				    return cb(err);
				}
				console.log('step finished');
				seq += limit;
				return cb();
			    });
			});
		}, err => {
		    if (err) {
			console.log('paging error', err.message);
		    }
		    console.log('DONE');
		});
	    });
    });

