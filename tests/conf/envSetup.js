const { TextEncoder, TextDecoder } = require('util');

global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;

process.env.BUCKETD_HOSTPORT = 'localhost:9000';
process.env.SPROXYD_HOSTPORT = 'localhost';
