const EventEmitter = require('events');

class Subprocess extends EventEmitter {
    constructor() {
        super();
        this.connected = true;
    }

    isConnected() {
        return this.connected;
    }

    send() {}
}

module.exports = Subprocess;

