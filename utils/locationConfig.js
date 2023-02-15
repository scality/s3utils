const fs = require('fs');

function getLocationConfig(log, locationConfigFile) {
    const locationConfiguration = locationConfigFile || process.env.LOCATION_CONFIG_FILE || 'conf/locationConfig.json';

    if (!fs.existsSync(locationConfiguration)) {
        return null;
    }

    const buf = fs.readFileSync(locationConfiguration);
    let parsedConfig;
    try {
        parsedConfig = JSON.parse(buf.toString());
    } catch (e) {
        log.error('could not parse location config file', { error: e.message });
        parsedConfig = null;
    }
    return parsedConfig;
}

module.exports = getLocationConfig;
