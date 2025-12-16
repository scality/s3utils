module.exports = {
    testEnvironment: 'node',
    moduleFileExtensions: ['js', 'jsx', 'json', 'node'],
    setupFiles: ['<rootDir>/tests/conf/envSetup.js'],
    testMatch: ['**/tests/**/*.js?(x)'],
    collectCoverageFrom: [
        '**/*.js',
        '!**/node_modules/**',
        '!**/tests/**',
        '!**/coverage/**',
        '!jest.config.js',
    ],
};

