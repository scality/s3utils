import { defineConfig } from 'eslint/config';
import scality from '@scality/eslint-config-scality';
import { jest } from '@scality/eslint-config-scality/testing';

export default defineConfig(
    ...scality,
    {
        files: ['**/*.js'],
        languageOptions: {
            sourceType: 'commonjs',
        },
    },
    jest(['tests/**/*.js']),
);
