import { defineConfig } from 'eslint/config';
import scality from '@scality/eslint-config-scality';
import { jest } from '@scality/eslint-config-scality/testing';

export default defineConfig(
    ...scality,
    {
        languageOptions: {
            ecmaVersion: 2020,
            sourceType: 'script',
        },
        rules: {
            'max-len': 'off',
            'no-redeclare': 'off',
        },
    },
    jest(['tests/**/*.js']),
);
