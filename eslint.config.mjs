import { defineConfig } from 'eslint/config';
import scality from '@scality/eslint-config-scality';
import { jest } from '@scality/eslint-config-scality/testing';

export default defineConfig(
    ...scality,
    {
        languageOptions: {
            ecmaVersion: 2020,
            sourceType: 'script',
            globals: {
                BigInt: 'readonly',
            },
        },
        rules: {
            'no-plusplus': 'off',
            'no-underscore-dangle': 'off',
            'no-restricted-syntax': 'off',
            'max-len': 'off',
            'import/no-extraneous-dependencies': 'off',
            'no-continue': 'off',
            'global-require': 'off',
            'class-methods-use-this': 'off',
            'no-lonely-if': 'off',
            'max-classes-per-file': 'off',
            'prefer-spread': 'off',
            'no-constructor-return': 'off',
            'no-redeclare': 'off',
        },
    },
    jest(['tests/**/*.js']),
);
