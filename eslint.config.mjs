import path from "node:path";
import { fileURLToPath } from "node:url";
import js from "@eslint/js";
import { FlatCompat } from "@eslint/eslintrc";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all
});

export default [...compat.extends('@scality/scality'), 
    {
        languageOptions: {
            ecmaVersion: 2020,
            sourceType: "script",
            globals: {
                BigInt: "readonly",
                jest: "readonly",
                expect: "readonly",
                fail: "readonly",
                beforeAll: "readonly",
                afterAll: "readonly",
            },
        },
        rules: {
            "no-plusplus": 'off',
            "no-underscore-dangle": 'off',
            "no-restricted-syntax": 'off',
            "max-len": 'off',
            "import/no-extraneous-dependencies": 'off',
            "no-continue": 'off',
            "global-require": 'off',
            "no-prototype-builtins": 'off',
            "class-methods-use-this": 'off',
            "no-unused-vars": 'off',
            "no-lonely-if": 'off',
            "max-classes-per-file": 'off',
            "prefer-spread": 'off',
            "no-constructor-return": 'off',
            "new-cap": 'off',
            "no-redeclare":"off",
        }
    }
];
