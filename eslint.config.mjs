import prettier from "eslint-config-prettier";
import tseslint from "typescript-eslint";

export default [
  {
    ignores: ["dist/**", "coverage/**", "node_modules/**", ".sisyphus/evidence/**"],
  },
  ...tseslint.configs.recommended,
  prettier,
  {
    files: ["**/*.ts"],
    rules: {
      "@typescript-eslint/consistent-type-imports": ["error", { prefer: "type-imports" }],
    },
  },
];
