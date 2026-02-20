import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/cli.ts", "src/codegen.ts"],
  format: ["esm"],
  target: "node18",
  outDir: "dist",
  clean: true,
  dts: true,
  sourcemap: true,
  splitting: true,
});
