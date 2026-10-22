import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, cpSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const dashboardRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const nextBin = join(dashboardRoot, "node_modules", "next", "dist", "bin", "next");

execFileSync(process.execPath, [nextBin, "build"], {
  cwd: dashboardRoot,
  stdio: "inherit",
});

const indexFile = join(dashboardRoot, "out", "index.html");
if (!existsSync(indexFile)) {
  throw new Error("Next.js static export did not create out/index.html");
}

const indexHtml = readFileSync(indexFile, "utf8");
if (!indexHtml.includes("/dashboard/_next/")) {
  throw new Error("Static export assets are not rooted at /dashboard");
}

const bundle = join(dashboardRoot, "dist");
rmSync(bundle, { recursive: true, force: true });
cpSync(join(dashboardRoot, "out"), bundle, { recursive: true });
console.log("Bundled static dashboard in dashboard/dist");
