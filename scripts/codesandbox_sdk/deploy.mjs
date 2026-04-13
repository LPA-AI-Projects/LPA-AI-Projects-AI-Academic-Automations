/**
 * Deploy generated Vite/React files to a CodeSandbox VM sandbox (Devbox) via @codesandbox/sdk.
 * Legacy browser "Define" sandboxes (SSE) are discontinued; SDK targets microVM sandboxes.
 *
 * Stdin: JSON { "files": { "path/to/file": "<string content>", ... } }
 * Stdout: one JSON line { "ok", "sandbox_id", "preview_url", "editor_url" }
 * Stderr: errors only
 */
import { CodeSandbox } from "@codesandbox/sdk";
import fs from "node:fs";

const VITE_PORT = 5173;

const key = process.env.CSB_API_KEY || process.env.CODESANDBOX_API_TOKEN;
if (!key) {
  process.stderr.write("Missing CSB_API_KEY or CODESANDBOX_API_TOKEN\n");
  process.exit(1);
}

const raw = fs.readFileSync(0, "utf8");
let payload;
try {
  payload = JSON.parse(raw);
} catch (e) {
  process.stderr.write(`Invalid JSON stdin: ${e}\n`);
  process.exit(1);
}

const files = payload.files;
if (!files || typeof files !== "object") {
  process.stderr.write('Expected JSON shape: { "files": { "path": "content" } }\n');
  process.exit(1);
}

async function main() {
  const sdk = new CodeSandbox(key);
  const sandbox = await sdk.sandboxes.create({
    privacy: "public-hosts",
    title: "Assessment Quiz",
  });
  const client = await sandbox.connect();

  const batch = Object.entries(files).map(([path, content]) => ({
    path,
    content: typeof content === "string" ? content : String(content ?? ""),
  }));

  await client.fs.batchWrite(batch);
  await client.commands.run("npm install");
  await client.commands.runBackground("npm run dev");
  await client.ports.waitForPort(VITE_PORT, { timeoutMs: 300000 });

  const previewUrl = client.hosts.getUrl(VITE_PORT);
  const editorUrl = client.editorUrl;

  process.stdout.write(
    JSON.stringify({
      ok: true,
      sandbox_id: sandbox.id,
      preview_url: previewUrl,
      editor_url: editorUrl,
    }) + "\n"
  );

  await client.disconnect();
}

main().catch((e) => {
  process.stderr.write(String(e?.stack || e) + "\n");
  process.exit(1);
});
