// Renders an animated SVG at exact times through headless Chromium and the
// DevTools protocol (Node's built-in WebSocket; no packages).
//
//   node render.mjs file.svg stills out_prefix t1 t2 ...   -> PNG stills
//   node render.mjs file.svg video out.webm fps seconds     -> VP9 webm
import { spawn } from "node:child_process";
import { readdirSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import path from "node:path";
import { writeFileSync } from "node:fs";

const [file, mode, out, ...rest] = process.argv.slice(2);
const pw = path.join(homedir(), ".cache/ms-playwright");
const shell = readdirSync(pw).find((d) => d.startsWith("chromium_headless_shell"));
const shellDir = path.join(pw, shell);
const sub = readdirSync(shellDir).find((d) => d.startsWith("chrome-"));
const chrome = path.join(shellDir, sub, "chrome-headless-shell");
const ffdir = readdirSync(pw).find((d) => d.startsWith("ffmpeg"));
const ffmpeg = path.join(pw, ffdir, "ffmpeg-linux");

const port = 9300 + Math.floor(Math.random() * 500);
const proc = spawn(chrome, ["--no-sandbox", "--disable-gpu", `--remote-debugging-port=${port}`, "--hide-scrollbars", "--force-device-scale-factor=1", "about:blank"], { stdio: "ignore" });

async function wsURL() {
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/json/list`);
      const list = await r.json();
      const p = list.find((t) => t.type === "page");
      if (p) return p.webSocketDebuggerUrl;
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("chrome did not start");
}

const ws = new WebSocket(await wsURL());
await new Promise((r) => ws.addEventListener("open", r, { once: true }));
let id = 0;
const pending = new Map();
ws.addEventListener("message", (e) => {
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) {
    const { res, rej } = pending.get(m.id);
    pending.delete(m.id);
    m.error ? rej(new Error(JSON.stringify(m.error))) : res(m.result);
  }
});
const send = (method, params = {}) =>
  new Promise((res, rej) => {
    const i = ++id;
    pending.set(i, { res, rej });
    ws.send(JSON.stringify({ id: i, method, params }));
  });

const W = 1280, H = 720;
await send("Emulation.setDeviceMetricsOverride", { width: W, height: H, deviceScaleFactor: 1, mobile: false });
await send("Page.enable");
// Inline the SVG in a temporary page, so its CSS animations can be stepped.
const { readFileSync } = await import("node:fs");
const svg = readFileSync(file, "utf8");
const framePage = path.join(tmpdir(), `flightrec-frame-${process.pid}.html`);
writeFileSync(framePage,
  `<!doctype html><html><body style="margin:0;background:#090d16;overflow:hidden">${svg.replace("<svg ", '<svg style="display:block;width:1280px;height:720px" ')}</body></html>`);
await send("Page.navigate", { url: "file://" + framePage });
await new Promise((r) => setTimeout(r, 800));

async function seek(t) {
  await send("Runtime.evaluate", {
    expression: `(()=>{for(const a of document.getAnimations()){a.pause();a.currentTime=${t * 1000};}return document.getAnimations().length})()`,
    returnByValue: true,
  });
}
async function shot(fmt = "png") {
  const r = await send("Page.captureScreenshot", { format: fmt, quality: fmt === "jpeg" ? 92 : undefined, clip: { x: 0, y: 0, width: W, height: H, scale: 1 } });
  return Buffer.from(r.data, "base64");
}

if (mode === "reduced") {
  // What a viewer who prefers reduced motion sees: no seeking, the CSS
  // media query alone picks the frame.
  await send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-reduced-motion", value: "reduce" }] });
  await new Promise((r) => setTimeout(r, 300));
  writeFileSync(`${out}.png`, await shot("png"));
} else if (mode === "stills") {
  for (const t of rest.map(Number)) {
    await seek(t);
    writeFileSync(`${out}_${t}.png`, await shot("png"));
  }
} else {
  const fps = Number(rest[0] || 30), secs = Number(rest[1] || 10);
  const ff = spawn(ffmpeg, ["-y", "-loglevel", "error", "-f", "image2pipe", "-c:v", "mjpeg", "-framerate", String(fps), "-i", "pipe:0", "-c:v", "libvpx-vp9", "-b:v", "0", "-crf", "34", "-deadline", "realtime", "-cpu-used", "8", "-row-mt", "1", out], { stdio: ["pipe", "ignore", "inherit"] });
  let dead = false;
  ff.on("close", (c) => { dead = true; if (c) { console.error("ffmpeg exited", c); process.exit(1); } });
  const n = Math.round(fps * secs);
  for (let i = 0; i < n; i++) {
    await seek(i / fps);
    const buf = await shot("jpeg");
    if (dead) break;
    if (!ff.stdin.write(buf)) await new Promise((r) => ff.stdin.once("drain", r));
    if (i % (fps * 5) === 0) process.stderr.write(`frame ${i}/${n}\n`);
  }
  ff.stdin.end();
  if (!dead) await new Promise((r) => ff.on("close", r));
}
ws.close();
proc.kill();
(await import("node:fs")).rmSync(framePage, { force: true });
process.exit(0);
