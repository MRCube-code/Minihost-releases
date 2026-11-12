import { spawn, execSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { promises as fs } from "node:fs";
import { WebSocketServer } from "ws";

// ─── Constants ──────────────────────────────────────────────────────────

const HOME             = os.homedir();
const MINIHOST_DIR     = path.join(HOME, "minihost");
const PAPER_DIR        = path.join(MINIHOST_DIR, "paper");
const PLUGINS_DIR      = path.join(PAPER_DIR, "plugins");
const RES_PACKS_DIR    = path.join(PAPER_DIR, "resourcepacks");
const SERVER_PROPERTIES_PATH = path.join(PAPER_DIR, "server.properties");
const EULA_PATH        = path.join(PAPER_DIR, "eula.txt");
const PAPER_JAR_PATH   = path.join(PAPER_DIR, "paper.jar");
const GEYSER_JAR_PATH  = path.join(PLUGINS_DIR, "Geyser-Spigot.jar");
const VIAVERSION_JAR_PATH   = path.join(PLUGINS_DIR, "ViaVersion.jar");
const VIABACKWARDS_JAR_PATH = path.join(PLUGINS_DIR, "ViaBackwards.jar");
const FLOODGATE_JAR_PATH = path.join(PLUGINS_DIR, "floodgate-spigot.jar");

const PLAYIT_DIR          = path.join(MINIHOST_DIR, "playit");
const BACKUPS_DIR         = path.join(MINIHOST_DIR, "backups");
const PLAYIT_TUNNELS_FILE = path.join(PLAYIT_DIR, "tunnels.json");    // saved addresses (our own cache)
const PLAYIT_SECRET_FILE  = path.join(PLAYIT_DIR, "secret.key");      // playit.gg agent secret, pasted by the user once
const DEBIAN_DISTRO       = "debian";

const GEYSER_URL       = "https://download.geysermc.org/v2/projects/geyser/versions/latest/builds/latest/downloads/spigot";
const FLOODGATE_URL    = "https://download.geysermc.org/v2/projects/floodgate/versions/latest/builds/latest/downloads/spigot";
const PAPER_API_BASE   = "https://fill.papermc.io/v3/projects/paper";
const PAPER_USER_AGENT = "MiniHost/1.0 (+https://github.com/MRCube-code/Mini-Host)";
const READY_PATTERNS   = [/Done \(/i, /\[Server thread\/INFO\].*Done/i];
const ALLOWED_BASE     = MINIHOST_DIR;
const MODE_FILE        = path.join(MINIHOST_DIR, "server-mode.json");

async function installProotDebian() {
  log("[Bridge] Installing proot-distro...");
  await runCmd("pkg", ["install", "-y", "proot-distro"]);
  log("[Bridge] Installing Debian (this takes 5-8 minutes, one time only)...");
  await runCmd("proot-distro", ["install", DEBIAN_DISTRO]);
  log("[Bridge] Debian installed");
}

async function installPlayitInDebian() {
  log("[Bridge] Installing playit inside Debian...");
  try {
    await runCmd("proot-distro", ["login", DEBIAN_DISTRO, "--", "bash", "-c",
      "apt update && apt install -y curl gpg ca-certificates && curl -fsSL https://packages.playit.gg/install.sh | bash"
    ]);
    log("[Bridge] playit installed inside Debian");
  } catch (err) {
    log(`[Bridge] Install script exited non-zero (${err.message.slice(0, 150)}) — this is expected if it's the known systemd post-install hook; verifying playitd exists anyway...`);
  }
}

function getLocalIp() {
  try {
    const r = execSync("ip -4 addr 2>/dev/null", { encoding: "utf8" });
    const ip = [...r.matchAll(/inet\s+([\d.]+)/g)].map(m => m[1]).find(x => !x.startsWith("127."));
    if (ip) return ip;
  } catch {}
  try {
    const r = execSync("ifconfig 2>/dev/null", { encoding: "utf8" });
    const ip = [...r.matchAll(/inet\s+(?:addr:)?([\d.]+)/g)].map(m => m[1]).find(x => !x.startsWith("127."));
    if (ip) return ip;
  } catch {}
  try {
    const ifs = os.networkInterfaces();
    for (const name of Object.keys(ifs)) {
      for (const a of ifs[name] || []) {
        if (a.family === "IPv4" && !a.internal) return a.address;
      }
    }
  } catch {}
  return "check-wifi";
}

const state = {
  serverStatus: "stopped",
  logs: [],
  players: [],
  stats: { cpuPercent: 0, ramUsedMb: 0, ramLimitMb: 2048, tps: 20, uptimeSeconds: 0 },
  settings: {
    gamemode: "survival", difficulty: "normal", maxPlayers: 20, ramMb: 2048,
    port: 25565, bedrockPort: 19132, serverName: "Mini Host Server",
    motd: "Powered by Mini Host", onlineMode: false, viewDistance: 8,
    pvp: true, allowFlight: false, whiteList: false,
    mode: null,
    bedrockAuth: "offline",
  },
  worlds: [],
  packs: [],
  tunnel: {
    online:          false,
    state:           "disconnected",
    claimUrl:        "",
    javaAddress:     "",
    bedrockAddress:  "",
    method:          "playit",
  },
  startedAtMs: 0,
};

let serverProcess = null, playitProcess = null, playitRestartTimer = null, statsInterval = null;
let playitSetupInProgress = false;
const clients = new Set();

function log(line) {
  const formatted = `[${new Date().toLocaleTimeString()}] ${line}`;
  state.logs = [formatted, ...state.logs].slice(0, 500);
  broadcast("console.log", { line: formatted });
  process.stdout.write(formatted + "\n");
}

function broadcast(event, payload) {
  const msg = JSON.stringify({ type: "event", event, payload });
  for (const c of clients) { if (c.readyState === 1) try { c.send(msg); } catch {} }
}

function snapshot() {
  return {
    bridgeConnected: true,
    serverStatus:    state.serverStatus,
    logs:            state.logs,
    players:         state.players,
    stats:           state.stats,
    worlds:          state.worlds,
    packs:           state.packs,
    settings:        state.settings,
    tunnel:          state.tunnel,
    localIp:         getLocalIp(),
  };
}

function wget(url, dest, extraArgs = []) {
  return runCmd("wget", ["-q", "--show-progress", ...extraArgs, "-O", dest, url]);
}

function wgetJson(url) {
  return new Promise((resolve, reject) => {
    const proc = spawn("wget", ["-q", "-O", "-", "--header", `User-Agent: ${PAPER_USER_AGENT}`, url], { stdio: ["ignore", "pipe", "pipe"] });
    let data = "", stderr = "";
    proc.stdout.on("data", d => { data += String(d); });
    proc.stderr.on("data", d => { stderr += String(d); });
    proc.on("close", code => {
      if (code !== 0) { reject(new Error(`wget failed (exit ${code}) for ${url}: ${stderr.trim().slice(0, 200) || "no stderr"}`)); return; }
      try { resolve(JSON.parse(data)); }
      catch (e) { reject(new Error(`JSON parse failed for ${url}: ${e.message}`)); }
    });
    proc.on("error", reject);
  });
}

function compareVersions(a, b) {
  const pa = String(a).split(".").map(n => parseInt(n, 10) || 0);
  const pb = String(b).split(".").map(n => parseInt(n, 10) || 0);
  const len = Math.max(pa.length, pb.length);
  for (let i = 0; i < len; i++) {
    const d = (pa[i] || 0) - (pb[i] || 0);
    if (d !== 0) return d;
  }
  return 0;
}

async function getLatestGithubAsset(owner, repo, assetPattern) {
  const release = await wgetJson(`https://api.github.com/repos/${owner}/${repo}/releases/latest`);
  const asset = (release.assets || []).find(a => assetPattern.test(a.name));
  if (!asset) throw new Error(`No matching asset in ${owner}/${repo}'s latest release (${release.tag_name || "unknown tag"})`);
  return { tag: release.tag_name, url: asset.browser_download_url, name: asset.name };
}

async function ensureViaJars() {
  await removeOldViaJars();
  if (!(await exists(VIAVERSION_JAR_PATH))) {
    log("[Bridge] Finding latest ViaVersion release...");
    const via = await getLatestGithubAsset("ViaVersion", "ViaVersion", /^ViaVersion-.*\.jar$/);
    log(`[Bridge] Downloading ViaVersion ${via.tag}...`);
    await wget(via.url, VIAVERSION_JAR_PATH);
    log(`[Bridge] ViaVersion ${via.tag} ready`);
  }
  if (!(await exists(VIABACKWARDS_JAR_PATH))) {
    log("[Bridge] Finding latest ViaBackwards release...");
    const vb = await getLatestGithubAsset("ViaVersion", "ViaBackwards", /^ViaBackwards-.*\.jar$/);
    log(`[Bridge] Downloading ViaBackwards ${vb.tag}...`);
    await wget(vb.url, VIABACKWARDS_JAR_PATH);
    log(`[Bridge] ViaBackwards ${vb.tag} ready`);
  }
}

async function ensureFloodgateJar() {
  if (!(await exists(FLOODGATE_JAR_PATH))) {
    log("[Bridge] Downloading Floodgate (lets Bedrock players join without a linked Xbox account)...");
    await wget(FLOODGATE_URL, FLOODGATE_JAR_PATH);
    log("[Bridge] Floodgate ready");
  }
}

async function getSystemRam() {
  try {
    const mem = await fs.readFile("/proc/meminfo", "utf8");
    const totalKb = parseInt(mem.match(/MemTotal:\s+(\d+)/)?.[1] || "0", 10);
    return { totalRamMb: Math.round(totalKb / 1024) };
  } catch (err) {
    log(`[Bridge] Failed to read /proc/meminfo: ${err.message}`);
    return { totalRamMb: 0 };
  }
}

function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], ...opts });
    let stderr = "";
    proc.stdout.on("data", d => String(d).split(/\r?\n/).filter(Boolean).forEach(l => log(`[setup] ${l}`)));
    proc.stderr.on("data", d => { stderr += String(d); String(d).split(/\r?\n/).filter(Boolean).forEach(l => log(`[setup] ${l}`)); });
    proc.on("error", reject);
    proc.on("close", code => code === 0 ? resolve() : reject(new Error(`${cmd} exited ${code}: ${stderr.slice(0, 200)}`)));
  });
}

async function exists(p) { try { await fs.access(p); return true; } catch { return false; } }

async function ensureDirs() {
  for (const d of [MINIHOST_DIR, PAPER_DIR, PLUGINS_DIR, RES_PACKS_DIR, PLAYIT_DIR, BACKUPS_DIR])
    await fs.mkdir(d, { recursive: true });
}

async function loadServerMode() {
  try {
    const data = JSON.parse(await fs.readFile(MODE_FILE, "utf8"));
    if (data.bedrockAuth === "floodgate") state.settings.bedrockAuth = "floodgate";
    if (data.mode === "java" || data.mode === "java_bedrock") { state.settings.mode = data.mode; return; }
  } catch {}
  if (await exists(GEYSER_JAR_PATH)) {
    state.settings.mode = "java_bedrock";
    await saveServerMode("java_bedrock").catch(() => {});
  } else {
    state.settings.mode = null;
  }
}

async function saveServerMode(mode) {
  await fs.mkdir(MINIHOST_DIR, { recursive: true });
  await fs.writeFile(MODE_FILE, JSON.stringify({ mode, bedrockAuth: state.settings.bedrockAuth || "offline" }));
}

async function setupPhAlias() {
  try {
    const bashrc = path.join(HOME, ".bashrc");
    const alias  = `alias mh='pkill -f bridge.mjs 2>/dev/null; sleep 1; cd ~/Mini-Host/termux && node bridge.mjs'`;
    const content = await fs.readFile(bashrc, "utf8").catch(() => "");
    if (!content.includes("alias mh=")) {
      await fs.appendFile(bashrc, `\n${alias}\n`);
      log("[Bridge] Added 'mh' shortcut — type 'mh' anytime to start bridge");
    }
  } catch {}
}

let ensurePlayitReadyPromise = null;
function ensurePlayitReady() {
  if (!ensurePlayitReadyPromise) {
    ensurePlayitReadyPromise = ensurePlayitReadyInner().finally(() => {
      ensurePlayitReadyPromise = null;
    });
  }
  return ensurePlayitReadyPromise;
}

async function ensurePlayitReadyInner() {
  await fs.mkdir(PLAYIT_DIR, { recursive: true });

  let listOutput = "";
  try {
    listOutput = execSync("proot-distro list 2>&1", { encoding: "utf8" });
  } catch (e) {
    listOutput = String(e.stdout || "");
  }
  const debianInstalled = new RegExp(`\\*\\s*${DEBIAN_DISTRO}\\b`).test(listOutput);

  if (!debianInstalled) {
    await installProotDebian();
  }

  let hasPlayitd = false;
  try {
    const r = execSync(`proot-distro login ${DEBIAN_DISTRO} -- which playitd 2>&1`, { encoding: "utf8" });
    hasPlayitd = r.trim().length > 0 && !r.includes("not found");
  } catch {
    hasPlayitd = false;
  }

  if (!hasPlayitd) {
    await installPlayitInDebian();
    try {
      const r2 = execSync(`proot-distro login ${DEBIAN_DISTRO} -- which playitd 2>&1`, { encoding: "utf8" });
      hasPlayitd = r2.trim().length > 0 && !r2.includes("not found");
    } catch {
      hasPlayitd = false;
    }
    if (!hasPlayitd) {
      throw new Error("playitd still not found after install attempt — real failure, not just the known systemd hook");
    }
  }
}

function parsePlayitLine(line) {
  const clean = line
    .replace(/\x1b\[[0-9;]*m/g, "")
    .replace(/[^\x20-\x7E]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!clean) return;

  log(`[playit] ${clean}`);
  let changed = false;

  const connectedMatch = clean.match(/playit connected;\s*tunnels loaded.*?tunnel_count=(\d+).*?account_status="?(\w+)"?/i);
  if (connectedMatch) {
    state.tunnel.state    = "connected";
    state.tunnel.online   = true;
    state.tunnel.claimUrl = "";
    changed = true;
    log(`[playit] Connected — ${connectedMatch[1]} tunnel(s), account ${connectedMatch[2]}`);
  }

  if (/waiting for frontend secret provisioning/i.test(clean)) {
    state.tunnel.state  = "waiting_auth";
    state.tunnel.online = false;
    changed = true;
  }

  if (/configured agent secret is no longer valid|InvalidAgentKey/i.test(clean)) {
    log("[playit] Saved secret was rejected by playit.gg — clearing it.");
    fs.rm(PLAYIT_SECRET_FILE, { force: true }).catch(() => {});
    clearPlayitdConfig().catch(() => {});
    state.tunnel.state  = "waiting_auth";
    state.tunnel.online = false;
    changed = true;
  }

  if (/control session expired; reconnecting/i.test(clean)) {
  }

  if (/network unreachable|failed to (send initial ping|ping tunnel server)/i.test(clean) && state.tunnel.state !== "connected") {
  }

  if (/playitd shutdown complete/i.test(clean)) {
    state.tunnel.online = false;
    if (state.tunnel.state !== "waiting_auth") state.tunnel.state = "disconnected";
    changed = true;
  }

  const mapMatch = clean.match(/([a-z0-9.-]+\.ply\.gg:\d+)\s*(?:=>|->)?\s*[\d.]*:?(\d+)?/i);
  if (mapMatch) {
    const addr = mapMatch[1];
    const localPort = mapMatch[2] ? parseInt(mapMatch[2], 10) : null;
    const isBedrock = localPort != null && (localPort === state.settings.bedrockPort || localPort === 19132);
    const isUdp = /\budp\b/i.test(clean);
    if (isBedrock || isUdp) { state.tunnel.bedrockAddress = addr; log(`[playit] Bedrock → ${addr} (unconfirmed format)`); }
    else                    { state.tunnel.javaAddress    = addr; log(`[playit] Java → ${addr} (unconfirmed format)`); }
    fs.writeFile(PLAYIT_TUNNELS_FILE, JSON.stringify({
      java:    state.tunnel.javaAddress,
      bedrock: state.tunnel.bedrockAddress,
    })).catch(() => {});
    changed = true;
  }

  if (changed) broadcast("tunnel.status", { tunnel: state.tunnel });
}

async function clearPlayitdConfig() {
  try {
    await runCmd("proot-distro", ["login", DEBIAN_DISTRO, "--", "rm", "-rf", "/root/.config/playit_gg"]);
  } catch (err) {
    log(`[playit] Could not clear playitd's internal config (non-fatal): ${err.message}`);
  }
}

async function getSavedSecret() {
  try {
    const s = (await fs.readFile(PLAYIT_SECRET_FILE, "utf8")).trim();
    return s || null;
  } catch {
    return null;
  }
}

async function saveSecret(secret) {
  await fs.mkdir(PLAYIT_DIR, { recursive: true });
  await clearPlayitdConfig();
  await fs.writeFile(PLAYIT_SECRET_FILE, secret.trim());
}

async function startPlayit() {
  if (playitProcess) return;

  try {
    await ensurePlayitReady();
  } catch (err) {
    log(`[playit] Setup failed: ${err.message}`);
    state.tunnel.state  = "error";
    state.tunnel.online = false;
    broadcast("tunnel.status", { tunnel: state.tunnel });
    return;
  }

  const secret = await getSavedSecret();
  if (!secret) {
    log("[playit] No secret key saved yet — waiting for one from the app.");
    state.tunnel.state  = "waiting_auth";
    state.tunnel.online = false;
    broadcast("tunnel.status", { tunnel: state.tunnel });
    return;
  }

  try {
    const t = JSON.parse(await fs.readFile(PLAYIT_TUNNELS_FILE, "utf8"));
    if (t.java)    state.tunnel.javaAddress    = t.java;
    if (t.bedrock) state.tunnel.bedrockAddress = t.bedrock;
  } catch {}

  state.tunnel.state    = "connecting";
  state.tunnel.online   = false;
  state.tunnel.claimUrl = "";
  broadcast("tunnel.status", { tunnel: state.tunnel });
  log("[playit] Starting playitd inside Debian...");

  playitProcess = spawn("proot-distro", ["login", DEBIAN_DISTRO, "--", "playitd", "--secret", secret], {
    stdio:    ["ignore", "pipe", "pipe"],
    detached: true,
  });

  const handleLine = (l) => {
    const cl = l.replace(/\x1b\[[0-9;]*m/g, "").trim();
    if (cl) parsePlayitLine(cl);
  };
  playitProcess.stdout.on("data", chunk => String(chunk).split(/\r?\n/).filter(Boolean).forEach(handleLine));
  playitProcess.stderr.on("data", chunk => String(chunk).split(/\r?\n/).filter(Boolean).forEach(handleLine));

  const stuckTimer = setTimeout(() => {
    if (state.tunnel.state === "connecting") {
      log("[playit] No connection after 25s. Check your internet connection, and that the secret key entered in the Tunnel tab is correct (get a new one at playit.gg/account/setup/wizard).");
      state.tunnel.state = "error";
      broadcast("tunnel.status", { tunnel: state.tunnel });
    }
  }, 25000);

  playitProcess.on("error", err => {
    clearTimeout(stuckTimer);
    log(`[playit] Spawn error: ${err.message}`);
    state.tunnel.state  = "error";
    state.tunnel.online = false;
    broadcast("tunnel.status", { tunnel: state.tunnel });
    playitProcess = null;
    schedulePlayitRestart();
  });

  playitProcess.on("close", code => {
    clearTimeout(stuckTimer);
    log(`[playit] Exited (${code ?? 0})`);
    playitProcess = null;
    if (state.tunnel.state !== "waiting_auth") {
      state.tunnel.state = (code === 0) ? "disconnected" : "error";
    }
    state.tunnel.online = false;
    broadcast("tunnel.status", { tunnel: state.tunnel });
    if (serverProcess) schedulePlayitRestart();
  });
}

async function schedulePlayitRestart() {
  if (playitRestartTimer) return;
  playitRestartTimer = setTimeout(async () => {
    playitRestartTimer = null;
    if (!serverProcess) return;
    startPlayit();
  }, 30000);
}

async function stopPlayit() {
  if (playitRestartTimer) { clearTimeout(playitRestartTimer); playitRestartTimer = null; }
  if (playitProcess) {
    const pid = playitProcess.pid;
    try {
      process.kill(-pid, "SIGINT");
    } catch {
      try { playitProcess.kill("SIGINT"); } catch {}
    }
    playitProcess = null;
  }
  state.tunnel.online   = false;
  state.tunnel.state    = "disconnected";
  state.tunnel.claimUrl = "";
  broadcast("tunnel.status", { tunnel: state.tunnel });
}

async function getAllPaperVersionsSorted() {
  const data   = await wgetJson(PAPER_API_BASE);
  const groups = data.versions || {};
  const all    = Object.values(groups).flat();
  if (!all.length) throw new Error("No Paper versions returned by Fill API");
  all.sort(compareVersions);
  return all;
}

async function tryGetStablePaperBuild(version) {
  try {
    const builds = await wgetJson(`${PAPER_API_BASE}/versions/${version}/builds`);
    const stable = (Array.isArray(builds) ? builds : []).filter(b => b.channel === "STABLE");
    if (!stable.length) return null;
    stable.sort((a, b) => b.id - a.id);
    const top = stable[0];
    const dl  = top.downloads && top.downloads["server:default"];
    if (!dl || !dl.url) return null;
    return { version, build: top.id, url: dl.url, sha256: dl.checksums?.sha256 || "" };
  } catch {
    return null;
  }
}

async function getLatestStablePaper() {
  const all = await getAllPaperVersionsSorted();
  for (let i = all.length - 1; i >= 0; i--) {
    const result = await tryGetStablePaperBuild(all[i]);
    if (result) {
      if (i !== all.length - 1) {
        log(`[Bridge] Newest version ${all[all.length - 1]} has no stable build yet — using ${all[i]} instead`);
      }
      return result;
    }
  }
  throw new Error("No Paper version with a stable build was found");
}

async function getPaperVersionList() {
  try {
    const all = await getAllPaperVersionsSorted();
    const latest20 = all.slice(-20).reverse();
    return { versions: latest20.map(v => ({ tag: v, name: `Paper ${v}`, url: "", mcVersion: v, published: "" })) };
  } catch {
    return { versions: [] };
  }
}

async function writeGeyserConfig(force = false) {
  const dir = path.join(PLUGINS_DIR, "Geyser-Spigot");
  const cfg = path.join(dir, "config.yml");
  await fs.mkdir(dir, { recursive: true });
  if (!force && await exists(cfg)) return;
  const config = [
    `bedrock:`, `  address: 0.0.0.0`, `  port: ${state.settings.bedrockPort}`,
    `  clone-remote-port: false`, `  motd1: "${state.settings.serverName || "Mini Host"}"`,
    `  motd2: "${state.settings.motd || "Powered by Mini Host"}"`,
    `  server-name: "${state.settings.serverName || "Mini Host"}"`,
    `remote:`, `  address: 127.0.0.1`,
    `  port: ${state.settings.port}`, `  auth-type: ${state.settings.bedrockAuth === "floodgate" ? "floodgate" : "offline"}`,
    `metrics:`, `  enabled: false`, `  uuid: 00000000-0000-0000-0000-000000000000`,
  ].join("\n") + "\n";
  await fs.writeFile(cfg, config);
}

async function initServerProperties() {
  if (await exists(SERVER_PROPERTIES_PATH)) return;
  const lines = [
    `online-mode=false`, `gamemode=${state.settings.gamemode}`,
    `difficulty=${state.settings.difficulty}`, `max-players=${state.settings.maxPlayers}`,
    `server-port=${state.settings.port}`, `motd=${state.settings.motd}`,
    `view-distance=${state.settings.viewDistance}`, `pvp=${state.settings.pvp}`,
    `allow-flight=${state.settings.allowFlight}`, `white-list=${state.settings.whiteList}`,
    `level-name=world`, `spawn-protection=0`, `enable-rcon=false`, `enable-command-block=true`,
  ];
  await fs.writeFile(SERVER_PROPERTIES_PATH, lines.join("\n") + "\n");
}

async function removeOldViaJars() {
  const entries = await fs.readdir(PLUGINS_DIR).catch(() => []);
  for (const f of entries) {
    if (f === "ViaVersion.jar" || f === "ViaBackwards.jar") continue;
    if (/^ViaVersion-.*\.jar$/.test(f) || /^ViaBackwards-.*\.jar$/.test(f)) {
      await fs.rm(path.join(PLUGINS_DIR, f), { force: true, recursive: true });
      log(`[Bridge] Removed old Via: ${f}`);
    }
  }
}

let JAVA_BIN = "java";

function termuxJvmPath(majorVersion) {
  const prefix = process.env.PREFIX || "/data/data/com.termux/files/usr";
  return path.join(prefix, "lib/jvm", `java-${majorVersion}-openjdk`, "bin", "java");
}

async function ensureJavaAtLeast(majorVersion) {
  const jvmPath = termuxJvmPath(majorVersion);
  if (await exists(jvmPath)) { JAVA_BIN = jvmPath; return true; }
  log(`[Bridge] This Paper build needs Java ${majorVersion}+ — installing openjdk-${majorVersion} via pkg...`);
  try {
    await runCmd("pkg", ["install", "-y", `openjdk-${majorVersion}`]);
  } catch (err) {
    log(`[Bridge] pkg install openjdk-${majorVersion} failed: ${err.message}`);
    return false;
  }
  if (await exists(jvmPath)) {
    JAVA_BIN = jvmPath;
    log(`[Bridge] openjdk-${majorVersion} installed successfully`);
    return true;
  }
  log(`[Bridge] pkg reported success but ${jvmPath} still doesn't exist`);
  return false;
}

async function installPaper() {
  await ensureDirs();
  try { execSync(`"${JAVA_BIN}" -version`, { stdio: "ignore" }); }
  catch { throw new Error("Java not found. Run: pkg install openjdk-21"); }

  if (!(await exists(PAPER_JAR_PATH))) {
    log("[Bridge] Finding latest stable Paper version...");
    const { version, build, url } = await getLatestStablePaper();
    log(`[Bridge] Downloading Paper ${version} build ${build}...`);
    await wget(url, PAPER_JAR_PATH, ["--header", `User-Agent: ${PAPER_USER_AGENT}`]);
    log(`[Bridge] Paper ${version} ready`);
  }

  if (state.settings.mode === "java_bedrock") {
    if (!(await exists(GEYSER_JAR_PATH))) {
      log("[Bridge] Downloading Geyser-Spigot (Bedrock support)...");
      await wget(GEYSER_URL, GEYSER_JAR_PATH);
      log("[Bridge] Geyser-Spigot ready");
    }
    await ensureViaJars();
    if (state.settings.bedrockAuth === "floodgate") await ensureFloodgateJar();
    await writeGeyserConfig();
  } else {
    log("[Bridge] Java-only mode — skipping Geyser/Bedrock download");
  }

  await fs.writeFile(EULA_PATH, "eula=true\n");
  await initServerProperties();
}

async function getWorlds() {
  await ensureDirs();
  const worlds = [];
  const KNOWN  = ["world", "world_nether", "world_the_end"];
  const entries = await fs.readdir(PAPER_DIR, { withFileTypes: true }).catch(() => []);
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const wp         = path.join(PAPER_DIR, e.name);
    const isKnown    = KNOWN.includes(e.name);
    const hasLevelDat = await exists(path.join(wp, "level.dat"));
    if (!isKnown && !hasLevelDat) continue;
    let sizeMb = 1;
    try {
      const r = execSync(`du -sm "${wp}" 2>/dev/null`, { encoding: "utf8" });
      sizeMb = Math.max(1, parseInt(r.trim().split("\t")[0], 10) || 1);
    } catch {}
    worlds.push({
      id:            `world_${e.name}`,
      name:          e.name,
      folder:        e.name,
      sizeMb,
      lastPlayedIso: new Date().toISOString(),
    });
  }
  state.worlds = worlds;
  return worlds;
}

let hasTar = null;
async function ensureTar() {
  if (hasTar) return true;
  try { execSync("which tar", { stdio: "ignore" }); hasTar = true; return true; } catch {}
  log("[Bridge] tar not found — installing via pkg...");
  try {
    await runCmd("pkg", ["install", "-y", "tar"]);
    execSync("which tar", { stdio: "ignore" });
    hasTar = true;
    return true;
  } catch (err) {
    log(`[Bridge] Could not install tar: ${err.message}`);
    return false;
  }
}

const MAX_BACKUPS_PER_WORLD = 5;

function parseBackupFilename(f) {
  const m = f.match(/^(.+)__(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z)\.tar\.gz$/);
  if (!m) return null;
  return { folder: m[1], isoSafe: m[2], createdIso: m[2].replace(/T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z$/, "T$1:$2:$3.$4Z") };
}

async function getBackups() {
  await ensureDirs();
  const entries = await fs.readdir(BACKUPS_DIR).catch(() => []);
  const backups = [];
  for (const f of entries) {
    const parsed = parseBackupFilename(f);
    if (!parsed) continue;
    let sizeMb = 0;
    try {
      const st = await fs.stat(path.join(BACKUPS_DIR, f));
      sizeMb = Math.max(1, Math.round(st.size / (1024 * 1024)));
    } catch {}
    backups.push({ id: f, folder: parsed.folder, createdIso: parsed.createdIso, sizeMb });
  }
  backups.sort((a, b) => b.createdIso.localeCompare(a.createdIso));
  return backups;
}

async function backupWorld(folder) {
  if (!(await ensureTar())) throw new Error("tar is not available and could not be installed");
  const worldPath = path.join(PAPER_DIR, folder);
  if (!(await exists(worldPath))) throw new Error(`World folder not found: ${folder}`);

  const isoSafe = new Date().toISOString().replace(/:/g, "-").replace(/\.(\d{3})Z$/, "-$1Z");
  const filename = `${folder}__${isoSafe}.tar.gz`;
  await fs.mkdir(BACKUPS_DIR, { recursive: true });
  log(`[Bridge] Backing up world '${folder}'...`);
  await runCmd("tar", ["-czf", path.join(BACKUPS_DIR, filename), "-C", PAPER_DIR, folder]);
  log(`[Bridge] Backup saved: ${filename}`);

  const all = (await getBackups()).filter(b => b.folder === folder);
  for (const old of all.slice(MAX_BACKUPS_PER_WORLD)) {
    await fs.rm(path.join(BACKUPS_DIR, old.id), { force: true }).catch(() => {});
    log(`[Bridge] Pruned old backup: ${old.id}`);
  }
  return { id: filename, folder, createdIso: new Date().toISOString() };
}

async function backupAllWorlds() {
  const worlds = await getWorlds();
  const results = [];
  for (const w of worlds) {
    try { results.push(await backupWorld(w.folder)); }
    catch (err) { log(`[Bridge] Backup of '${w.folder}' failed (continuing): ${err.message}`); }
  }
  return results;
}

async function restoreBackup(backupId) {
  const parsed = parseBackupFilename(backupId);
  if (!parsed) throw new Error("Invalid backup id");
  const archivePath = path.join(BACKUPS_DIR, backupId);
  if (!(await exists(archivePath))) throw new Error("Backup not found");
  if (!(await ensureTar())) throw new Error("tar is not available and could not be installed");

  if (serverProcess) {
    log("[Bridge] Stopping server before restoring backup...");
    await stopServer();
  }

  const worldPath = path.join(PAPER_DIR, parsed.folder);
  log(`[Bridge] Restoring '${parsed.folder}' from ${backupId}...`);
  await fs.rm(worldPath, { recursive: true, force: true });
  await runCmd("tar", ["-xzf", archivePath, "-C", PAPER_DIR]);
  log(`[Bridge] Restore complete: '${parsed.folder}'`);

  await getWorlds();
  broadcast("worlds.update", { worlds: state.worlds });
  return { restored: parsed.folder };
}

async function deleteBackup(backupId) {
  if (!parseBackupFilename(backupId)) throw new Error("Invalid backup id");
  await fs.rm(path.join(BACKUPS_DIR, backupId), { force: true });
  log(`[Bridge] Backup deleted: ${backupId}`);
  return { deleted: backupId };
}

async function getPacks() {
  await ensureDirs();
  const packs   = [];
  const entries = await fs.readdir(RES_PACKS_DIR, { withFileTypes: true }).catch(() => []);
  for (const e of entries) {
    if (!e.isFile() && !e.isDirectory()) continue;
    if (e.isFile() && !e.name.endsWith(".zip")) continue;
    packs.push({ id: `resource_${e.name}`, name: e.name.replace(/\.zip$/, ""), type: "resource", version: "1.0.0", folder: e.name, uuid: "" });
  }
  state.packs = packs;
  return packs;
}

const MAX_PACK_UPLOAD_MB = 60;

async function packInstallFromUpload(filename, dataBase64) {
  const safeName = path.basename(String(filename || "").trim());
  if (!safeName || !safeName.toLowerCase().endsWith(".zip")) {
    throw new Error("Resource packs must be a .zip file");
  }
  if (!dataBase64) throw new Error("No file data received");

  const buf = Buffer.from(dataBase64, "base64");
  if (buf.length === 0) throw new Error("Uploaded file is empty");
  if (buf.length > MAX_PACK_UPLOAD_MB * 1024 * 1024) {
    throw new Error(`Pack is too large (max ${MAX_PACK_UPLOAD_MB} MB)`);
  }
  if (buf[0] !== 0x50 || buf[1] !== 0x4b) {
    throw new Error("That file isn't a valid .zip resource pack");
  }

  await fs.mkdir(RES_PACKS_DIR, { recursive: true });
  const dest = path.join(RES_PACKS_DIR, safeName);
  await fs.writeFile(dest, buf);
  log(`[Bridge] Resource pack added: ${safeName} (${Math.round(buf.length / (1024 * 1024))} MB)`);

  await getPacks();
  broadcast("packs.update", { packs: state.packs });
  return { installed: safeName };
}

async function pluginList() {
  await fs.mkdir(PLUGINS_DIR, { recursive: true });
  const entries = await fs.readdir(PLUGINS_DIR).catch(() => []);
  return { plugins: entries.filter(f => f.endsWith(".jar")).map(f => ({ name: f.replace(/\.jar$/, ""), file: f })) };
}

async function pluginInstall(url, name) {
  await fs.mkdir(PLUGINS_DIR, { recursive: true });
  const filename = name.endsWith(".jar") ? name : name + ".jar";
  const dest     = path.join(PLUGINS_DIR, filename);
  log(`[Bridge] Downloading plugin: ${filename}`);
  await wget(url, dest).catch(async (err) => {
    await fs.rm(dest, { force: true });
    throw new Error(`Download failed: ${err.message}`);
  });
  try {
    const buf = Buffer.alloc(4);
    const fd  = await fs.open(dest, "r");
    await fd.read(buf, 0, 4, 0);
    await fd.close();
    if (buf[0] !== 0x50 || buf[1] !== 0x4B) {
      await fs.rm(dest, { force: true });
      throw new Error("URL did not return a JAR file — use a direct .jar download link, not a webpage URL");
    }
  } catch (e) {
    if (e.message.includes("direct .jar")) throw e;
    throw new Error(`Could not verify JAR: ${e.message}`);
  }
  log(`[Bridge] Plugin installed: ${filename}`);
  return { installed: filename };
}

async function pluginDelete(name) {
  const filename = name.endsWith(".jar") ? name : name + ".jar";
  await fs.rm(path.join(PLUGINS_DIR, filename), { force: true, recursive: true });
  log(`[Bridge] Plugin deleted: ${filename}`);
  return { deleted: filename };
}

const MODRINTH_API = "https://api.modrinth.com/v2";

async function pluginSearch(query, projectType) {
  const type = projectType === "mod" ? "mod" : "plugin";
  const facets = encodeURIComponent(JSON.stringify([[`project_type:${type}`]]));
  const q = encodeURIComponent(query || "");
  const data = await wgetJson(`${MODRINTH_API}/search?query=${q}&facets=${facets}&limit=20&index=relevance`);
  return (data.hits || []).map(h => ({
    id:          h.project_id,
    slug:        h.slug || h.project_id,
    title:       h.title,
    description: h.description || "",
    iconUrl:     h.icon_url || "",
    downloads:   h.downloads || 0,
  }));
}

async function modrinthResolveDownload(slug, projectType) {
  const loaders = projectType === "mod" ? ["fabric"] : ["paper", "spigot", "bukkit"];
  const loadersParam = encodeURIComponent(JSON.stringify(loaders));
  const versions = await wgetJson(`${MODRINTH_API}/project/${encodeURIComponent(slug)}/version?loaders=${loadersParam}`);
  if (!Array.isArray(versions) || !versions.length) {
    throw new Error("No version of this plugin/mod is compatible with this server");
  }
  for (const v of versions) {
    const file = (v.files || []).find(f => f.primary) || v.files?.[0];
    if (file && file.url && file.filename) return { url: file.url, filename: file.filename };
  }
  throw new Error("No downloadable file found for this plugin/mod");
}

async function pluginInstallFromModrinth(slug, projectType) {
  const { url, filename } = await modrinthResolveDownload(slug, projectType);
  return pluginInstall(url, filename);
}

function isSafePath(p) { return path.resolve(p).startsWith(path.resolve(ALLOWED_BASE)); }

async function fileList(dir) {
  const target = dir ? path.join(MINIHOST_DIR, dir) : MINIHOST_DIR;
  if (!isSafePath(target)) throw new Error("Access denied");
  const entries = await fs.readdir(target, { withFileTypes: true }).catch(() => []);
  return {
    dir:   target.replace(HOME, "~"),
    files: entries.map(e => ({
      name: e.name,
      type: e.isDirectory() ? "dir" : "file",
      path: path.join(target, e.name).replace(HOME, "~"),
    })),
  };
}

async function fileRead(filePath) {
  const resolved = filePath.startsWith("~") ? filePath.replace("~", HOME) : filePath;
  if (!isSafePath(resolved)) throw new Error("Access denied");
  return { path: filePath, content: await fs.readFile(resolved, "utf8") };
}

async function fileWrite(filePath, content) {
  const resolved = filePath.startsWith("~") ? filePath.replace("~", HOME) : filePath;
  if (!isSafePath(resolved)) throw new Error("Access denied");
  try { await fs.copyFile(resolved, resolved + ".bak"); } catch {}
  await fs.writeFile(resolved, content, "utf8");
  log(`[Bridge] Saved ${filePath}`);
  return { path: filePath, saved: true };
}

async function shellExec(cmd) {
  return new Promise(resolve => {
    const proc = spawn("bash", ["-c", cmd], { stdio: ["ignore", "pipe", "pipe"] });
    let out = "", err = "", done = false;
    const finish = (v) => { if (done) return; done = true; clearTimeout(t); resolve(v); };
    proc.stdout.on("data", d => { out += String(d); });
    proc.stderr.on("data", d => { err += String(d); });
    proc.on("close", code => finish({ output: (out + err).slice(0, 8000), code }));
    proc.on("error", e  => finish({ output: e.message, code: -1 }));
    const t = setTimeout(() => {
      try { proc.kill("SIGKILL"); } catch {}
      finish({ output: (out + err + "\n[timeout 15s]").slice(0, 8000), code: -1 });
    }, 15000);
  });
}

async function switchPaperVersion(mcVersion) {
  log(`[Bridge] Switching to Paper ${mcVersion}...`);
  if (serverProcess) {
    log("[Bridge] Stopping server before version switch...");
    await stopServer();
  }
  await backupAllWorlds().catch(err => log(`[Bridge] Pre-switch backup failed (continuing): ${err.message}`));
  let result = await tryGetStablePaperBuild(mcVersion);
  if (!result) {
    log(`[Bridge] ${mcVersion} has no stable build — finding the closest version that does...`);
    result = await getLatestStablePaper();
  }
  await fs.rm(PAPER_JAR_PATH, { force: true });
  await wget(result.url, PAPER_JAR_PATH, ["--header", `User-Agent: ${PAPER_USER_AGENT}`]);
  log(`[Bridge] Switched to Paper ${result.version} build ${result.build}`);

  let bedrockResynced = false;
  if (state.settings.mode === "java_bedrock") {
    if (!(await exists(GEYSER_JAR_PATH))) {
      log("[Bridge] Re-downloading Geyser-Spigot after version switch...");
      await wget(GEYSER_URL, GEYSER_JAR_PATH);
    }
    await ensureViaJars();
    if (state.settings.bedrockAuth === "floodgate") await ensureFloodgateJar();
    await writeGeyserConfig(true);
    bedrockResynced = true;
    log("[Bridge] Bedrock support (Geyser/ViaVersion/ViaBackwards" + (state.settings.bedrockAuth === "floodgate" ? "/Floodgate" : "") + ") re-synced for the new version");
  }

  return {
    switched:      result.version,
    requested:     mcVersion,
    fellBack:      result.version !== mcVersion,
    bedrockResynced,
  };
}

async function updateServerProperties(s) {
  let content = "";
  try { content = await fs.readFile(SERVER_PROPERTIES_PATH, "utf8"); } catch {}
  const map = new Map();
  for (const line of content.split(/\r?\n/)) {
    if (!line || line.startsWith("#") || !line.includes("=")) continue;
    const [k, ...v] = line.split("=");
    map.set(k.trim(), v.join("=").trim());
  }
  if (s.gamemode)                       map.set("gamemode",      s.gamemode);
  if (s.difficulty)                     map.set("difficulty",    s.difficulty);
  if (typeof s.maxPlayers === "number") map.set("max-players",   String(s.maxPlayers));
  if (typeof s.port === "number")       map.set("server-port",   String(s.port));
  if (s.motd)                           map.set("motd",          s.motd);
  if (typeof s.onlineMode === "boolean")map.set("online-mode",   String(s.onlineMode));
  if (typeof s.viewDistance === "number")map.set("view-distance",String(s.viewDistance));
  if (typeof s.pvp === "boolean")       map.set("pvp",           String(s.pvp));
  if (typeof s.allowFlight === "boolean")map.set("allow-flight", String(s.allowFlight));
  if (typeof s.whiteList === "boolean") map.set("white-list",    String(s.whiteList));
  await fs.writeFile(SERVER_PROPERTIES_PATH, [...map.entries()].map(([k, v]) => `${k}=${v}`).join("\n") + "\n");
}

function parseLogLine(line) {
  const clean  = line.replace(/^\[[\d:]+\]\s*\[[\w /]+\]:\s*/, "").replace(/^\[[\d:]+\]\s*/, "");
  const joined = clean.match(/^([A-Za-z0-9_]{3,16}) joined the game/i);
  if (joined) {
    const name = joined[1];
    if (!state.players.find(p => p.name === name)) {
      state.players.push({ id: `player_${name}`, name, ping: 0 });
      broadcast("players.update", { players: state.players });
    }
  }
  const left = clean.match(/^([A-Za-z0-9_]{3,16}) left the game/i);
  if (left) {
    state.players = state.players.filter(p => p.name !== left[1]);
    broadcast("players.update", { players: state.players });
  }
  const tps = line.match(/TPS from last.*?(\d+\.\d+)/i);
  if (tps) { state.stats.tps = Math.round(Number(tps[1]) * 10) / 10; broadcast("stats.update", { stats: state.stats }); }
}

function startStatsLoop() {
  if (statsInterval) return;
  let lastCpuTotal = 0, lastCpuIdle = 0;

  statsInterval = setInterval(async () => {
    if (!serverProcess || !state.startedAtMs) {
      state.stats = { cpuPercent: 0, ramUsedMb: 0, ramLimitMb: state.settings.ramMb, tps: 20, uptimeSeconds: 0 };
      return;
    }
    state.stats.uptimeSeconds = Math.floor((Date.now() - state.startedAtMs) / 1000);

    try {
      const stat    = await fs.readFile("/proc/stat", "utf8");
      const cpuLine = stat.split("\n")[0];
      const nums    = cpuLine.trim().split(/\s+/).slice(1).map(Number);
      const total   = nums.reduce((a, b) => a + b, 0);
      const idle    = (nums[3] || 0) + (nums[4] || 0);
      const dtotal  = total - lastCpuTotal;
      const didle   = idle  - lastCpuIdle;
      if (dtotal > 0) state.stats.cpuPercent = Math.min(100, Math.max(0, Math.round((1 - didle / dtotal) * 100)));
      lastCpuTotal = total;
      lastCpuIdle  = idle;
    } catch {}

    try {
      const mem  = await fs.readFile("/proc/meminfo", "utf8");
      const val  = (key) => { const m = mem.match(new RegExp(key + ":\\s+(\\d+)")); return m ? parseInt(m[1], 10) : 0; };
      const totalKb = val("MemTotal");
      const availKb = val("MemAvailable");
      state.stats.ramUsedMb  = Math.round((totalKb - availKb) / 1024);
      state.stats.ramLimitMb = state.settings.ramMb;
    } catch {}

    broadcast("stats.update", { stats: state.stats });
  }, 2000);
}

function stopStatsLoop() { if (statsInterval) { clearInterval(statsInterval); statsInterval = null; } }

async function startServer() {
  if (serverProcess || state.serverStatus === "installing") return snapshot();
  if (state.settings.mode !== "java" && state.settings.mode !== "java_bedrock") {
    throw new Error("MODE_NOT_SET");
  }
  state.serverStatus = "installing";
  broadcast("server.status", { status: "installing" });
  log("[Bridge] Checking installation...");

  installPaper().then(() => {
    state.serverStatus     = "starting";
    state.startedAtMs      = 0;
    state.stats.ramUsedMb  = 0;
    broadcast("server.status", { status: "starting" });

    let javaRequirementLine = null;
    let hasRetriedForJava   = false;

    function launchPaper() {
      const javaArgs = [
        `-Xmx${state.settings.ramMb}M`, `-Xms256M`,
        `-XX:+UseG1GC`, `-XX:+ParallelRefProcEnabled`, `-XX:MaxGCPauseMillis=200`,
        `-jar`, `paper.jar`, `--nogui`,
      ];
      log(`[Bridge] Starting Paper (${JAVA_BIN})...`);
      javaRequirementLine = null;
      serverProcess = spawn(JAVA_BIN, javaArgs, { cwd: PAPER_DIR, stdio: ["pipe", "pipe", "pipe"] });
      startStatsLoop();

      serverProcess.stdout.on("data", chunk => {
        String(chunk).split(/\r?\n/).map(l => l.trim()).filter(Boolean).forEach(line => {
          log(line);
          parseLogLine(line);

          if (/playit/i.test(line)) {
            if (/https:\/\/playit\.gg\/claim\//i.test(line) || /ply\.gg:\d+/i.test(line)) {
              parsePlayitLine(line);
            }
          }

          if (state.serverStatus !== "running" && READY_PATTERNS.some(p => p.test(line))) {
            state.serverStatus     = "running";
            state.startedAtMs      = Date.now();
            state.stats.tps        = 20;
            broadcast("server.status", { status: "running" });
            broadcast("stats.update",  { stats: state.stats });
            startPlayit();
          }
        });
      });

      serverProcess.stderr.on("data", chunk => {
        String(chunk).split(/\r?\n/).map(l => l.trim()).filter(Boolean).forEach(l => {
          log(`[err] ${l}`);
          const m = l.match(/requires running the server with java (\d+)/i);
          if (m) javaRequirementLine = parseInt(m[1], 10);
        });
      });

      serverProcess.on("error", err => {
        log(`[Bridge] Spawn error: ${err.message}`);
        serverProcess = null; state.serverStatus = "stopped";
        broadcast("server.status", { status: "stopped" });
        stopStatsLoop(); stopPlayit();
      });

      serverProcess.on("close", code => {
        if (code !== 0 && javaRequirementLine && !hasRetriedForJava) {
          hasRetriedForJava = true;
          const needed = javaRequirementLine;
          serverProcess = null;
          stopStatsLoop();
          log(`[Bridge] Paper requires Java ${needed}+ — attempting automatic upgrade...`);
          ensureJavaAtLeast(needed).then(ok => {
            if (ok) {
              log(`[Bridge] Retrying server start with Java ${needed}...`);
              launchPaper();
            } else {
              log(`[Bridge] Could not install Java ${needed} automatically. Run manually in Termux: pkg install openjdk-${needed}`);
              state.serverStatus = "stopped";
              broadcast("server.status", { status: "stopped" });
            }
          });
          return;
        }

        log(`[Bridge] Paper exited (${code ?? 0})`);
        serverProcess = null; state.serverStatus = "stopped";
        state.players = []; state.startedAtMs = 0;
        state.stats = { cpuPercent: 0, ramUsedMb: 0, ramLimitMb: state.settings.ramMb, tps: 20, uptimeSeconds: 0 };
        broadcast("server.status",  { status: "stopped" });
        broadcast("players.update", { players: [] });
        broadcast("stats.update",   { stats: state.stats });
        stopStatsLoop(); stopPlayit();
      });
    }

    launchPaper();
  }).catch(err => {
    log(`[Bridge] Install failed: ${err.message}`);
    state.serverStatus = "stopped";
    broadcast("server.status", { status: "stopped" });
  });

  return snapshot();
}

async function stopServer() {
  if (!serverProcess) { state.serverStatus = "stopped"; return snapshot(); }
  state.serverStatus = "stopping";
  broadcast("server.status", { status: "stopping" });
  log("[Bridge] Stopping...");
  const proc = serverProcess;
  try { proc.stdin.write("stop\n"); } catch {}
  await stopPlayit();
  await new Promise(resolve => {
    let done = false;
    const finish = () => { if (!done) { done = true; resolve(); } };
    proc.once("close", finish);
    proc.once("exit",  finish);
    setTimeout(() => {
      if (!done) { log("[Bridge] Paper slow to stop — sending SIGTERM"); try { proc.kill("SIGTERM"); } catch {} }
    }, 25000);
    setTimeout(() => {
      if (!done) { log("[Bridge] Paper unresponsive — SIGKILL"); try { proc.kill("SIGKILL"); } catch {} }
      finish();
    }, 30000);
  });
  return snapshot();
}

async function restartServer() {
  await stopServer();
  await new Promise(r => setTimeout(r, 1500));
  return startServer();
}

async function sendCommand(cmd) {
  if (!serverProcess) { log("[Bridge] Server not running"); return snapshot(); }
  try { serverProcess.stdin.write(`${cmd}\n`); } catch {}
  log(`> ${cmd}`);
  return snapshot();
}

async function deleteWorld(worldId) {
  const folder = worldId.replace(/^world_/, "");
  await fs.rm(path.join(PAPER_DIR, folder), { recursive: true, force: true });
  log(`[Bridge] World '${folder}' deleted`);
  await getWorlds(); broadcast("worlds.update", { worlds: state.worlds });
  return snapshot();
}

async function deletePack(packId) {
  const folder = packId.replace(/^resource_/, "");
  await fs.rm(path.join(RES_PACKS_DIR, folder), { recursive: true, force: true });
  await getPacks(); broadcast("packs.update", { packs: state.packs });
  return snapshot();
}

async function setServerMode(mode) {
  if (mode !== "java" && mode !== "java_bedrock") throw new Error(`Invalid mode: ${mode}`);

  const wasBedrock = state.settings.mode === "java_bedrock";
  const nowBedrock = mode === "java_bedrock";

  if (wasBedrock === nowBedrock) {
    state.settings.mode = mode;
    await saveServerMode(mode);
    return snapshot();
  }

  if (serverProcess) {
    log("[Bridge] Stopping server before changing mode...");
    await stopServer();
  }

  await backupAllWorlds().catch(err => log(`[Bridge] Pre-switch backup failed (continuing): ${err.message}`));

  state.settings.mode = mode;
  await saveServerMode(mode);

  await fs.mkdir(PLUGINS_DIR, { recursive: true });

  if (nowBedrock) {
    log("[Bridge] Enabling Bedrock support — downloading Geyser + ViaVersion/ViaBackwards...");
    if (!(await exists(GEYSER_JAR_PATH))) await wget(GEYSER_URL, GEYSER_JAR_PATH);
    await ensureViaJars();
    if (state.settings.bedrockAuth === "floodgate") await ensureFloodgateJar();
    await writeGeyserConfig(true);
    log("[Bridge] Bedrock support ready — start the server to activate");
  } else {
    log("[Bridge] Disabling Bedrock support — removing Geyser + Via + Floodgate plugins...");
    await fs.rm(GEYSER_JAR_PATH, { force: true });
    await fs.rm(VIAVERSION_JAR_PATH, { force: true });
    await fs.rm(VIABACKWARDS_JAR_PATH, { force: true });
    await fs.rm(FLOODGATE_JAR_PATH, { force: true });
    await fs.rm(path.join(PLUGINS_DIR, "Geyser-Spigot"), { force: true, recursive: true });
    log("[Bridge] Bedrock support removed — Java-only from next start");
  }

  broadcast("settings.update", { settings: state.settings });
  return snapshot();
}

async function setBedrockAuth(authType) {
  if (authType !== "offline" && authType !== "floodgate") throw new Error(`Invalid Bedrock auth type: ${authType}`);
  if (state.settings.mode !== "java_bedrock") throw new Error("Bedrock support isn't enabled — turn on Java + Bedrock mode first");
  if (state.settings.bedrockAuth === authType) return snapshot();

  if (serverProcess) {
    log("[Bridge] Stopping server before changing Bedrock auth mode...");
    await stopServer();
  }

  state.settings.bedrockAuth = authType;
  await saveServerMode(state.settings.mode);
  await fs.mkdir(PLUGINS_DIR, { recursive: true });

  if (authType === "floodgate") {
    await ensureFloodgateJar();
  } else {
    await fs.rm(FLOODGATE_JAR_PATH, { force: true });
  }
  await writeGeyserConfig(true);
  log(`[Bridge] Bedrock auth set to "${authType}" — start/restart the server to apply`);

  broadcast("settings.update", { settings: state.settings });
  return snapshot();
}

async function applySettings(payload) {
  const old = { ...state.settings };
  state.settings      = { ...state.settings, ...payload };
  state.stats.ramLimitMb = state.settings.ramMb;
  await updateServerProperties(state.settings).catch(() => {});
  if (state.settings.bedrockPort !== old.bedrockPort || state.settings.port !== old.port
      || state.settings.serverName !== old.serverName || state.settings.motd !== old.motd) {
    await writeGeyserConfig(true).catch(err => log(`[Bridge] Geyser config rewrite failed: ${err.message}`));
    log("[Bridge] Geyser config updated — restart server to apply");
  }
  log("[Bridge] Settings saved");
  broadcast("settings.update", { settings: state.settings });
  return snapshot();
}

async function handleRequest(action, payload = {}) {
  switch (action) {
    case "server.start":           return startServer();
    case "server.stop":            return stopServer();
    case "server.restart":         return restartServer();
    case "server.command":         return sendCommand(String(payload.command || ""));
    case "server.settings.update": return applySettings(payload);
    case "server.mode.switch":     return setServerMode(String(payload.mode || ""));
    case "bedrock.auth.set":       return setBedrockAuth(String(payload.authType || ""));
    case "system.ram":             return getSystemRam();
    case "server.snapshot":        await getWorlds(); await getPacks(); return snapshot();
    case "world.delete":           return deleteWorld(String(payload.worldId || ""));
    case "world.backup":           return backupWorld(String(payload.folder || ""));
    case "world.backups.list":     return { backups: await getBackups() };
    case "world.backup.restore":   return restoreBackup(String(payload.backupId || ""));
    case "world.backup.delete":    return deleteBackup(String(payload.backupId || ""));
    case "pack.delete":            return deletePack(String(payload.packId || ""));
    case "pack.refresh":           await getPacks(); broadcast("packs.update", { packs: state.packs }); return snapshot();
    case "pack.upload":            return packInstallFromUpload(String(payload.filename || ""), String(payload.dataBase64 || ""));
    case "plugin.list":            return pluginList();
    case "plugin.install":         return pluginInstall(String(payload.url || ""), String(payload.name || ""));
    case "plugin.delete":          return pluginDelete(String(payload.name || ""));
    case "plugin.search":          return { results: await pluginSearch(String(payload.query || ""), String(payload.type || "plugin")) };
    case "plugin.install.modrinth": return pluginInstallFromModrinth(String(payload.slug || ""), String(payload.type || "plugin"));
    case "file.list":              return fileList(String(payload.dir || ""));
    case "file.read":              return fileRead(String(payload.path || ""));
    case "file.write":             return fileWrite(String(payload.path || ""), String(payload.content || ""));
    case "shell.exec":             return shellExec(String(payload.cmd || ""));
    case "server.versions":        return getPaperVersionList();
    case "server.version.switch":  return switchPaperVersion(String(payload.tag || ""));

    case "playit.setup": {
      if (playitSetupInProgress) {
        return { ok: true, alreadyRunning: true };
      }
      playitSetupInProgress = true;
      state.tunnel.state = "connecting";
      broadcast("tunnel.status", { tunnel: state.tunnel });
      (async () => {
        try {
          await ensurePlayitReady();
          if (playitProcess) {
            await stopPlayit();
            await new Promise(r => setTimeout(r, 800));
          }
          await startPlayit();
        } catch (err) {
          log(`[playit] Setup failed: ${err.message}`);
          state.tunnel.state = "error";
          broadcast("tunnel.status", { tunnel: state.tunnel });
        } finally {
          playitSetupInProgress = false;
        }
      })();
      return { ok: true, started: true };
    }

    case "playit.set_secret": {
      const secret = String(payload.secret || "").trim();
      if (!secret) throw new Error("No secret key provided");
      await saveSecret(secret);
      if (playitProcess) {
        await stopPlayit();
        await new Promise(r => setTimeout(r, 800));
      }
      await startPlayit();
      return { ok: true };
    }

    case "playit.save_tunnels": {
      const java    = String(payload.java    || "").trim();
      const bedrock = String(payload.bedrock || "").trim();
      state.tunnel.javaAddress    = java;
      state.tunnel.bedrockAddress = bedrock;
      if (java || bedrock) {
        state.tunnel.online = true;
        state.tunnel.state  = "connected";
      }
      await fs.mkdir(PLAYIT_DIR, { recursive: true });
      await fs.writeFile(PLAYIT_TUNNELS_FILE, JSON.stringify({ java, bedrock }));
      broadcast("tunnel.status", { tunnel: state.tunnel });
      log("[playit] Addresses saved");
      return { ok: true };
    }

    case "playit.stop": {
      await stopPlayit();
      return { ok: true };
    }

    case "playit.reset": {
      if (playitProcess) await stopPlayit();
      await clearPlayitdConfig();
      await fs.rm(PLAYIT_SECRET_FILE, { force: true }).catch(() => {});
      await fs.rm(PLAYIT_TUNNELS_FILE, { force: true }).catch(() => {});
      state.tunnel = { online: false, state: "waiting_auth", claimUrl: "", bedrockAddress: "", javaAddress: "", method: "playit" };
      broadcast("tunnel.status", { tunnel: state.tunnel });
      log("[playit] Tunnel reset — secret key and saved addresses cleared.");
      return { ok: true };
    }

    case "playit.status": {
      let listOutput = "";
      try { listOutput = execSync("proot-distro list 2>&1", { encoding: "utf8" }); } catch (e) { listOutput = String(e.stdout || ""); }
      const debianInstalled = new RegExp(`\\*\\s*${DEBIAN_DISTRO}\\b`).test(listOutput);
      const running         = !!playitProcess;
      const hasSecret       = !!(await getSavedSecret());
      let   savedTunnels    = {};
      try { savedTunnels = JSON.parse(await fs.readFile(PLAYIT_TUNNELS_FILE, "utf8")); } catch {}

      let hydrated = false;
      if (!state.tunnel.javaAddress && savedTunnels.java)       { state.tunnel.javaAddress    = savedTunnels.java;    hydrated = true; }
      if (!state.tunnel.bedrockAddress && savedTunnels.bedrock) { state.tunnel.bedrockAddress = savedTunnels.bedrock; hydrated = true; }
      if (hydrated) broadcast("tunnel.status", { tunnel: state.tunnel });

      const hasConfig = state.tunnel.state === "connected" || !!(savedTunnels.java || savedTunnels.bedrock);
      return {
        binaryExists: debianInstalled,
        hasConfig,
        hasSecret,
        running,
        tunnelState: state.tunnel.state,
        claimUrl:    state.tunnel.claimUrl,
        tunnels:     savedTunnels,
      };
    }

    default: throw new Error(`Unknown action: ${action}`);
  }
}

async function bootstrap() {
  await ensureDirs();
  await loadServerMode();
  await getWorlds();
  await getPacks();
  startStatsLoop();
  await setupPhAlias();

  try {
    const cached = JSON.parse(await fs.readFile(PLAYIT_TUNNELS_FILE, "utf8"));
    if (cached.java)    state.tunnel.javaAddress    = cached.java;
    if (cached.bedrock) state.tunnel.bedrockAddress = cached.bedrock;
    if (cached.java || cached.bedrock) log("[Bridge] Restored cached tunnel addresses from previous session");
  } catch {}

  const wss = new WebSocketServer({ host: "127.0.0.1", port: 8765 });
  console.log("[Bridge] Mini Host listening on ws://127.0.0.1:8765");
  console.log("[Bridge] Tip: type 'mh' next time to start bridge");

  wss.on("connection", ws => {
    clients.add(ws);
    console.log(`[Bridge] Client connected (${clients.size})`);
    try { ws.send(JSON.stringify({ type: "event", event: "connected", payload: snapshot() })); } catch {}

    ws.on("message", async raw => {
      let msg;
      try { msg = JSON.parse(String(raw)); } catch { return; }
      const { id, action } = msg;
      if (!id || !action) return;
      try {
        const result = await handleRequest(action, msg);
        try { ws.send(JSON.stringify({ type: "response", id, ok: true, payload: result })); } catch {}
      } catch (err) {
        console.error(`[Bridge] Error ${action}:`, err.message);
        try { ws.send(JSON.stringify({ type: "response", id, ok: false, error: err.message })); } catch {}
      }
    });

    ws.on("close", () => { clients.delete(ws); console.log(`[Bridge] Client disconnected (${clients.size})`); });
    ws.on("error", err  => { console.error("[Bridge] WS error:", err.message); clients.delete(ws); });
  });

  wss.on("error", err => console.error("[Bridge] Server error:", err.message));
}

bootstrap().catch(err => { console.error("[Bridge] Fatal:", err); stopStatsLoop(); process.exit(1); });
