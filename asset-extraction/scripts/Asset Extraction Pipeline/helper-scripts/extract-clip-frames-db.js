/**
 * Step 5b: Read animationclip rows from catalog_enriched (or --clip-list-csv), load enc_key->ruid (or --enc-ruid-map-csv),
 * parse each clip .mod, write animation_frames (DB) or frame_index.csv (--out-csv). When --out-csv set, CSV mode (DuckDB pipeline).
 *
 * Usage: node extract-clip-frames-db.js --db <path> --cache-dir <path> [--test] [--concurrency N]
 *        node extract-clip-frames-db.js --clip-list-csv <path> --enc-ruid-map-csv <path> --out-csv <path> --cache-dir <path> [--test] [--concurrency N]
 */
const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const os = require('os');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

/** Write buffer batch size from memory: ~100 bytes per frame row; clamp 5k–50k. */
function getDefaultBatch() {
  const free = typeof os.freemem === 'function' ? os.freemem() : 2e9;
  const n = Math.floor(free / (100 * 1024));
  return Math.max(5000, Math.min(50000, n));
}
const BATCH = getDefaultBatch();
/** Half of logical CPUs, cap 32; matches pipeline _get-parallelism.ps1 when --concurrency not passed. */
const DEFAULT_CONCURRENCY = Math.max(1, Math.min(32, Math.floor(((os.cpus && os.cpus().length) || 4) / 2)));

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { db: null, cacheDir: null, clipListCsv: null, encRuidMapCsv: null, outCsv: null, test: false, concurrency: DEFAULT_CONCURRENCY };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--db' && args[i + 1]) out.db = args[++i];
    else if (args[i] === '--cache-dir' && args[i + 1]) out.cacheDir = args[++i];
    else if (args[i] === '--clip-list-csv' && args[i + 1]) out.clipListCsv = args[++i];
    else if (args[i] === '--enc-ruid-map-csv' && args[i + 1]) out.encRuidMapCsv = args[++i];
    else if (args[i] === '--out-csv' && args[i + 1]) out.outCsv = args[++i];
    else if (args[i] === '--test') out.test = true;
    else if (args[i] === '--concurrency' && args[i + 1]) out.concurrency = Math.max(1, parseInt(args[++i], 10) || 1);
  }
  return out;
}

function parseCsvLine(line) {
  const out = [];
  let i = 0;
  const len = line.length;
  while (i < len) {
    if (line[i] === '"') {
      i++; let s = '';
      while (i < len) {
        if (line[i] === '"') { i++; if (i < len && line[i] === '"') { s += '"'; i++; } else break; }
        else { s += line[i]; i++; }
      }
      out.push(s);
    } else {
      let start = i;
      while (i < len && line[i] !== ',') i++;
      out.push(line.slice(start, i).replace(/""/g, '"'));
      if (i < len) i++;
    }
  }
  return out;
}

async function loadEncRuidMap(filePath) {
  const encMap = new Map();
  const rl = createInterface({ input: createReadStream(filePath, { encoding: 'utf8' }), crlfDelay: Infinity });
  let header = null;
  let encIdx = 0, ruidIdx = 1;
  for await (const line of rl) {
    const row = parseCsvLine(line);
    if (header === null) {
      header = row;
      const lower = (s) => (s || '').toLowerCase();
      encIdx = header.findIndex(h => lower(h) === 'enc_key');
      ruidIdx = header.findIndex(h => lower(h) === 'ruid');
      if (encIdx < 0 || ruidIdx < 0) {
        process.stderr.write('enc_ruid_map CSV must have enc_key, ruid.\n');
        process.exit(1);
      }
      continue;
    }
    const k = (row[encIdx] || '').trim().toLowerCase();
    if (k) encMap.set(k, (row[ruidIdx] || '').trim());
  }
  return encMap;
}

async function loadClipList(filePath, limit) {
  const clips = [];
  const rl = createInterface({ input: createReadStream(filePath, { encoding: 'utf8' }), crlfDelay: Infinity });
  let header = null;
  let ruidIdx = 0, relIdx = 1, suffixIdx = 2, encIdx = 3;
  for await (const line of rl) {
    const row = parseCsvLine(line);
    if (header === null) {
      header = row;
      const lower = (s) => (s || '').toLowerCase();
      ruidIdx = header.findIndex(h => lower(h) === 'ruid');
      relIdx = header.findIndex(h => lower(h) === 'relative_path');
      suffixIdx = header.findIndex(h => lower(h) === 'suffix');
      encIdx = header.findIndex(h => lower(h) === 'enc_key');
      if (ruidIdx < 0 || relIdx < 0) {
        process.stderr.write('clip_list CSV must have ruid, relative_path.\n');
        process.exit(1);
      }
      continue;
    }
    clips.push({ ruid: row[ruidIdx], relative_path: row[relIdx], suffix: suffixIdx >= 0 ? row[suffixIdx] : '' });
    if (limit != null && clips.length >= limit) break;
  }
  return clips;
}

async function runCsvMode(clipListCsv, encRuidMapCsv, outCsv, cacheDir, test, concurrency) {
  const cacheRoot = path.resolve(cacheDir).replace(/[/\\]+$/, '');
  const encMap = await loadEncRuidMap(encRuidMapCsv);
  process.stderr.write(`Enc map: ${encMap.size} entries.\n`);
  const clipLimit = test ? 50 : null;
  const clips = await loadClipList(clipListCsv, clipLimit);
  process.stderr.write(`Clips: ${clips.length} rows, concurrency: ${concurrency}\n`);
  const out = fs.createWriteStream(outCsv, { encoding: 'utf8' });
  out.write('clip_ruid,frame_index,frame_ruid,frame_duration_ms\n');
  const escape = (s) => (String(s ?? '').includes(',') || String(s ?? '').includes('"') ? '"' + String(s).replace(/"/g, '""') + '"' : String(s));
  let processed = 0;
  const PROGRESS_INTERVAL_MS = 30000;
  let lastProgressTime = Date.now();
  if (concurrency <= 1) {
    for (const clip of clips) {
      const fullPath = path.join(cacheRoot, (clip.relative_path || '').replace(/\//g, path.sep));
      if (!fs.existsSync(fullPath)) continue;
      let buf;
      try { buf = fs.readFileSync(fullPath); } catch (_) { continue; }
      const rows = parseClipToRows(clip, buf, encMap);
      for (const r of rows) {
        out.write(escape(r.clip_ruid) + ',' + r.frame_number + ',' + escape(r.frame_ruid) + ',' + r.frame_duration_ms + '\n');
      }
      processed++;
      const now = Date.now();
      if (processed % 500 === 0 || now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
        process.stderr.write(`progress: clip ${processed}\n`);
        lastProgressTime = now;
      }
    }
  } else {
    for (let i = 0; i < clips.length; i += concurrency) {
      const chunk = clips.slice(i, i + concurrency);
      const paths = chunk.map(c => path.join(cacheRoot, (c.relative_path || '').replace(/\//g, path.sep)));
      const bufs = await Promise.all(paths.map(p => fsPromises.readFile(p).catch(() => null)));
      for (let j = 0; j < chunk.length; j++) {
        if (!bufs[j]) continue;
        const rows = parseClipToRows(chunk[j], bufs[j], encMap);
        for (const r of rows) {
          out.write(escape(r.clip_ruid) + ',' + r.frame_number + ',' + escape(r.frame_ruid) + ',' + r.frame_duration_ms + '\n');
        }
        processed++;
      }
      const now = Date.now();
      if (processed % 500 < concurrency || now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
        process.stderr.write(`progress: clip ${processed}\n`);
        lastProgressTime = now;
      }
    }
  }
  out.end();
  process.stderr.write(`frame_index written to ${outCsv}\n`);
}


/** enc_key in DB is 32-char hex (lowercase). Build same key from 16-byte buffer. */
function bufToEncKey(buf, offset = 0) {
  return buf.slice(offset, offset + 16).toString('hex').toLowerCase();
}

/** .model.mod: decode 16-byte payload to 32-char hex RUID (word0 byte-reversed, word1 1,0,3,2). */
function decodeModelModChunk(buf, offset) {
  if (offset + 16 > buf.length) return null;
  const r = Buffer.alloc(16);
  r[0] = buf[offset + 3]; r[1] = buf[offset + 2]; r[2] = buf[offset + 1]; r[3] = buf[offset + 0];
  r[4] = buf[offset + 5]; r[5] = buf[offset + 4]; r[6] = buf[offset + 7]; r[7] = buf[offset + 6];
  for (let i = 8; i < 16; i++) r[i] = buf[offset + i];
  const hex = r.toString('hex').toLowerCase();
  return /^[0-9a-f]{32}$/.test(hex) ? hex : null;
}

/** Find payload start ([[ or Unity or System.Single). */
function findPayloadStart(buf) {
  let payloadStart = 24;
  const len = Math.min(128, buf.length - 4);
  for (let off = 0; off < len; off++) {
    const s = buf.slice(off, off + Math.min(30, buf.length - off)).toString('ascii');
    if (/\[\[|Unity|System\.Single/.test(s)) {
      payloadStart = off;
      break;
    }
  }
  return payloadStart;
}

/** Get frame duration (ms) from 0A 79 08 float blocks. */
function getFrameDurationMs(buf, payloadStart) {
  const len = buf.length - payloadStart;
  for (let i = 0; i <= Math.min(len - 9, 256); i++) {
    const base = payloadStart + i;
    if (buf[base] === 0x0A && buf[base + 1] === 0x79 && buf[base + 2] === 0x08) {
      const f = buf.readFloatLE(base + 5);
      if (!Number.isNaN(f) && f >= 0.01 && f <= 2) return Math.max(1, Math.round(f * 1000));
    }
  }
  return 80;
}

/** Parse .model.mod: 0A 79 08 blocks, decode 16-byte to RUID; return { frameRuids, frameDurationMs }. */
function parseModelMod(buf, payloadStart) {
  const frameRuids = [];
  const durations = [];
  const len = buf.length - payloadStart;
  for (let i = 0; i <= len - 16 - 13; i++) {
    const base = payloadStart + i;
    if (buf[base] === 0x0A && buf[base + 1] === 0x79 && buf[base + 2] === 0x08) {
      const ruid = decodeModelModChunk(buf, base + 13);
      if (ruid) frameRuids.push(ruid);
      if (base + 9 <= buf.length) {
        const f = buf.readFloatLE(base + 5);
        if (!Number.isNaN(f) && f >= 0.01 && f <= 2) durations.push(f);
      }
    }
  }
  let frameDurationMs = 80;
  if (durations.length > 0) {
    durations.sort((a, b) => a - b);
    const mid = durations.length / 2;
    const medianSec = durations.length % 2 === 1
      ? durations[Math.floor(mid)]
      : (durations[mid - 1] + durations[mid]) / 2;
    frameDurationMs = Math.max(1, Math.round(medianSec * 1000));
  }
  return { frameRuids, frameDurationMs };
}

/** Parse non-.model clip: scan 16-byte chunks, lookup enc_key -> ruid; get duration from 0A 79 08. */
function parseClipWithEncMap(buf, payloadStart, encToRuid) {
  const seen = new Set();
  const frameRuids = [];
  for (let i = payloadStart; i <= buf.length - 16; i++) {
    const key = bufToEncKey(buf, i);
    const ruid = encToRuid.get(key);
    if (ruid) {
      const k = ruid.toLowerCase();
      if (!seen.has(k)) {
        seen.add(k);
        frameRuids.push(ruid);
      }
    }
  }
  const frameDurationMs = getFrameDurationMs(buf, payloadStart);
  return { frameRuids, frameDurationMs };
}

/** Parse one clip buffer; returns array of { clip_ruid, frame_number, frame_ruid, frame_duration_ms }. */
function parseClipToRows(clip, buf, encMap) {
  const clipRuid = (clip.ruid || '').trim();
  let frameRuids = [];
  let frameDurationMs = 80;
  try {
    const payloadStart = findPayloadStart(buf);
    const isModelMod = (clip.suffix || '').toLowerCase() === 'model' || /\.model\.mod$/i.test(clip.relative_path || '');
    if (isModelMod) {
      const out = parseModelMod(buf, payloadStart);
      frameRuids = out.frameRuids;
      frameDurationMs = out.frameDurationMs;
    } else {
      const out = parseClipWithEncMap(buf, payloadStart, encMap);
      frameRuids = out.frameRuids;
      frameDurationMs = out.frameDurationMs;
    }
  } catch (_) {}
  if (frameRuids.length === 0) return [];
  return frameRuids.map((frame_ruid, i) => ({
    clip_ruid: clipRuid,
    frame_number: i,
    frame_ruid,
    frame_duration_ms: frameDurationMs
  }));
}

async function mainAsync() {
  const { db: dbPath, cacheDir, clipListCsv, encRuidMapCsv, outCsv, test, concurrency } = parseArgs();
  if (clipListCsv && encRuidMapCsv && outCsv && cacheDir) {
    await runCsvMode(clipListCsv, encRuidMapCsv, outCsv, cacheDir, test, concurrency);
    return;
  }
  if (!dbPath || !cacheDir) {
    process.stderr.write('Usage: node extract-clip-frames-db.js --db <path> --cache-dir <path> [--test] [--concurrency N]\n');
    process.stderr.write('   or: node extract-clip-frames-db.js --clip-list-csv <path> --enc-ruid-map-csv <path> --out-csv <path> --cache-dir <path> [--test]\n');
    process.exit(1);
  }
  const Database = require('better-sqlite3');
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  const cacheRoot = path.resolve(cacheDir).replace(/[/\\]+$/, '');

  const encMap = new Map();
  for (const r of db.prepare('SELECT enc_key, ruid FROM catalog_enriched WHERE enc_key IS NOT NULL').all()) {
    const k = (r.enc_key || '').trim().toLowerCase();
    if (k) encMap.set(k, (r.ruid || '').trim());
  }
  process.stderr.write(`Enc map: ${encMap.size} entries.\n`);

  const clipLimit = test ? 50 : null;
  process.stderr.write(`Clips: streaming (no row limit when not --test), concurrency: ${concurrency}\n`);

  db.exec(`
    DROP TABLE IF EXISTS animation_frames;
    CREATE TABLE animation_frames (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      clip_ruid TEXT NOT NULL,
      frame_index INTEGER NOT NULL,
      frame_ruid TEXT NOT NULL,
      frame_duration_ms INTEGER NOT NULL,
      image_path TEXT,
      offset_x REAL,
      offset_y REAL,
      UNIQUE(clip_ruid, frame_index)
    );
  `);
  const insert = db.prepare(`
    INSERT INTO animation_frames (clip_ruid, frame_index, frame_ruid, frame_duration_ms, image_path, offset_x, offset_y)
    VALUES (?, ?, ?, ?, NULL, NULL, NULL)
  `);
  const runBatch = db.transaction((rows) => {
    for (const r of rows) insert.run(r.clip_ruid, r.frame_number, r.frame_ruid, r.frame_duration_ms);
  });

  const batch = [];
  let processed = 0;
  const PROGRESS_INTERVAL_MS = 30000;
  let lastProgressTime = Date.now();

  if (concurrency <= 1) {
    const clipStmt = db.prepare(`
      SELECT ruid, relative_path, suffix
      FROM catalog_enriched
      WHERE LOWER(TRIM(asset_type)) = 'animationclip'
      ORDER BY rowid
    `);
    for (const clip of clipStmt.iterate()) {
      if (clipLimit != null && processed >= clipLimit) break;
      const fullPath = path.join(cacheRoot, (clip.relative_path || '').replace(/\//g, path.sep));
      if (!fs.existsSync(fullPath)) continue;
      let buf;
      try { buf = fs.readFileSync(fullPath); } catch (_) { continue; }
      const rows = parseClipToRows(clip, buf, encMap);
      for (const r of rows) {
        batch.push(r);
        if (batch.length >= BATCH) { runBatch(batch); batch.length = 0; }
      }
      processed++;
      const now = Date.now();
      if (processed % 500 === 0 || now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
        process.stderr.write(`progress: clip frames - clip ${processed} processed\n`);
        lastProgressTime = now;
      }
    }
  } else {
    // Concurrency > 1: read all clip rows into memory first so we never hold
    // a DB iterator open across await (avoids "connection is busy").
    const clipQuery = clipLimit != null
      ? db.prepare(`
          SELECT ruid, relative_path, suffix
          FROM catalog_enriched
          WHERE LOWER(TRIM(asset_type)) = 'animationclip'
          ORDER BY rowid
          LIMIT ?
        `)
      : db.prepare(`
          SELECT ruid, relative_path, suffix
          FROM catalog_enriched
          WHERE LOWER(TRIM(asset_type)) = 'animationclip'
          ORDER BY rowid
        `);
    const allClips = clipLimit != null ? clipQuery.all(clipLimit) : clipQuery.all();
    for (let i = 0; i < allClips.length; i += concurrency) {
      const chunk = allClips.slice(i, i + concurrency);
      const paths = chunk.map(c => path.join(cacheRoot, (c.relative_path || '').replace(/\//g, path.sep)));
      const bufs = await Promise.all(paths.map(p => fsPromises.readFile(p).catch(() => null)));
      for (let j = 0; j < chunk.length; j++) {
        if (!bufs[j]) continue;
        const rows = parseClipToRows(chunk[j], bufs[j], encMap);
        for (const r of rows) {
          batch.push(r);
          if (batch.length >= BATCH) { runBatch(batch); batch.length = 0; }
        }
        processed++;
      }
      const now = Date.now();
      if (processed % 500 < concurrency || now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
        process.stderr.write(`progress: clip frames - clip ${processed} processed\n`);
        lastProgressTime = now;
      }
    }
    if (processed % 500 > 0 || processed === 0) process.stderr.write(`  ${processed} clips\n`);
  }

  if (batch.length > 0) runBatch(batch);
  process.stderr.write(`Step 5b done. animation_frames (clip/frame/ruid/duration) written.\n`);
  db.close();
}

mainAsync().catch(err => {
  console.error(err);
  process.exit(1);
});
