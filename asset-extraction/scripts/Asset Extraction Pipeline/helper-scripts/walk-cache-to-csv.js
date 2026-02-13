/**
 * Walk MSW resource cache (msw + raw dirs) and write cache_index.csv for DuckDB pipeline Step 2.
 * Output columns: ruid, relative_path, suffix, asset_type, kind
 *
 * Usage: node walk-cache-to-csv.js --cache-dir <path> --out-csv <path> [--test [--catalog-csv <path>] [--sample-all-categories]]
 *   Without --test: scan full cache, emit all entries.
 *   With --test --catalog-csv <path>: only emit cache entries whose ruid is in the catalog, then sample per category.
 *   With --test --sample-all-categories: do NOT filter by catalog; full scan, then sample up to TEST_MAX_PER_CATEGORY
 *   per asset_type so test run exercises sprite, audioclip, animationclip, avataritem, etc. (step 2 augments catalog for join).
 */

const fs = require('fs');

/** In test mode, max cache index rows to keep per asset_type so we exercise each category. */
const TEST_MAX_PER_CATEGORY = 10;
const path = require('path');

const RUID_32 = /^[0-9a-fA-F]{32}$/;

function progress(msg) {
  process.stderr.write(msg + '\n');
}

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { cacheDir: null, outCsv: null, test: false, catalogCsv: null, sampleAllCategories: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--cache-dir' && args[i + 1]) out.cacheDir = args[++i];
    else if (args[i] === '--out-csv' && args[i + 1]) out.outCsv = args[++i];
    else if (args[i] === '--test') out.test = true;
    else if (args[i] === '--catalog-csv' && args[i + 1]) out.catalogCsv = args[++i];
    else if (args[i] === '--sample-all-categories') out.sampleAllCategories = true;
  }
  return out;
}

/** Parse CSV line (simple: no quoted commas). */
function parseCsvLine(line) {
  return line.split(',').map((s) => s.replace(/^"|"$/g, '').trim());
}

/** Load set of RUIDs (lowercase) from catalog CSV. Returns Set or null if no catalog. */
function loadCatalogRuids(catalogCsvPath) {
  if (!catalogCsvPath || !fs.existsSync(catalogCsvPath)) return null;
  const content = fs.readFileSync(catalogCsvPath, 'utf8');
  const lines = content.split(/\r?\n/).filter((l) => l.length > 0);
  if (lines.length < 2) return new Set();
  const header = parseCsvLine(lines[0]);
  const ruidIdx = header.findIndex((h) => h.toLowerCase() === 'ruid');
  if (ruidIdx < 0) return new Set();
  const set = new Set();
  for (let i = 1; i < lines.length; i++) {
    const row = parseCsvLine(lines[i]);
    const ruid = (row[ruidIdx] || '').trim().toLowerCase();
    if (RUID_32.test(ruid)) set.add(ruid);
  }
  return set;
}

function scanMswDir(mswPath, cacheRoot, maxScan, ruidSet) {
  const list = [];
  function walk(dir, bucketName) {
    if (list.length >= maxScan) return;
    const assetType = bucketName.match(/^[0-9a-f]+-(.+)$/) ? bucketName.replace(/^[0-9a-f]+-/, '') : bucketName;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const e of entries) {
      if (list.length >= maxScan) return;
      const full = path.join(dir, e.name);
      if (e.isDirectory()) walk(full, bucketName);
      else if (e.isFile() && e.name.endsWith('.mod')) {
        const m = e.name.match(/^([0-9a-fA-F]{32})\.([^.]+)\.mod$/) || e.name.match(/^([0-9a-fA-F]{32})\.mod$/);
        const ruid = m ? m[1] : e.name.replace(/\.mod$/, '');
        if (ruidSet && !ruidSet.has(ruid.toLowerCase())) continue;
        const suffix = m && m[2] !== undefined ? m[2] : '';
        const rel = path.relative(cacheRoot, full).replace(/\\/g, '/');
        list.push({ ruid, assetType, suffix, relativePath: rel, kind: 'msw' });
      }
    }
  }
  const buckets = fs.readdirSync(mswPath, { withFileTypes: true });
  for (const b of buckets) {
    if (b.isDirectory()) walk(path.join(mswPath, b.name), b.name);
  }
  return list;
}

function scanRawDir(rawPath, cacheRoot, maxScan, ruidSet) {
  const list = [];
  function walk(dir) {
    if (list.length >= maxScan) return;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const e of entries) {
      if (list.length >= maxScan) return;
      const full = path.join(dir, e.name);
      if (e.isDirectory()) walk(full);
      else if (e.isFile()) {
        const ext = path.extname(e.name).replace(/^\./, '');
        const base = path.basename(e.name, path.extname(e.name));
        const ruid = RUID_32.test(base) ? base : base;
        if (ruidSet && !ruidSet.has(ruid.toLowerCase())) continue;
        const rel = path.relative(cacheRoot, full).replace(/\\/g, '/');
        const pathUnderRaw = path.relative(rawPath, full);
        const segments = pathUnderRaw.split(/[/\\]/);
        const assetType = segments.length > 0 ? segments[0] : 'raw';
        list.push({ ruid, assetType, suffix: ext, relativePath: rel, kind: 'raw' });
      }
    }
  }
  walk(rawPath);
  return list;
}

/** In test mode with catalog filter, keep at most N entries per asset_type so each category is exercised. */
function samplePerCategory(list, maxPerCategory, progressFn) {
  const byCategory = new Map();
  for (const r of list) {
    const key = (r.assetType || 'unknown').toLowerCase();
    if (!byCategory.has(key)) byCategory.set(key, []);
    const arr = byCategory.get(key);
    if (arr.length < maxPerCategory) arr.push(r);
  }
  const out = [];
  for (const [cat, arr] of byCategory) {
    out.push(...arr);
    if (progressFn) progressFn('  category "' + cat + '": ' + arr.length + ' rows');
  }
  return out;
}

function escapeCsv(s) {
  const t = String(s ?? '');
  if (t.includes(',') || t.includes('"') || t.includes('\n') || t.includes('\r')) {
    return '"' + t.replace(/"/g, '""') + '"';
  }
  return t;
}

function main() {
  const { cacheDir, outCsv, test, catalogCsv, sampleAllCategories } = parseArgs();
  if (!cacheDir || !outCsv) {
    console.error('Usage: node walk-cache-to-csv.js --cache-dir <path> --out-csv <path> [--test [--catalog-csv <path>] [--sample-all-categories]]');
    process.exit(1);
  }
  const cacheRoot = path.resolve(cacheDir).replace(/[/\\]+$/, '');
  if (!fs.existsSync(cacheRoot)) {
    console.error('Cache dir not found:', cacheRoot);
    process.exit(1);
  }
  const outDir = path.dirname(outCsv);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  let ruidSet = null;
  if (test && sampleAllCategories) {
    progress('  Test mode: sample from all categories (full cache scan, no catalog filter)');
  } else if (test && catalogCsv) {
    ruidSet = loadCatalogRuids(catalogCsv);
    progress('  Test mode: only cache entries for ' + ruidSet.size + ' catalog RUID(s)');
  }
  const maxScan = Number.MAX_SAFE_INTEGER;
  let list = [];
  const mswPath = path.join(cacheRoot, 'msw');
  const rawPath = path.join(cacheRoot, 'raw');
  if (fs.existsSync(mswPath)) {
    const mswList = scanMswDir(mswPath, cacheRoot, maxScan, ruidSet);
    list = list.concat(mswList);
    progress('  msw: ' + mswList.length + ' files');
  }
  if (fs.existsSync(rawPath)) {
    const rawList = scanRawDir(rawPath, cacheRoot, maxScan, ruidSet);
    list = list.concat(rawList);
    progress('  raw: ' + rawList.length + ' files');
  }
  if (test && list.length > 0) {
    progress('  Test: sampling up to ' + TEST_MAX_PER_CATEGORY + ' per category');
    list = samplePerCategory(list, TEST_MAX_PER_CATEGORY, progress);
    progress('  Total after sampling: ' + list.length + ' rows');
  } else {
    progress('  Total: ' + list.length + ' rows');
  }
  progress('  Writing -> ' + outCsv);

  const out = fs.createWriteStream(outCsv, { encoding: 'utf8' });
  out.write('ruid,relative_path,suffix,asset_type,kind\n');
  for (const r of list) {
    out.write(escapeCsv(r.ruid) + ',' + escapeCsv(r.relativePath) + ',' + escapeCsv(r.suffix) + ',' + escapeCsv(r.assetType) + ',' + escapeCsv(r.kind) + '\n');
  }
  out.end();
  progress('Done.');
}

main();
