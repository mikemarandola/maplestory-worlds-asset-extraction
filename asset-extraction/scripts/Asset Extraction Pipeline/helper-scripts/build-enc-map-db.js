/**
 * Step 5a helper: Read sprite .win/.dxt rows from --input-csv, read .mod files (bytes 3-18 = enc),
 * write enc_key map to --out-csv. CSV mode only (DuckDB pipeline).
 *
 * Usage: node build-enc-map-db.js --input-csv <path> --out-csv <path> --cache-dir <path> [--test] [--concurrency N]
 */
const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const os = require('os');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

/** Write buffer batch size from memory: ~50KB per row estimate; clamp 2k–50k. */
function getDefaultBatch() {
  const free = typeof os.freemem === 'function' ? os.freemem() : 2e9;
  const n = Math.floor(free / (50 * 1024));
  return Math.max(2000, Math.min(50000, n));
}
const BATCH = getDefaultBatch();
/** Half of logical CPUs, cap 32; matches pipeline _get-parallelism.ps1 when --concurrency not passed. */
const DEFAULT_CONCURRENCY = Math.max(1, Math.min(32, Math.floor(((os.cpus && os.cpus().length) || 4) / 2)));

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { cacheDir: null, inputCsv: null, outCsv: null, test: false, concurrency: DEFAULT_CONCURRENCY };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--cache-dir' && args[i + 1]) out.cacheDir = args[++i];
    else if (args[i] === '--input-csv' && args[i + 1]) out.inputCsv = args[++i];
    else if (args[i] === '--out-csv' && args[i + 1]) out.outCsv = args[++i];
    else if (args[i] === '--test') out.test = true;
    else if (args[i] === '--concurrency' && args[i + 1]) out.concurrency = Math.max(1, parseInt(args[++i], 10) || 1);
  }
  return out;
}

async function processChunkAsync(cacheRoot, chunk) {
  const paths = chunk.map(r => path.join(cacheRoot, (r.relative_path || '').replace(/\//g, path.sep)));
  const bufs = await Promise.all(
    paths.map(p => fsPromises.readFile(p).catch(() => null))
  );
  const out = [];
  for (let i = 0; i < chunk.length; i++) {
    const buf = bufs[i];
    if (buf && buf.length >= 19) {
      out.push({ rowid: chunk[i].rowid, encKey: buf.slice(3, 19).toString('hex').toLowerCase(), ruid: chunk[i].ruid });
    }
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

async function runCsvMode(inputCsv, outCsv, cacheDir, test, concurrency) {
  const cacheRoot = path.resolve(cacheDir).replace(/[/\\]+$/, '');
  const rowLimit = test ? 50 : null;
  const rows = [];
  const rl = createInterface({ input: createReadStream(inputCsv, { encoding: 'utf8' }), crlfDelay: Infinity });
  let header = null;
  let ruidIdx = 0, relIdx = 1;
  for await (const line of rl) {
    const row = parseCsvLine(line);
    if (header === null) {
      header = row;
      const lower = (s) => (s || '').toLowerCase();
      ruidIdx = header.findIndex(h => lower(h) === 'ruid');
      relIdx = header.findIndex(h => lower(h) === 'relative_path');
      if (ruidIdx < 0 || relIdx < 0) {
        process.stderr.write('CSV must have ruid, relative_path.\n');
        process.exit(1);
      }
      continue;
    }
    rows.push({ ruid: row[ruidIdx], relative_path: row[relIdx] });
    if (rowLimit != null && rows.length >= rowLimit) break;
  }
  process.stderr.write(`enc map CSV: ${rows.length} rows, concurrency ${concurrency}\n`);
  const out = fs.createWriteStream(outCsv, { encoding: 'utf8' });
  out.write('ruid,enc_key\n');
  if (concurrency <= 1) {
    for (const r of rows) {
      const fullPath = path.join(cacheRoot, (r.relative_path || '').replace(/\//g, path.sep));
      try {
        const buf = fs.readFileSync(fullPath);
        if (buf.length >= 19) {
          const encKey = buf.slice(3, 19).toString('hex').toLowerCase();
          out.write(`"${(r.ruid || '').replace(/"/g, '""')}","${encKey}"\n`);
        }
      } catch (_) {}
    }
  } else {
    for (let i = 0; i < rows.length; i += concurrency) {
      const chunk = rows.slice(i, i + concurrency);
      const paths = chunk.map(r => path.join(cacheRoot, (r.relative_path || '').replace(/\//g, path.sep)));
      const bufs = await Promise.all(paths.map(p => fsPromises.readFile(p).catch(() => null)));
      for (let j = 0; j < chunk.length; j++) {
        if (bufs[j] && bufs[j].length >= 19) {
          const encKey = bufs[j].slice(3, 19).toString('hex').toLowerCase();
          out.write(`"${(chunk[j].ruid || '').replace(/"/g, '""')}","${encKey}"\n`);
        }
      }
    }
  }
  out.end();
  process.stderr.write('enc_keys written to ' + outCsv + '\n');
}

async function mainAsync() {
  const { cacheDir, inputCsv, outCsv, test, concurrency } = parseArgs();
  if (inputCsv && outCsv && cacheDir) {
    await runCsvMode(inputCsv, outCsv, cacheDir, test, concurrency);
    return;
  }
  process.stderr.write('Usage: node build-enc-map-db.js --input-csv <path> --out-csv <path> --cache-dir <path> [--test] [--concurrency N]\n');
  process.exit(1);
}

mainAsync().catch(err => {
  console.error(err);
  process.exit(1);
});
