/**
 * Stream extract_list.csv as JSONL (same format as stream-catalog-enriched-for-extract.js).
 * Used by DuckDB pipeline Step 3 when reading from staging CSV instead of DB.
 *
 * Usage: node stream-from-csv.js --input-csv <path> --cache-dir <path>
 */

const fs = require('fs');
const path = require('path');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { inputCsv: null, cacheDir: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--input-csv' && args[i + 1]) out.inputCsv = args[++i];
    else if (args[i] === '--cache-dir' && args[i + 1]) out.cacheDir = args[++i];
  }
  return out;
}

function escapeCsvField(s) {
  const t = String(s ?? '');
  if (t.includes(',') || t.includes('"') || t.includes('\n') || t.includes('\r')) {
    return '"' + t.replace(/"/g, '""') + '"';
  }
  return t;
}

function parseCsvLine(line) {
  const out = [];
  let i = 0;
  const len = line.length;
  while (i < len) {
    if (line[i] === '"') {
      i++;
      let s = '';
      while (i < len) {
        if (line[i] === '"') {
          i++;
          if (i < len && line[i] === '"') { s += '"'; i++; }
          else break;
        } else { s += line[i]; i++; }
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

async function main() {
  const { inputCsv, cacheDir } = parseArgs();
  if (!inputCsv || !cacheDir) {
    process.stderr.write('Usage: node stream-from-csv.js --input-csv <path> --cache-dir <path>\n');
    process.exit(1);
  }
  const cacheRoot = path.resolve(cacheDir).replace(/[/\\]+$/, '');
  if (!fs.existsSync(inputCsv)) {
    process.stderr.write('Input CSV not found: ' + inputCsv + '\n');
    process.exit(1);
  }

  const rl = createInterface({ input: createReadStream(inputCsv, { encoding: 'utf8' }), crlfDelay: Infinity });
  let header = null;
  let indices = null;
  for await (const line of rl) {
    const row = parseCsvLine(line);
    if (header === null) {
      header = row;
      const lower = (s) => (s || '').toLowerCase();
      const ruidIdx = header.findIndex(h => lower(h) === 'ruid');
      const outputSubdirIdx = header.findIndex(h => lower(h) === 'output_subdir');
      const relativePathIdx = header.findIndex(h => lower(h) === 'relative_path');
      const suffixIdx = header.findIndex(h => lower(h) === 'suffix');
      const assetTypeIdx = header.findIndex(h => lower(h) === 'asset_type');
      if (ruidIdx < 0 || relativePathIdx < 0 || assetTypeIdx < 0) {
        process.stderr.write('CSV must have ruid, relative_path, asset_type columns.\n');
        process.exit(1);
      }
      indices = { ruid: ruidIdx, output_subdir: outputSubdirIdx >= 0 ? outputSubdirIdx : null, relative_path: relativePathIdx, suffix: suffixIdx >= 0 ? suffixIdx : null, asset_type: assetTypeIdx };
      continue;
    }
    const ruid = (row[indices.ruid] ?? '').trim();
    const output_subdir = indices.output_subdir != null ? (row[indices.output_subdir] ?? '').trim() || 'Unknown' : 'Unknown';
    const relative_path = (row[indices.relative_path] ?? '').trim();
    const suffix = indices.suffix != null ? (row[indices.suffix] ?? '').trim() : '';
    const asset_type = (row[indices.asset_type] ?? '').trim();
    const modPath = path.join(cacheRoot, relative_path.replace(/\//g, path.sep));
    const obj = { ruid, output_subdir, relative_path, modPath, asset_type };
    if (suffix) obj.suffix = suffix;
    process.stdout.write(JSON.stringify(obj) + '\n');
  }
}

main().catch(err => {
  process.stderr.write(String(err) + '\n');
  process.exit(1);
});
