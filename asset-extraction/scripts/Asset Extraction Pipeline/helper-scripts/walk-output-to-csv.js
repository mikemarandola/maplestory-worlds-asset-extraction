/**
 * Walk output/images and output/audio directories; write existing_paths.csv (relative path per line).
 * Used by DuckDB pipeline Step 6 Phase A to determine has_data.
 *
 * Usage: node walk-output-to-csv.js --images-dir <path> --audio-dir <path> --out-csv <path>
 */

const fs = require('fs');
const path = require('path');

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { imagesDir: null, audioDir: null, outCsv: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--images-dir' && args[i + 1]) out.imagesDir = args[++i];
    else if (args[i] === '--audio-dir' && args[i + 1]) out.audioDir = args[++i];
    else if (args[i] === '--out-csv' && args[i + 1]) out.outCsv = args[++i];
  }
  return out;
}

function walkDir(dir, baseDir, list) {
  if (!fs.existsSync(dir)) return;
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      walkDir(full, baseDir, list);
    } else if (e.isFile()) {
      const rel = path.relative(baseDir, full).replace(/\\/g, '/');
      list.push(rel);
    }
  }
}

function main() {
  const { imagesDir, audioDir, outCsv } = parseArgs();
  if (!imagesDir || !audioDir || !outCsv) {
    process.stderr.write('Usage: node walk-output-to-csv.js --images-dir <path> --audio-dir <path> --out-csv <path>\n');
    process.exit(1);
  }
  const imagesBase = path.resolve(imagesDir);
  const audioBase = path.resolve(audioDir);
  const list = [];
  walkDir(imagesBase, imagesBase, list);
  walkDir(audioBase, audioBase, list);
  const outDir = path.dirname(outCsv);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const out = fs.createWriteStream(outCsv, { encoding: 'utf8' });
  out.write('relative_path\n');
  for (const rel of list) {
    out.write('"' + rel.replace(/"/g, '""') + '"\n');
  }
  out.end();
  process.stderr.write('existing_paths: ' + list.length + ' rows -> ' + outCsv + '\n');
}

main();
