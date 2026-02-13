/**
 * Build thumbnails for all image-bearing types: sprite, damageskin, avataritem, atlas.
 * One thumb per image RUID (dedupe by RUID across types). Audio = no thumb.
 * Animation clips do not get their own thumb file; DB has thumbnail_ruid = median frame RUID.
 * Output: thumbs/<ruid>.png. Requires: output/images (step 4), temp/ruids.csv (step 2).
 * Default: overwrites existing thumb files. Use --skip-existing to skip files that already exist.
 * DB mode reads from a PGLite metadata directory (output/metadata).
 */
const fs = require('fs');
const path = require('path');
const os = require('os');
const Papa = require('papaparse');
const sharp = require('sharp');

/** Half of logical CPUs, cap 32; used when --concurrency not passed. */
function getDefaultConcurrency() {
  const n = (os.cpus && os.cpus().length) || 4;
  return Math.max(1, Math.min(32, Math.floor(n / 2)));
}

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--images-dir' && args[i + 1]) out.imagesDir = args[++i];
    else if (args[i] === '--thumbs-dir' && args[i + 1]) out.thumbsDir = args[++i];
    else if (args[i] === '--ruids-csv' && args[i + 1]) out.ruidsCsv = args[++i];
    else if (args[i] === '--db' && args[i + 1]) out.db = args[++i];
    else if (args[i] === '--max-pixels' && args[i + 1]) out.maxPixels = parseInt(args[++i], 10);
    else if (args[i] === '--skip-existing') out.skipExisting = true;
    else if (args[i] === '--concurrency' && args[i + 1]) out.concurrency = parseInt(args[++i], 10);
    else if (args[i] === '--test' && args[i + 1]) out.testLimit = parseInt(args[++i], 10);
    else if (args[i] === '--test') out.testLimit = 500;
  }
  return out;
}

const DEFAULT_CONCURRENCY = getDefaultConcurrency();
const PROGRESS_EVERY = 5000;
const PROGRESS_INTERVAL_MS = 30000;
/** RUID batch size from available memory (~500 bytes per item); clamp 10k–500k. */
function getDefaultRuidBatchSize() {
  const free = typeof os.freemem === 'function' ? os.freemem() : 4 * 1024 * 1024 * 1024;
  return Math.max(10000, Math.min(500000, Math.floor(free / (500 * 2))));
}
const RUID_BATCH_SIZE = getDefaultRuidBatchSize();

/** Run async tasks with a concurrency limit. */
async function runWithLimit(items, concurrency, runOne) {
  let index = 0;
  async function worker() {
    while (index < items.length) {
      const i = index++;
      const item = items[i];
      try {
        await runOne(item, i);
      } catch (_e) {
        // counted in writeThumb (thumbFail)
      }
    }
  }
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
}

/** Build map: ruid (lowercase) -> absolute path to image. Walks nested Category/Subcategory dirs. */
function buildImageMap(imagesDir) {
  const map = new Map();
  if (!fs.existsSync(imagesDir)) return map;
  function walk(dir) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const ent of entries) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        walk(full);
      } else if (ent.name.endsWith('.png')) {
        const base = ent.name.slice(0, -4);
        const key = base.toLowerCase();
        if (!map.has(key)) map.set(key, full);
      }
    }
  }
  walk(imagesDir);
  return map;
}

async function main() {
  const opts = parseArgs();
  const imagesDir = opts.imagesDir;
  const thumbsDir = opts.thumbsDir;
  const ruidsCsv = opts.ruidsCsv;
  const maxPixels = opts.maxPixels || 128;
  const skipExisting = opts.skipExisting;
  const testLimit = opts.testLimit;
  const concurrency = (opts.concurrency > 0 && opts.concurrency) || DEFAULT_CONCURRENCY;

  if (!imagesDir || !thumbsDir) {
    console.error(
      'Usage: node build-thumbs.js --images-dir <path> --thumbs-dir <path> (--db <path> | --ruids-csv <path>) [--max-pixels 128] [--concurrency 16] [--skip-existing] [--test [N]]'
    );
    process.exit(1);
  }
  if (!opts.db && !ruidsCsv) {
    console.error('Provide either --db or --ruids-csv.');
    process.exit(1);
  }

  if (!fs.existsSync(imagesDir)) {
    console.error('Images dir not found:', imagesDir);
    process.exit(1);
  }
  if (!fs.existsSync(thumbsDir)) fs.mkdirSync(thumbsDir, { recursive: true });

  console.log('Building image map from', imagesDir, '...');
  const imageMap = buildImageMap(imagesDir);
  console.log('Image map:', imageMap.size, 'RUID(s)');

  const IMAGE_ASSET_TYPES = ['sprite', 'damageskin', 'avataritem', 'atlas'];
  let thumbOk = 0;
  let thumbSkip = 0;
  let thumbFail = 0;
  let lastProgress = 0;
  let lastProgressTime = Date.now();
  let totalProcessed = 0;

  async function writeThumb(item) {
    const { src, outPath } = item;
    if (skipExisting && fs.existsSync(outPath)) {
      thumbSkip++;
      return;
    }
    try {
      const meta = await sharp(src).metadata();
      const w = meta.width || 0;
      const h = meta.height || 0;
      const tw = Math.max(1, Math.min(Math.round(w / 2), maxPixels));
      const th = Math.max(1, Math.min(Math.round(h / 2), maxPixels));
      await sharp(src)
        .resize(tw, th)
        .png()
        .toFile(outPath);
      thumbOk++;
    } catch (e) {
      thumbFail++;
    }
    const done = thumbOk + thumbSkip + thumbFail;
    const now = Date.now();
    if (done - lastProgress >= PROGRESS_EVERY || now - lastProgressTime >= PROGRESS_INTERVAL_MS) {
      lastProgress = done;
      lastProgressTime = now;
      console.log('progress: thumbs -', done, 'written/skipped/fail, total RUIDs:', done);
    }
  }

  if (opts.db) {
    // In test mode: drive by image map so every image gets a thumb (avoids "more images than thumbs" from DB order + limit).
    if (testLimit != null) {
      const imageRuids = Array.from(imageMap.keys());
      const limit = Math.min(imageRuids.length, testLimit);
      console.log('Test mode: creating thumbs for', limit, 'image(s) from images dir (one thumb per image, limit', testLimit, ')');
      const workItems = imageRuids.slice(0, limit).map((ruidLower) => {
        const ruid = ruidLower; // use as-is for output filename (DB may have mixed case)
        const src = imageMap.get(ruidLower);
        return src ? { ruid, src, outPath: path.join(thumbsDir, ruidLower + '.png') } : null;
      }).filter(Boolean);
      totalProcessed = workItems.length;
      if (workItems.length > 0) await runWithLimit(workItems, concurrency, writeThumb);
    } else {
      // PGLite (async) — dynamic import since this file is CJS
      const { PGlite } = await import('@electric-sql/pglite');
      const db = await PGlite.create(opts.db);

      // Check if 'assets' table exists
      const { rows: tableCheck } = await db.query(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'assets'"
      );
      const table = tableCheck.length > 0 ? 'assets' : 'catalog_enriched';

      console.log('RUIDs from DB: streaming in batches of', RUID_BATCH_SIZE);

      // Use cursor-based iteration for large result sets
      await db.transaction(async (tx) => {
        await tx.exec(`DECLARE ruid_cursor CURSOR FOR
          SELECT DISTINCT ruid FROM ${table}
          WHERE LOWER(TRIM(asset_type)) IN ('sprite', 'damageskin', 'avataritem', 'atlas')`);

        while (true) {
          const { rows } = await tx.query(`FETCH ${RUID_BATCH_SIZE} FROM ruid_cursor`);
          if (rows.length === 0) break;

          const ruidBatch = rows.map((r) => (r.ruid || '').trim()).filter(Boolean);
          const workItems = ruidBatch
            .map((r) => ({ ruid: r, src: imageMap.get(r.toLowerCase()), outPath: path.join(thumbsDir, r + '.png') }))
            .filter((w) => w.src);
          if (workItems.length > 0) await runWithLimit(workItems, concurrency, writeThumb);
          totalProcessed += ruidBatch.length;
        }

        await tx.exec('CLOSE ruid_cursor');
      });

      await db.close();
    }
  } else {
    console.log('Reading master CSV...');
    const ruidsText = fs.readFileSync(ruidsCsv, 'utf8');
    const ruidsRows = Papa.parse(ruidsText, { header: true, skipEmptyLines: true }).data;
    const imageRows = ruidsRows.filter((r) => IMAGE_ASSET_TYPES.includes((r.AssetType || '').toLowerCase()));
    const seenRuid = new Set();
    const imageRuids = [];
    for (const r of imageRows) {
      const ruid = (r.RUID || '').trim();
      if (ruid && !seenRuid.has(ruid.toLowerCase())) {
        seenRuid.add(ruid.toLowerCase());
        imageRuids.push(ruid);
      }
    }
    console.log('  Loaded', imageRuids.length, 'image RUID(s)');
    let imageLimit = imageRuids.length;
    if (testLimit) {
      imageLimit = Math.min(imageLimit, testLimit);
      console.log('Test: limiting to', imageLimit, 'image RUIDs');
    }
    const workItems = [];
    for (let i = 0; i < imageLimit; i++) {
      const ruid = imageRuids[i];
      const src = imageMap.get(ruid.toLowerCase());
      if (!src) continue;
      workItems.push({ ruid, src, outPath: path.join(thumbsDir, ruid + '.png') });
    }
    totalProcessed = workItems.length;
    console.log('Images: generating thumbs for', workItems.length, 'RUID(s) (concurrency:', concurrency, ')...');
    if (!skipExisting) console.log('  (overwriting existing thumb files)');
    await runWithLimit(workItems, concurrency, writeThumb);
  }

  if (totalProcessed > 0 && (thumbOk + thumbSkip + thumbFail - lastProgress > 0 || lastProgress === 0)) {
    console.log('  thumbs:', thumbOk + thumbSkip + thumbFail, '(total RUIDs:', totalProcessed, ')');
  }
  console.log('Images: done.', thumbOk, 'written,', thumbSkip, 'skipped,', thumbFail, 'failed.');
  console.log('Done. Thumbs ->', thumbsDir);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
