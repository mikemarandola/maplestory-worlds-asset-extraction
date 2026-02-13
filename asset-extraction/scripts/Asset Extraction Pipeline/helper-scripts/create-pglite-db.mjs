#!/usr/bin/env node
// create-pglite-db.mjs — Create a PGLite metadata database from final CSVs produced by DuckDB (Step 6 Phase B).
// Usage: node create-pglite-db.mjs --out-dir <path> --staging-dir <path> [--enable-trigram]

import { PGlite } from '@electric-sql/pglite';
import fs from 'node:fs';
import path from 'node:path';

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(name);
  return idx !== -1 && idx + 1 < args.length ? args[idx + 1] : null;
}
const outDir = getArg('--out-dir');
const stagingDir = getArg('--staging-dir');
const enableTrigram = args.includes('--enable-trigram');

if (!outDir || !stagingDir) {
  console.error('Usage: node create-pglite-db.mjs --out-dir <path> --staging-dir <path> [--enable-trigram]');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// CSV file paths (headerless CSVs produced by DuckDB Phase B)
// ---------------------------------------------------------------------------

const csvFiles = {
  tags:              path.join(stagingDir, 'final_tags.csv'),
  asset_tags:        path.join(stagingDir, 'final_asset_tags.csv'),
  assets:            path.join(stagingDir, 'final_assets.csv'),
  animation_frames:  path.join(stagingDir, 'final_animation_frames.csv'),
  cache_locations:   path.join(stagingDir, 'final_cache_locations.csv'),
};

for (const [table, filePath] of Object.entries(csvFiles)) {
  if (!fs.existsSync(filePath)) {
    console.error(`Missing CSV for ${table}: ${filePath}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// DDL — tables only, no indexes (indexes created after bulk load)
// Column order must match the CSV column order from DuckDB Phase B.
// ---------------------------------------------------------------------------

const DDL = `
CREATE TABLE tags (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE asset_tags (
  id INTEGER PRIMARY KEY,
  ruid TEXT NOT NULL,
  tag_id INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(ruid, tag_id),
  FOREIGN KEY (tag_id) REFERENCES tags(id)
);

CREATE TABLE assets (
  id INTEGER PRIMARY KEY,
  ruid TEXT NOT NULL UNIQUE,
  name TEXT,
  asset_type TEXT,
  category TEXT,
  subcategory TEXT,
  path TEXT,
  thumbnail_ruid TEXT,
  has_data INTEGER DEFAULT 0,
  offset_x DOUBLE PRECISION DEFAULT 0,
  offset_y DOUBLE PRECISION DEFAULT 0,
  output_subdir TEXT,
  filename TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE animation_frames (
  id INTEGER PRIMARY KEY,
  clip_ruid TEXT,
  frame_index INTEGER,
  frame_ruid TEXT,
  frame_duration_ms INTEGER,
  image_path TEXT,
  offset_x DOUBLE PRECISION DEFAULT 0,
  offset_y DOUBLE PRECISION DEFAULT 0
);

CREATE TABLE cache_locations (
  ruid TEXT NOT NULL,
  cache_path TEXT,
  asset_type TEXT,
  suffix TEXT
);
`;

// ---------------------------------------------------------------------------
// Indexes — created AFTER bulk data load for performance
// ---------------------------------------------------------------------------

const INDEXES = [
  'CREATE INDEX idx_tags_name ON tags(name)',
  'CREATE INDEX idx_asset_tags_ruid ON asset_tags(ruid)',
  'CREATE INDEX idx_asset_tags_tag_id ON asset_tags(tag_id)',
  'CREATE INDEX idx_assets_ruid ON assets(ruid)',
  'CREATE INDEX idx_assets_asset_type ON assets(asset_type)',
  'CREATE INDEX idx_assets_category ON assets(category)',
  'CREATE INDEX idx_assets_has_data ON assets(has_data)',
  'CREATE INDEX idx_anim_clip ON animation_frames(clip_ruid)',
  'CREATE INDEX idx_anim_frame_ruid ON animation_frames(frame_ruid)',
  'CREATE INDEX idx_anim_image_path ON animation_frames(image_path)',
  'CREATE INDEX idx_cache_locations_ruid ON cache_locations(ruid)',
];

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const t0 = performance.now();

  // Build extensions list
  const extensions = {};
  if (enableTrigram) {
    const { pg_trgm } = await import('@electric-sql/pglite/contrib/pg_trgm');
    extensions.pg_trgm = pg_trgm;
  }

  console.log(`Creating PGLite database at: ${outDir}`);
  const db = await PGlite.create(outDir, { extensions });

  // 1. DDL — create tables (no indexes yet)
  console.log('  Creating tables...');
  await db.exec(DDL);

  // 2. Bulk load CSVs via COPY FROM /dev/blob
  const loadOrder = ['tags', 'asset_tags', 'assets', 'animation_frames', 'cache_locations'];
  for (const table of loadOrder) {
    const csvPath = csvFiles[table];
    const csvData = fs.readFileSync(csvPath);
    const blob = new Blob([csvData]);
    console.log(`  Loading ${table} from ${path.basename(csvPath)} (${(csvData.length / 1024 / 1024).toFixed(1)} MB)...`);
    await db.query(`COPY ${table} FROM '/dev/blob' WITH (FORMAT csv)`, [], { blob });
  }

  // 3. Create B-tree indexes (after bulk load)
  console.log('  Creating indexes...');
  for (const idx of INDEXES) {
    await db.exec(idx);
  }

  // 4. Optional pg_trgm GIN index for fast wildcard tag search
  if (enableTrigram) {
    console.log('  Creating pg_trgm extension and GIN trigram index on tags.name...');
    await db.exec('CREATE EXTENSION IF NOT EXISTS pg_trgm');
    await db.exec('CREATE INDEX idx_tags_name_trgm ON tags USING GIN (name gin_trgm_ops)');
  }

  // 5. Verify — print row counts
  console.log('  Row counts:');
  for (const table of loadOrder) {
    const { rows } = await db.query(`SELECT COUNT(*) AS cnt FROM ${table}`);
    console.log(`    ${table}: ${rows[0].cnt}`);
  }

  // 6. Close
  await db.close();

  const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
  console.log(`PGLite database created successfully in ${elapsed}s.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
