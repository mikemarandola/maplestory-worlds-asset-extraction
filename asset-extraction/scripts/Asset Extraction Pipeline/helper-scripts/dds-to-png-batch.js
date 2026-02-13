/**
 * Batch convert DDS or PNG (inside .mod files) to PNG for the asset browser.
 * Supports sprite .win.mod / .dxt.mod (DDS) and .png.mod (PNG). Reads pivot from each .mod using
 * long-header or short-header layout (see SPRITE_PIVOT_AND_OFFSET_EXTRACTION.md). Offset (offsetX, offsetY)
 * is emitted on stdout so the caller can write image-offsets.csv.
 * Optional: --origin-dot — for testing only; expand canvas and composite a red dot at world origin.
 * Usage: node dds-to-png-batch.js <batch.json> [--origin-dot]
 * Stdout: one line per item {"pngPath":"...","ok":true,"offsetX":n,"offsetY":n} or {"pngPath":"...","ok":false}; then {"summary":true,"ok":N,"fail":M}.
 * Prereqs: npm install parse-dds decode-dxt sharp
 */

const fs = require('fs');
const path = require('path');

// Asset-extraction root (helper-scripts -> Asset Extraction Pipeline -> scripts -> asset-extraction)
const projectRoot = path.join(__dirname, '..', '..', '..');
const nodeModules = path.join(projectRoot, 'node_modules');
if (fs.existsSync(nodeModules)) {
  module.paths.unshift(nodeModules);
}

const parseDDS = require('parse-dds');
const decodeDXT = require('decode-dxt');
const sharp = require('sharp');
const { placementFromPivot } = require('./pivot-placement.js');
const { getPivotFromMod } = require('./pivot-from-mod.js');

const DDS_MAGIC = Buffer.from([0x44, 0x44, 0x53, 0x20]); // "DDS "
const PNG_MAGIC = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const PAYLOAD_SEARCH_LIMIT = 2048;

function findDdsOffset(buf) {
  const max = Math.min(PAYLOAD_SEARCH_LIMIT, buf.length - 4);
  for (let i = 0; i < max; i++) {
    if (buf[i] === DDS_MAGIC[0] && buf[i + 1] === DDS_MAGIC[1] && buf[i + 2] === DDS_MAGIC[2] && buf[i + 3] === DDS_MAGIC[3]) {
      return i;
    }
  }
  return -1;
}

function findPngOffset(buf) {
  const max = Math.min(PAYLOAD_SEARCH_LIMIT, buf.length - 8);
  for (let i = 0; i < max; i++) {
    if (buf[i] === PNG_MAGIC[0] && buf[i + 1] === PNG_MAGIC[1] && buf[i + 2] === PNG_MAGIC[2] && buf[i + 3] === PNG_MAGIC[3] &&
        buf[i + 4] === PNG_MAGIC[4] && buf[i + 5] === PNG_MAGIC[5] && buf[i + 6] === PNG_MAGIC[6] && buf[i + 7] === PNG_MAGIC[7]) {
      return i;
    }
  }
  return -1;
}

/** Read width and height from PNG IHDR (at payloadStart: 8-byte sig, then chunk length 4, "IHDR" 4, then width 4 BE, height 4 BE). */
function readPngDimensionsFromIhdr(buffer, payloadStart) {
  if (buffer.length < payloadStart + 24) return null;
  const width = buffer.readUInt32BE(payloadStart + 16);
  const height = buffer.readUInt32BE(payloadStart + 20);
  return width > 0 && height > 0 ? { width, height } : null;
}

/** Returns { ok, reason?, path?, offsetX?, offsetY? }. options.showOriginDot = true for test red-dot canvas. */
async function convertOneWithReason(modPath, pngPath, options = {}) {
  const showOriginDot = options.showOriginDot === true;
  const modPathResolved = path.resolve(modPath);
  if (!fs.existsSync(modPathResolved)) return { ok: false, reason: 'file not found', path: modPathResolved };
  const modBuffer = fs.readFileSync(modPathResolved);

  const ddsOffset = findDdsOffset(modBuffer);
  const pngOffset = findPngOffset(modBuffer);

  let payloadStart;
  let imageWidth;
  let imageHeight;
  let isPngPayload = false;

  if (ddsOffset >= 0) {
    payloadStart = ddsOffset;
    const ddsBuffer = modBuffer.subarray(payloadStart);
    let ddsData;
    try {
      ddsData = parseDDS(ddsBuffer.buffer.slice(ddsBuffer.byteOffset, ddsBuffer.byteOffset + ddsBuffer.byteLength));
    } catch (e) {
      return { ok: false, reason: 'parseDDS: ' + (e.message || e), path: modPathResolved };
    }
    const image = ddsData.images[0];
    if (!image) return { ok: false, reason: 'no images[0] in DDS', path: modPathResolved };
    [imageWidth, imageHeight] = image.shape;
  } else if (pngOffset >= 0) {
    payloadStart = pngOffset;
    isPngPayload = true;
    const dims = readPngDimensionsFromIhdr(modBuffer, payloadStart);
    if (!dims) return { ok: false, reason: 'PNG IHDR not found or invalid dimensions', path: modPathResolved };
    imageWidth = dims.width;
    imageHeight = dims.height;
  } else {
    return { ok: false, reason: 'DDS/PNG magic not found in first ' + PAYLOAD_SEARCH_LIMIT + ' bytes', path: modPathResolved };
  }

  const pivot = getPivotFromMod(modBuffer, payloadStart, imageWidth, imageHeight);
  const pivotX = pivot ? pivot.pivotX : 0.5;
  const pivotY = pivot ? pivot.pivotY : 0.5;
  const { left, top } = placementFromPivot(imageWidth, imageHeight, pivotX, pivotY);
  const offsetX = Math.round(left);
  const offsetY = Math.round(top);

  const outDir = path.dirname(pngPath);
  if (outDir && !fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const pngPathResolved = path.resolve(pngPath);

  try {
    if (isPngPayload) {
      fs.writeFileSync(pngPathResolved, modBuffer.subarray(payloadStart), null);
    } else {
      const ddsBuffer = modBuffer.subarray(payloadStart);
      const ddsData = parseDDS(ddsBuffer.buffer.slice(ddsBuffer.byteOffset, ddsBuffer.byteOffset + ddsBuffer.byteLength));
      const image = ddsData.images[0];
      const [w, h] = image.shape;
      let rgbaData;
      try {
        const imageDataView = new DataView(ddsBuffer.buffer, ddsBuffer.byteOffset + image.offset, image.length);
        rgbaData = decodeDXT(imageDataView, w, h, ddsData.format);
      } catch (e) {
        return { ok: false, reason: 'decodeDXT: ' + (e.message || e), path: modPathResolved };
      }
      const rowBytes = w * 4;
      const flipped = new Uint8Array(rgbaData.length);
      for (let y = 0; y < h; y++) {
        const srcRow = h - 1 - y;
        flipped.set(rgbaData.subarray(srcRow * rowBytes, (srcRow + 1) * rowBytes), y * rowBytes);
      }
      const spriteBuffer = Buffer.from(flipped);
      if (showOriginDot) {
        const right = left + w;
        const bottom = top + h;
        const canvasW = Math.ceil(2 * Math.max(-left, right));
        const canvasH = Math.ceil(2 * Math.max(-top, bottom));
        const x = Math.round(canvasW / 2 + left);
        const y = Math.round(canvasH / 2 + top);
        const DOT_R = 4;
        const DOT_SIZE = DOT_R * 2 + 1;
        const redDotSvg = `<svg width="${DOT_SIZE}" height="${DOT_SIZE}" xmlns="http://www.w3.org/2000/svg"><circle cx="${DOT_R}" cy="${DOT_R}" r="${DOT_R}" fill="red"/></svg>`;
        const redDotBuffer = await sharp(Buffer.from(redDotSvg)).png().toBuffer();
        const dotLeft = Math.round(canvasW / 2 - DOT_R);
        const dotTop = Math.round(canvasH / 2 - DOT_R);
        const spritePng = await sharp(spriteBuffer, { raw: { width: w, height: h, channels: 4 } }).png().toBuffer();
        const canvas = sharp({
          create: { width: canvasW, height: canvasH, channels: 4, background: { r: 0, g: 0, b: 0, alpha: 0 } },
        });
        await canvas
          .composite([
            { input: spritePng, left: x, top: y },
            { input: redDotBuffer, left: dotLeft, top: dotTop },
          ])
          .png()
          .toFile(pngPathResolved);
      } else {
        await sharp(spriteBuffer, { raw: { width: w, height: h, channels: 4 } })
          .png()
          .toFile(pngPathResolved);
      }
    }
  } catch (e) {
    return { ok: false, reason: 'write PNG: ' + (e.message || e), path: pngPath };
  }
  return { ok: true, offsetX, offsetY };
}

async function main() {
  const args = process.argv.slice(2);
  let batchPath = null;
  let showOriginDot = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--origin-dot') showOriginDot = true;
    else if (!batchPath && !args[i].startsWith('-')) batchPath = args[i];
  }
  if (!batchPath) {
    console.error('Usage: node dds-to-png-batch.js <batch.json> [--origin-dot]');
    process.exit(1);
  }
  batchPath = path.resolve(batchPath);
  if (!fs.existsSync(batchPath)) {
    console.error('Batch file not found:', batchPath);
    process.exit(1);
  }
  const batch = JSON.parse(fs.readFileSync(batchPath, 'utf8'));
  if (!Array.isArray(batch)) {
    console.error('batch.json must be an array of { modPath, pngPath }');
    process.exit(1);
  }
  const debug = process.env.DEBUG === '1' || process.env.DEBUG === 'true';
  let ok = 0;
  let fail = 0;
  let firstFailureReported = false;
  for (const item of batch) {
    const modPath = item.modPath || item.ModPath || item.modpath;
    const pngPath = item.pngPath || item.PngPath || item.pngpath;
    if (!modPath || !pngPath) {
      fail++;
      console.log(JSON.stringify({ pngPath: pngPath || '', ok: false }));
      if (debug && !firstFailureReported) {
        console.error('[DEBUG] missing path: modPath=' + !!modPath + ' pngPath=' + !!pngPath + ' keys=' + JSON.stringify(Object.keys(item)));
        firstFailureReported = true;
      }
      continue;
    }
    try {
      const r = await convertOneWithReason(modPath, pngPath, { showOriginDot });
      if (r.ok) {
        ok++;
        console.log(JSON.stringify({ pngPath, ok: true, offsetX: r.offsetX, offsetY: r.offsetY }));
      } else {
        fail++;
        console.log(JSON.stringify({ pngPath, ok: false }));
        if (debug && !firstFailureReported) {
          console.error('[DEBUG] first failure:', r.reason);
          console.error('[DEBUG] path:', r.path);
          firstFailureReported = true;
        }
      }
    } catch (e) {
      fail++;
      console.log(JSON.stringify({ pngPath, ok: false }));
      if (debug && !firstFailureReported) {
        console.error('[DEBUG] exception:', e.message || e);
        console.error('[DEBUG] path:', modPath);
        firstFailureReported = true;
      }
    }
  }
  console.log(JSON.stringify({ summary: true, ok, fail }));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
