/**
 * Batch extract images from .mod for damageskin, avataritem, atlas (non-sprite image types).
 * Every type supports long-header and short-header pivot (getPivotFromMod); offsets written to image-offsets.csv.
 * - damageskin: payload at offset 3488 (PNG or DDS); search limit ≥ 4096. PNG: read IHDR for dimensions, pivot from header.
 * - avataritem: PNG or DDS in first 8192 bytes. PNG: IHDR + pivot. DDS: decode + pivot.
 * - atlas: DDS at offset 847 or search 0–4096; PNG in first 8192 bytes. Same pivot/offset for both.
 * Output: PNG only. Offset (offsetX, offsetY) emitted on stdout for image-offsets.csv.
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

function findPngOffset(buf, searchLimit) {
  const max = Math.min(searchLimit, buf.length - 8);
  for (let i = 0; i < max; i++) {
    if (buf[i] === PNG_MAGIC[0] && buf[i + 1] === PNG_MAGIC[1] && buf[i + 2] === PNG_MAGIC[2] && buf[i + 3] === PNG_MAGIC[3] &&
        buf[i + 4] === PNG_MAGIC[4] && buf[i + 5] === PNG_MAGIC[5] && buf[i + 6] === PNG_MAGIC[6] && buf[i + 7] === PNG_MAGIC[7]) {
      return i;
    }
  }
  return -1;
}

function findDdsOffsetInRange(buf, start, limit) {
  const end = Math.min(start + limit, buf.length - 4);
  for (let i = start; i < end; i++) {
    if (buf[i] === DDS_MAGIC[0] && buf[i + 1] === DDS_MAGIC[1] && buf[i + 2] === DDS_MAGIC[2] && buf[i + 3] === DDS_MAGIC[3]) {
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

/** Return { offsetX, offsetY } from pivot in .mod header (long or short header per getPivotFromMod), or null. */
function getOffsetFromPivot(modBuffer, payloadStart, width, height) {
  const pivot = getPivotFromMod(modBuffer, payloadStart, width, height);
  if (!pivot) return null;
  const { left, top } = placementFromPivot(width, height, pivot.pivotX, pivot.pivotY);
  return { offsetX: Math.round(left), offsetY: Math.round(top) };
}

/** Decode DDS buffer to RGBA and flip Y; return { rgbaBuffer, width, height }. */
function decodeDdsToRgba(ddsBuffer) {
  const ddsData = parseDDS(ddsBuffer.buffer.slice(ddsBuffer.byteOffset, ddsBuffer.byteOffset + ddsBuffer.byteLength));
  const image = ddsData.images[0];
  if (!image) return null;
  const [imageWidth, imageHeight] = image.shape;
  const imageDataView = new DataView(ddsBuffer.buffer, ddsBuffer.byteOffset + image.offset, image.length);
  const rgbaData = decodeDXT(imageDataView, imageWidth, imageHeight, ddsData.format);
  const rowBytes = imageWidth * 4;
  const flipped = new Uint8Array(rgbaData.length);
  for (let y = 0; y < imageHeight; y++) {
    const srcRow = imageHeight - 1 - y;
    flipped.set(rgbaData.subarray(srcRow * rowBytes, (srcRow + 1) * rowBytes), y * rowBytes);
  }
  return { rgbaBuffer: Buffer.from(flipped), width: imageWidth, height: imageHeight };
}

/** Returns { ok, reason?, path?, offsetX?, offsetY? }. imageType: damageskin | avataritem | atlas */
async function convertOne(modPath, pngPath, imageType) {
  const modPathResolved = path.resolve(modPath);
  if (!fs.existsSync(modPathResolved)) return { ok: false, reason: 'file not found', path: modPathResolved };
  const modBuffer = fs.readFileSync(modPathResolved);
  const type = (imageType || '').toLowerCase();
  let payloadStart = -1;
  let isPng = false;
  let offsetX = 0;
  let offsetY = 0;

  if (type === 'damageskin') {
    const at = 3488;
    if (modBuffer.length < at + 8) return { ok: false, reason: 'file too short for damageskin offset 3488', path: modPathResolved };
    if (modBuffer[at] === PNG_MAGIC[0] && modBuffer[at + 1] === PNG_MAGIC[1] && modBuffer[at + 2] === PNG_MAGIC[2] && modBuffer[at + 3] === PNG_MAGIC[3]) {
      payloadStart = at;
      isPng = true;
      const dims = readPngDimensionsFromIhdr(modBuffer, payloadStart);
      if (dims) {
        const o = getOffsetFromPivot(modBuffer, payloadStart, dims.width, dims.height);
        if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
      }
    } else {
      const ddsAt = findDdsOffsetInRange(modBuffer, at, 4096);
      if (ddsAt >= 0) {
        payloadStart = ddsAt;
        isPng = false;
        const decoded = decodeDdsToRgba(modBuffer.subarray(payloadStart));
        if (decoded) {
          const o = getOffsetFromPivot(modBuffer, payloadStart, decoded.width, decoded.height);
          if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
        }
      }
    }
  } else if (type === 'avataritem') {
    payloadStart = findPngOffset(modBuffer, 8192);
    if (payloadStart >= 0) {
      isPng = true;
      const dims = readPngDimensionsFromIhdr(modBuffer, payloadStart);
      if (dims) {
        const o = getOffsetFromPivot(modBuffer, payloadStart, dims.width, dims.height);
        if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
      }
    } else {
      const ddsAt = findDdsOffsetInRange(modBuffer, 0, 8192);
      if (ddsAt >= 0) {
        payloadStart = ddsAt;
        isPng = false;
        const decoded = decodeDdsToRgba(modBuffer.subarray(payloadStart));
        if (decoded) {
          const o = getOffsetFromPivot(modBuffer, payloadStart, decoded.width, decoded.height);
          if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
        }
      }
    }
  } else if (type === 'atlas') {
    const at = 847;
    if (modBuffer.length >= at + 4 && modBuffer[at] === DDS_MAGIC[0] && modBuffer[at + 1] === DDS_MAGIC[1] && modBuffer[at + 2] === DDS_MAGIC[2] && modBuffer[at + 3] === DDS_MAGIC[3]) {
      payloadStart = at;
      isPng = false;
      const decoded = decodeDdsToRgba(modBuffer.subarray(payloadStart));
      if (decoded) {
        const o = getOffsetFromPivot(modBuffer, payloadStart, decoded.width, decoded.height);
        if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
      }
    }
    if (payloadStart < 0) {
      const ddsAt = findDdsOffsetInRange(modBuffer, 0, 4096);
      if (ddsAt >= 0) {
        payloadStart = ddsAt;
        isPng = false;
        const decoded = decodeDdsToRgba(modBuffer.subarray(payloadStart));
        if (decoded) {
          const o = getOffsetFromPivot(modBuffer, payloadStart, decoded.width, decoded.height);
          if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
        }
      }
    }
    if (payloadStart < 0) {
      const pngAt = findPngOffset(modBuffer, 8192);
      if (pngAt >= 0) {
        payloadStart = pngAt;
        isPng = true;
        const dims = readPngDimensionsFromIhdr(modBuffer, payloadStart);
        if (dims) {
          const o = getOffsetFromPivot(modBuffer, payloadStart, dims.width, dims.height);
          if (o) { offsetX = o.offsetX; offsetY = o.offsetY; }
        }
      }
    }
  } else {
    return { ok: false, reason: 'unsupported imageType: ' + imageType, path: modPathResolved };
  }

  if (payloadStart < 0) {
    const msg = type === 'avataritem' ? 'PNG/DDS not found in first 8192 bytes' : type === 'atlas' ? 'PNG/DDS payload not found for atlas' : type + ' payload not found';
    return { ok: false, reason: msg, path: modPathResolved };
  }

  const outDir = path.dirname(pngPath);
  if (outDir && !fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const pngPathResolved = path.resolve(pngPath);

  try {
    if (isPng) {
      fs.writeFileSync(pngPathResolved, modBuffer.subarray(payloadStart), null);
    } else {
      const decoded = decodeDdsToRgba(modBuffer.subarray(payloadStart));
      if (!decoded) return { ok: false, reason: 'DDS decode failed', path: modPathResolved };
      await sharp(decoded.rgbaBuffer, { raw: { width: decoded.width, height: decoded.height, channels: 4 } })
        .png()
        .toFile(pngPathResolved);
    }
  } catch (e) {
    return { ok: false, reason: 'write: ' + (e.message || e), path: pngPath };
  }
  return { ok: true, offsetX, offsetY };
}

async function main() {
  const args = process.argv.slice(2);
  let batchPath = null;
  for (let i = 0; i < args.length; i++) {
    if (!batchPath && !args[i].startsWith('-')) batchPath = args[i];
  }
  if (!batchPath) {
    console.error('Usage: node extract-image-batch.js <batch.json>');
    console.error('  batch.json: array of { modPath, pngPath, imageType } where imageType is damageskin | avataritem | atlas');
    process.exit(1);
  }
  batchPath = path.resolve(batchPath);
  if (!fs.existsSync(batchPath)) {
    console.error('Batch file not found:', batchPath);
    process.exit(1);
  }
  const batch = JSON.parse(fs.readFileSync(batchPath, 'utf8'));
  if (!Array.isArray(batch)) {
    console.error('batch.json must be an array of { modPath, pngPath, imageType }');
    process.exit(1);
  }
  const debug = process.env.DEBUG === '1' || process.env.DEBUG === 'true';
  let ok = 0;
  let fail = 0;
  let firstFailureReported = false;
  for (const item of batch) {
    const modPath = item.modPath || item.ModPath || item.modpath;
    const pngPath = item.pngPath || item.PngPath || item.pngpath;
    const imageType = item.imageType || item.ImageType || item.imagetype || 'damageskin';
    if (!modPath || !pngPath) {
      fail++;
      console.log(JSON.stringify({ pngPath: pngPath || '', ok: false }));
      if (debug && !firstFailureReported) {
        console.error('[DEBUG] missing path:', Object.keys(item));
        firstFailureReported = true;
      }
      continue;
    }
    try {
      const r = await convertOne(modPath, pngPath, imageType);
      if (r.ok) {
        ok++;
        console.log(JSON.stringify({ pngPath, ok: true, offsetX: r.offsetX, offsetY: r.offsetY }));
      } else {
        fail++;
        console.log(JSON.stringify({ pngPath, ok: false }));
        if (debug && !firstFailureReported) {
          console.error('[DEBUG]', r.reason, r.path);
          firstFailureReported = true;
        }
      }
    } catch (e) {
      fail++;
      console.log(JSON.stringify({ pngPath, ok: false }));
      if (debug && !firstFailureReported) {
        console.error('[DEBUG] exception:', e.message, modPath);
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
