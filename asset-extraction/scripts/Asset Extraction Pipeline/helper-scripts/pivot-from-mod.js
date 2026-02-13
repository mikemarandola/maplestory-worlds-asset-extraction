/**
 * Read normalized pivot (pivotX, pivotY) from sprite .mod header.
 * Supports two header layouts per SPRITE_PIVOT_AND_OFFSET_EXTRACTION.md:
 * - Long header: pivot at k-9 / k-4 relative to pattern 2D 00 00 C8 42.
 * - Short header: payload 51–60 bytes, implausible long-header pivotX → pivotX = byte[30]/width, pivotY = 0.
 *
 * Worker-safe: pure function, no shared state or I/O. Safe for single or multiple Node processes
 * (e.g. step 4 running many dds-to-png-batch.js processes in parallel).
 *
 * @param {Buffer} buffer - Full .mod file buffer
 * @param {number} payloadStart - Offset of image payload (DDS or PNG magic)
 * @param {number} width - Sprite width (pixels); required for short-header pivotX
 * @param {number} height - Sprite height (unused; for API consistency)
 * @returns {{ pivotX: number, pivotY: number } | null} Normalized pivot (pivotY from bottom), or null if not found
 */
const PIVOT_PATTERN = Buffer.from([0x2d, 0x00, 0x00, 0xc8, 0x42]);

function getPivotFromMod(buffer, payloadStart, width, height) {
  if (payloadStart < 12 || buffer.length < payloadStart) return null;

  const searchEnd = Math.min(payloadStart - 5, 2048);

  let pivotX = 0.5;
  let pivotY = 0.5;
  let foundLong = false;

  for (let k = 12; k <= searchEnd; k++) {
    if (
      buffer[k] === PIVOT_PATTERN[0] &&
      buffer[k + 1] === PIVOT_PATTERN[1] &&
      buffer[k + 2] === PIVOT_PATTERN[2] &&
      buffer[k + 3] === PIVOT_PATTERN[3] &&
      buffer[k + 4] === PIVOT_PATTERN[4]
    ) {
      if (k >= 9) {
        pivotX = buffer.readFloatLE(k - 9);
        pivotY = buffer.readFloatLE(k - 4);
        foundLong = true;
        break;
      }
    }
  }

  // Short header: payload 51–60 and long-header pivotX implausible (denormal/tiny)
  if (
    foundLong &&
    payloadStart >= 51 &&
    payloadStart <= 60 &&
    Math.abs(pivotX) < 0.001 &&
    buffer.length >= 38 &&
    width > 0
  ) {
    pivotX = buffer[30] / width;
    pivotY = 0;
  }

  return foundLong ? { pivotX, pivotY } : null;
}

module.exports = { getPivotFromMod };
