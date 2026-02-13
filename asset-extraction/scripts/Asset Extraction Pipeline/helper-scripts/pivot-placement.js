/**
 * Canonical pivot placement for ALL sprite and animation image renders.
 * Use this module wherever we composite sprites or animation frames so placement is consistent.
 *
 * MSW pivot: pivotX from left (0–1). pivotY from BOTTOM (0=bottom, 1=top).
 * Anchor in texture: (pivotX*w, h - pivotY*h) from top-left.
 * To place anchor at world (0,0): left = -pivotX*w, top = (pivotY - 1)*h.
 */

/**
 * @param {number} w - width of sprite/frame
 * @param {number} h - height of sprite/frame
 * @param {number} pivotX - from left (0–1)
 * @param {number} pivotY - from bottom (0=bottom, 1=top)
 * @returns {{ left: number, top: number }} position so anchor is at world (0,0)
 */
function placementFromPivot(w, h, pivotX, pivotY) {
  return {
    left: -pivotX * w,
    top: (pivotY - 1) * h,
  };
}

module.exports = { placementFromPivot };
