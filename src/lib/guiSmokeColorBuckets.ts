export type Rgb = { r: number; g: number; b: number };

export type SampleGrid = {
  insetX: number;
  insetTop: number;
  insetBottom: number;
  step: number;
  bucketSize: number;
  minBuckets?: number;
};

/**
 * Windows release GUI smoke sampling. Keep in sync with
 * `scripts/windows-gui-smoke.ps1`.
 *
 * GitHub-hosted Windows runners use a light OS theme, so the no-project Home
 * empty state is a sparse light canvas. The application chrome (tabs, IDE
 * button) lives in the top ~56px because Windows uses app-owned window
 * controls. Sampling from y=72 therefore only hits the empty canvas.
 */
export const GUI_SMOKE_SAMPLE = {
  insetX: 16,
  insetTop: 16,
  insetBottom: 16,
  step: 16,
  bucketSize: 8,
  minBuckets: 6,
} as const satisfies SampleGrid & { minBuckets: number };

export function colorBucket(pixel: Rgb, bucketSize = GUI_SMOKE_SAMPLE.bucketSize): string {
  return `${Math.floor(pixel.r / bucketSize)},${Math.floor(pixel.g / bucketSize)},${Math.floor(pixel.b / bucketSize)}`;
}

export function countSampledColorBuckets(
  getPixel: (x: number, y: number) => Rgb,
  width: number,
  height: number,
  sample: SampleGrid = GUI_SMOKE_SAMPLE,
): number {
  const colors = new Set<string>();
  for (let x = sample.insetX; x < width - sample.insetX; x += sample.step) {
    for (let y = sample.insetTop; y < height - sample.insetBottom; y += sample.step) {
      colors.add(colorBucket(getPixel(x, y), sample.bucketSize));
    }
  }
  return colors.size;
}
