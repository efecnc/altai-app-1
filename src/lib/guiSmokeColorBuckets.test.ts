import { describe, expect, it } from "vitest";
import {
  GUI_SMOKE_SAMPLE,
  countSampledColorBuckets,
  type Rgb,
  type SampleGrid,
} from "./guiSmokeColorBuckets";

const LEGACY_SAMPLE: SampleGrid = {
  insetX: 24,
  insetTop: 72,
  insetBottom: 24,
  step: 32,
  bucketSize: 8,
};

function makeBuffer(
  width: number,
  height: number,
  fill: (x: number, y: number) => Rgb,
): { width: number; height: number; getPixel: (x: number, y: number) => Rgb } {
  const pixels = new Array<Rgb>(width * height);
  for (let y = 0; y < height; y += 1) {
    for (let x = 0; x < width; x += 1) {
      pixels[y * width + x] = fill(x, y);
    }
  }
  return {
    width,
    height,
    getPixel: (x, y) => pixels[y * width + x]!,
  };
}

/** Light-theme Home empty state: chrome in the top ~56px, sparse canvas below. */
function sparseLightHome() {
  return makeBuffer(816, 609, (x, y) => {
    if (y < 56) {
      if (x < 80) return { r: 12, g: 12, b: 12 };
      if (x < 160) return { r: 230, g: 230, b: 230 };
      if (x < 240) return { r: 200, g: 200, b: 200 };
      if (x < 320) return { r: 136, g: 48, b: 16 };
      if (x < 400) return { r: 110, g: 110, b: 110 };
      if (x < 480) return { r: 48, g: 96, b: 160 };
      return { r: 245, g: 245, b: 245 };
    }
    if (x === 24 && y === 72) return { r: 224, g: 224, b: 232 };
    if (x === 24 && y === 104) return { r: 136, g: 48, b: 16 };
    return { r: 240, g: 240, b: 240 };
  });
}

describe("countSampledColorBuckets", () => {
  it("rejects the legacy grid on a light-theme Home empty state", () => {
    const image = sparseLightHome();
    expect(
      countSampledColorBuckets(image.getPixel, image.width, image.height, LEGACY_SAMPLE),
    ).toBeLessThan(GUI_SMOKE_SAMPLE.minBuckets);
  });

  it("accepts the same Home empty state when the grid includes app chrome", () => {
    const image = sparseLightHome();
    expect(
      countSampledColorBuckets(image.getPixel, image.width, image.height),
    ).toBeGreaterThanOrEqual(GUI_SMOKE_SAMPLE.minBuckets);
  });

  it("still rejects a blank compositor", () => {
    const image = makeBuffer(816, 609, () => ({ r: 240, g: 240, b: 240 }));
    expect(
      countSampledColorBuckets(image.getPixel, image.width, image.height),
    ).toBeLessThan(GUI_SMOKE_SAMPLE.minBuckets);
  });

  it("still rejects native chrome over an unpainted viewport", () => {
    const image = makeBuffer(816, 609, (_x, y) =>
      y < 40 ? { r: 12, g: 12, b: 12 } : { r: 240, g: 240, b: 240 },
    );
    expect(
      countSampledColorBuckets(image.getPixel, image.width, image.height),
    ).toBeLessThan(GUI_SMOKE_SAMPLE.minBuckets);
  });
});
