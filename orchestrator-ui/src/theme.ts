/**
 * Fabric-aligned Fluent 2 theme configuration.
 *
 * Per the Fabric UX System and Fluent 2 design system:
 * - Use semantic tokens, never raw hex values
 * - Support both light and dark modes automatically
 * - Brand colors used sparingly (accents only, not large surfaces)
 * - Neutral palette for surfaces, containers, text hierarchy
 * - Semantic colors only to convey meaning (success, warning, error)
 *
 * This module defines a custom Fabric-aligned brand ramp
 * and spacing tokens that match Fabric's visual density.
 */

import {
  createLightTheme,
  createDarkTheme,
  type BrandVariants,
  type Theme,
  tokens,
} from "@fluentui/react-components";

/**
 * Fabric brand color ramp.
 *
 * Based on Microsoft Fabric's teal-blue brand accent (#117865).
 * Generated to align with the Fluent 2 brand variant scale (10–160).
 * Used sparingly per the theming guide: accent highlights, interactive
 * elements, and small brand anchors only.
 */
const fabricBrand: BrandVariants = {
  10: "#020E0C",
  20: "#072A24",
  30: "#0A3F36",
  40: "#0D5447",
  50: "#106A58",
  60: "#117865",
  70: "#148E77",
  80: "#19A48A",
  90: "#2AB89C",
  100: "#45C8AC",
  110: "#62D4BB",
  120: "#81DECA",
  130: "#A0E8D8",
  140: "#BFF0E5",
  150: "#DDF8F1",
  160: "#F0FCF9",
};

/**
 * Light theme — Fabric-aligned.
 * Uses neutral backgrounds for surfaces (not brand).
 */
export const fabricLightTheme: Theme = {
  ...createLightTheme(fabricBrand),
};

/**
 * Dark theme — Fabric-aligned.
 * Automatically inverts neutrals and adjusts brand for dark surfaces.
 */
export const fabricDarkTheme: Theme = {
  ...createDarkTheme(fabricBrand),
};

/**
 * Fluent 2 spacing scale.
 * Use these instead of raw pixel values to maintain Fabric's visual density.
 *
 * Reference: tokens.spacingHorizontalXXS through spacingHorizontalXXXL
 * These are already available via Fluent tokens, but aliased here
 * for quick reference in component styles.
 */
export const spacing = {
  /** 2px */ xxs: tokens.spacingHorizontalXXS,
  /** 4px */ xs: tokens.spacingHorizontalXS,
  /** 6px */ sNudge: tokens.spacingHorizontalSNudge,
  /** 8px */ s: tokens.spacingHorizontalS,
  /** 10px */ mNudge: tokens.spacingHorizontalMNudge,
  /** 12px */ m: tokens.spacingHorizontalM,
  /** 16px */ l: tokens.spacingHorizontalL,
  /** 20px */ xl: tokens.spacingHorizontalXL,
  /** 24px */ xxl: tokens.spacingHorizontalXXL,
  /** 32px */ xxxl: tokens.spacingHorizontalXXXL,
} as const;
