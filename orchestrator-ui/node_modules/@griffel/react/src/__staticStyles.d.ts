import type { CSSRulesByBucket } from '@griffel/core';
/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms.
 *
 * @internal
 */
export declare function __staticStyles(cssRules: CSSRulesByBucket): () => void;
