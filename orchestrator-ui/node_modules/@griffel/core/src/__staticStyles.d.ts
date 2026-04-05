import type { CSSRulesByBucket, GriffelInsertionFactory } from './types';
import type { MakeStaticStylesOptions } from './makeStaticStyles';
/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms.
 *
 * @internal
 */
export declare function __staticStyles(cssRules: CSSRulesByBucket, factory?: GriffelInsertionFactory): (options: MakeStaticStylesOptions) => void;
