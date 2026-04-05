"use client";
import { __staticStyles as __staticStyles$1 } from '@griffel/core';
import { insertionFactory } from './insertionFactory.esm.js';
import { useRenderer } from './RendererContext.esm.js';

/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms.
 *
 * @internal
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
function __staticStyles(cssRules) {
  const getStyles = __staticStyles$1(cssRules, insertionFactory);
  return function useStaticStyles() {
    const renderer = useRenderer();
    return getStyles({
      renderer
    });
  };
}

export { __staticStyles };
//# sourceMappingURL=__staticStyles.esm.js.map
