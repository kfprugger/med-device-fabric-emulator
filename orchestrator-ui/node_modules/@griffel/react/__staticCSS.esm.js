"use client";
import { __staticCSS as __staticCSS$1 } from '@griffel/core';

/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms & DOM insertion.
 *
 * @internal
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
function __staticCSS() {
  const getStyles = __staticCSS$1();
  return function useStaticStyles() {
    return getStyles();
  };
}

export { __staticCSS };
//# sourceMappingURL=__staticCSS.esm.js.map
