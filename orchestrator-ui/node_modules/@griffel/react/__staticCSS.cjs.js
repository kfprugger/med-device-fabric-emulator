"use client";
'use strict';

var core = require('@griffel/core');

/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms & DOM insertion.
 *
 * @internal
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
function __staticCSS() {
  const getStyles = core.__staticCSS();
  return function useStaticStyles() {
    return getStyles();
  };
}

exports.__staticCSS = __staticCSS;
//# sourceMappingURL=__staticCSS.cjs.js.map
