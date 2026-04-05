"use client";
'use strict';

var core = require('@griffel/core');
var insertionFactory = require('./insertionFactory.cjs.js');
var RendererContext = require('./RendererContext.cjs.js');

/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms.
 *
 * @internal
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
function __staticStyles(cssRules) {
  const getStyles = core.__staticStyles(cssRules, insertionFactory.insertionFactory);
  return function useStaticStyles() {
    const renderer = RendererContext.useRenderer();
    return getStyles({
      renderer
    });
  };
}

exports.__staticStyles = __staticStyles;
//# sourceMappingURL=__staticStyles.cjs.js.map
