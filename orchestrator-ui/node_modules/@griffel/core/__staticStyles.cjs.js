'use strict';

var insertionFactory = require('./insertionFactory.cjs.js');

/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms.
 *
 * @internal
 */
function __staticStyles(cssRules, factory = insertionFactory.insertionFactory) {
  const insertStyles = factory();
  function useStaticStyles(options) {
    insertStyles(options.renderer, cssRules);
  }
  return useStaticStyles;
}

exports.__staticStyles = __staticStyles;
//# sourceMappingURL=__staticStyles.cjs.js.map
