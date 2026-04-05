import { insertionFactory } from './insertionFactory.esm.js';

/**
 * A version of makeStaticStyles() that accepts build output as an input and skips all runtime transforms.
 *
 * @internal
 */
function __staticStyles(cssRules, factory = insertionFactory) {
  const insertStyles = factory();
  function useStaticStyles(options) {
    insertStyles(options.renderer, cssRules);
  }
  return useStaticStyles;
}

export { __staticStyles };
//# sourceMappingURL=__staticStyles.esm.js.map
