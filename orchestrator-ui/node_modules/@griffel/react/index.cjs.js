"use client";
'use strict';

var core = require('@griffel/core');
var makeStyles = require('./makeStyles.cjs.js');
var makeResetStyles = require('./makeResetStyles.cjs.js');
var makeStaticStyles = require('./makeStaticStyles.cjs.js');
var renderToStyleElements = require('./renderToStyleElements.cjs.js');
var RendererContext = require('./RendererContext.cjs.js');
var TextDirectionContext = require('./TextDirectionContext.cjs.js');
var __css = require('./__css.cjs.js');
var __styles = require('./__styles.cjs.js');
var __resetCSS = require('./__resetCSS.cjs.js');
var __resetStyles = require('./__resetStyles.cjs.js');
var __staticCSS = require('./__staticCSS.cjs.js');
var __staticStyles = require('./__staticStyles.cjs.js');



Object.defineProperty(exports, "RESET", {
	enumerable: true,
	get: function () { return core.RESET; }
});
Object.defineProperty(exports, "createDOMRenderer", {
	enumerable: true,
	get: function () { return core.createDOMRenderer; }
});
Object.defineProperty(exports, "mergeClasses", {
	enumerable: true,
	get: function () { return core.mergeClasses; }
});
Object.defineProperty(exports, "shorthands", {
	enumerable: true,
	get: function () { return core.shorthands; }
});
exports.makeStyles = makeStyles.makeStyles;
exports.makeResetStyles = makeResetStyles.makeResetStyles;
exports.makeStaticStyles = makeStaticStyles.makeStaticStyles;
exports.renderToStyleElements = renderToStyleElements.renderToStyleElements;
exports.RendererProvider = RendererContext.RendererProvider;
exports.useRenderer_unstable = RendererContext.useRenderer;
exports.TextDirectionProvider = TextDirectionContext.TextDirectionProvider;
exports.__css = __css.__css;
exports.__styles = __styles.__styles;
exports.__resetCSS = __resetCSS.__resetCSS;
exports.__resetStyles = __resetStyles.__resetStyles;
exports.__staticCSS = __staticCSS.__staticCSS;
exports.__staticStyles = __staticStyles.__staticStyles;
//# sourceMappingURL=index.cjs.js.map
