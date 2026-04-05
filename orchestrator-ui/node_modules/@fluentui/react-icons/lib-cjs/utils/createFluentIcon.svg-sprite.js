"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createFluentIcon = void 0;
const tslib_1 = require("tslib");
const React = tslib_1.__importStar(require("react"));
const react_1 = require("@griffel/react");
const useIconState_1 = require("./useIconState");
const createFluentIcon_styles_1 = require("./createFluentIcon.styles");
const constants_1 = require("./constants");
/**
 * Creates a React component for a Fluent icon that references an SVG symbol from a sprite.
 *
 * @access private
 * @alpha
 *
 * @param iconId - The SVG symbol id in the sprite sheet.
 * @param size - The icon size (for example, `"1em"` or a numeric string).
 * @param spritePath - Optional path/URL to the SVG sprite file. If omitted, an in-document symbol reference is used.
 * @param options - Optional creation settings (for example RTL flipping and color behavior).
 * @returns A Fluent icon React component.
 */
const createFluentIcon = (iconId, size, spritePath, options) => {
    const viewBoxWidth = size === '1em' ? '20' : size;
    const Icon = React.forwardRef((props, ref) => {
        const styles = createFluentIcon_styles_1.useRootStyles();
        const iconState = useIconState_1.useIconState(props, { flipInRtl: options === null || options === void 0 ? void 0 : options.flipInRtl }); // HTML attributes/props for things like accessibility can be passed in, and will be expanded on the svg object at the start of the object
        const state = {
            ...iconState,
            className: react_1.mergeClasses(constants_1.iconClassName, iconState.className, styles.root),
            ref,
            width: size,
            height: size,
            viewBox: `0 0 ${viewBoxWidth} ${viewBoxWidth}`,
            xmlns: 'http://www.w3.org/2000/svg',
        };
        const href = spritePath ? `${spritePath}#${iconId}` : `#${iconId}`;
        return React.createElement('svg', state, React.createElement('use', {
            href,
        }));
    });
    Icon.displayName = iconId;
    return Icon;
};
exports.createFluentIcon = createFluentIcon;
