import * as React from 'react';
import { mergeClasses } from '@griffel/react';
import { useIconState } from './useIconState';
import { useRootStyles } from './createFluentIcon.styles';
import { iconClassName } from './constants';
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
export const createFluentIcon = (iconId, size, spritePath, options) => {
    const viewBoxWidth = size === '1em' ? '20' : size;
    const Icon = React.forwardRef((props, ref) => {
        const styles = useRootStyles();
        const iconState = useIconState(props, { flipInRtl: options === null || options === void 0 ? void 0 : options.flipInRtl }); // HTML attributes/props for things like accessibility can be passed in, and will be expanded on the svg object at the start of the object
        const state = {
            ...iconState,
            className: mergeClasses(iconClassName, iconState.className, styles.root),
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
