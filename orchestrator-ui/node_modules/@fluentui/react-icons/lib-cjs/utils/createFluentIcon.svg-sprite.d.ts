import * as React from 'react';
import type { FluentIconsProps } from './FluentIconsProps.types';
export declare type FluentIcon = React.FC<FluentIconsProps>;
declare type CreateFluentIconOptions = {
    flipInRtl?: boolean;
    color?: boolean;
};
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
export declare const createFluentIcon: (iconId: string, size: string, spritePath?: string | undefined, options?: CreateFluentIconOptions | undefined) => FluentIcon;
export {};
