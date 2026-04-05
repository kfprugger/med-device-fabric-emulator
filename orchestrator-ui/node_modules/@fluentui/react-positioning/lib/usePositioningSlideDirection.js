'use client';
import * as React from 'react';
import { useEventCallback, isHTMLElement } from '@fluentui/react-utilities';
import { POSITIONING_SLIDE_DIRECTION_VAR_X, POSITIONING_SLIDE_DIRECTION_VAR_Y } from './constants';
/**
 * Returns the slide direction unit vectors for a given Floating UI placement.
 * Values are -1, 0, or 1, representing the direction the element slides in from.
 */ export function getPlacementSlideDirections(placement) {
    const side = placement.split('-')[0];
    // Default to sliding down from the top side
    let x = 0;
    let y = 1;
    if (side === 'right') {
        x = -1;
        y = 0;
    } else if (side === 'bottom') {
        x = 0;
        y = -1;
    } else if (side === 'left') {
        x = 1;
        y = 0;
    }
    return {
        x,
        y
    };
}
/**
 * A hook that manages CSS custom properties for slide direction based on positioning placement.
 *
 * It wraps the `onPositioningEnd` callback to set `--fui-positioning-slide-direction-x` and
 * `--fui-positioning-slide-direction-y` CSS custom properties on the positioned element,
 * and registers them via `CSS.registerProperty` to avoid properties propagation down to a DOM tree.
 *
 * @returns The wrapped `onPositioningEnd` handler to pass to the positioning config.
 */ export function usePositioningSlideDirection(options) {
    const { targetDocument, onPositioningEnd } = options;
    const handlePositionEnd = useEventCallback((e)=>{
        onPositioningEnd === null || onPositioningEnd === void 0 ? void 0 : onPositioningEnd(e);
        const element = e.target;
        const placement = e.detail.placement;
        if (!isHTMLElement(element)) {
            return;
        }
        const { x, y } = getPlacementSlideDirections(placement);
        element.style.setProperty(POSITIONING_SLIDE_DIRECTION_VAR_X, `${x}px`);
        element.style.setProperty(POSITIONING_SLIDE_DIRECTION_VAR_Y, `${y}px`);
    });
    // Register the CSS custom properties so they can be interpolated during animations.
    // CSS.registerProperty is idempotent — the try/catch handles the case where
    // properties are already registered.
    React.useEffect(()=>{
        var _targetDocument_defaultView_CSS, _targetDocument_defaultView;
        var _targetDocument_defaultView_CSS_registerProperty;
        const registerProperty = (_targetDocument_defaultView_CSS_registerProperty = targetDocument === null || targetDocument === void 0 ? void 0 : (_targetDocument_defaultView = targetDocument.defaultView) === null || _targetDocument_defaultView === void 0 ? void 0 : (_targetDocument_defaultView_CSS = _targetDocument_defaultView.CSS) === null || _targetDocument_defaultView_CSS === void 0 ? void 0 : _targetDocument_defaultView_CSS.registerProperty) !== null && _targetDocument_defaultView_CSS_registerProperty !== void 0 ? _targetDocument_defaultView_CSS_registerProperty : ()=>{
        // No-op if registerProperty is not supported
        };
        try {
            registerProperty({
                name: POSITIONING_SLIDE_DIRECTION_VAR_X,
                syntax: '<length>',
                inherits: false,
                initialValue: '0px'
            });
            registerProperty({
                name: POSITIONING_SLIDE_DIRECTION_VAR_Y,
                syntax: '<length>',
                inherits: false,
                initialValue: '0px'
            });
        } catch (e) {
        // Ignore errors from registerProperty, which can occur if the properties are already registered
        }
    }, [
        targetDocument
    ]);
    return handlePositionEnd;
}
