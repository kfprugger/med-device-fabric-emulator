'use client';
"use strict";
Object.defineProperty(exports, "__esModule", {
    value: true
});
function _export(target, all) {
    for(var name in all)Object.defineProperty(target, name, {
        enumerable: true,
        get: all[name]
    });
}
_export(exports, {
    getPlacementSlideDirections: function() {
        return getPlacementSlideDirections;
    },
    usePositioningSlideDirection: function() {
        return usePositioningSlideDirection;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactutilities = require("@fluentui/react-utilities");
const _constants = require("./constants");
function getPlacementSlideDirections(placement) {
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
function usePositioningSlideDirection(options) {
    const { targetDocument, onPositioningEnd } = options;
    const handlePositionEnd = (0, _reactutilities.useEventCallback)((e)=>{
        onPositioningEnd === null || onPositioningEnd === void 0 ? void 0 : onPositioningEnd(e);
        const element = e.target;
        const placement = e.detail.placement;
        if (!(0, _reactutilities.isHTMLElement)(element)) {
            return;
        }
        const { x, y } = getPlacementSlideDirections(placement);
        element.style.setProperty(_constants.POSITIONING_SLIDE_DIRECTION_VAR_X, `${x}px`);
        element.style.setProperty(_constants.POSITIONING_SLIDE_DIRECTION_VAR_Y, `${y}px`);
    });
    // Register the CSS custom properties so they can be interpolated during animations.
    // CSS.registerProperty is idempotent — the try/catch handles the case where
    // properties are already registered.
    _react.useEffect(()=>{
        var _targetDocument_defaultView_CSS, _targetDocument_defaultView;
        var _targetDocument_defaultView_CSS_registerProperty;
        const registerProperty = (_targetDocument_defaultView_CSS_registerProperty = targetDocument === null || targetDocument === void 0 ? void 0 : (_targetDocument_defaultView = targetDocument.defaultView) === null || _targetDocument_defaultView === void 0 ? void 0 : (_targetDocument_defaultView_CSS = _targetDocument_defaultView.CSS) === null || _targetDocument_defaultView_CSS === void 0 ? void 0 : _targetDocument_defaultView_CSS.registerProperty) !== null && _targetDocument_defaultView_CSS_registerProperty !== void 0 ? _targetDocument_defaultView_CSS_registerProperty : ()=>{
        // No-op if registerProperty is not supported
        };
        try {
            registerProperty({
                name: _constants.POSITIONING_SLIDE_DIRECTION_VAR_X,
                syntax: '<length>',
                inherits: false,
                initialValue: '0px'
            });
            registerProperty({
                name: _constants.POSITIONING_SLIDE_DIRECTION_VAR_Y,
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
