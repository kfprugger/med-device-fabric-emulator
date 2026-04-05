"use strict";
Object.defineProperty(exports, "__esModule", {
    value: true
});
Object.defineProperty(exports, "motionSlot", {
    enumerable: true,
    get: function() {
        return motionSlot;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactutilities = require("@fluentui/react-utilities");
function motionSlot(motion, options) {
    // eslint-disable-next-line @typescript-eslint/no-deprecated
    const { as, children, ...rest } = motion !== null && motion !== void 0 ? motion : {};
    if (process.env.NODE_ENV !== 'production') {
        if (typeof as !== 'undefined') {
            throw new Error(`@fluentui/react-motion: "as" property is not supported on motion slots.`);
        }
    }
    if (motion === null) {
        // Heads up!
        // Render function is used there to avoid rendering a motion component and render children directly
        const renderFn = (_, props)=>/*#__PURE__*/ _react.createElement(_react.Fragment, null, props.children);
        /**
     * Casting is required here as SlotComponentType is a function, not an object.
     * Although SlotComponentType has a function signature, it is still just an object.
     * This is required to make a slot callable (JSX compatible), this is the exact same approach
     * that is used on `@types/react` components
     */ return {
            [_reactutilities.SLOT_RENDER_FUNCTION_SYMBOL]: renderFn,
            [_reactutilities.SLOT_ELEMENT_TYPE_SYMBOL]: options.elementType
        };
    }
    /**
   * Casting is required here as SlotComponentType is a function, not an object.
   * Although SlotComponentType has a function signature, it is still just an object.
   * This is required to make a slot callable (JSX compatible), this is the exact same approach
   * that is used on `@types/react` components
   */ const propsWithMetadata = {
        ...options.defaultProps,
        ...rest,
        [_reactutilities.SLOT_ELEMENT_TYPE_SYMBOL]: options.elementType
    };
    if (typeof children === 'function') {
        propsWithMetadata[_reactutilities.SLOT_RENDER_FUNCTION_SYMBOL] = children;
    }
    return propsWithMetadata;
}
