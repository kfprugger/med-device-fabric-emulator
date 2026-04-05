"use strict";
Object.defineProperty(exports, "__esModule", {
    value: true
});
Object.defineProperty(exports, "renderMenu_unstable", {
    enumerable: true,
    get: function() {
        return renderMenu_unstable;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _jsxruntime = require("@fluentui/react-jsx-runtime/jsx-runtime");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactmotion = require("@fluentui/react-motion");
const _reactutilities = require("@fluentui/react-utilities");
const _menuContext = require("../../contexts/menuContext");
const renderMenu_unstable = (state, contextValues)=>{
    (0, _reactutilities.assertSlots)(state);
    return /*#__PURE__*/ (0, _jsxruntime.jsxs)(_menuContext.MenuProvider, {
        value: contextValues.menu,
        children: [
            state.menuTrigger,
            state.menuPopover && /*#__PURE__*/ (0, _jsxruntime.jsx)(state.surfaceMotion, {
                children: /*#__PURE__*/ (0, _jsxruntime.jsx)(_reactmotion.MotionRefForwarder, {
                    children: state.menuPopover
                })
            })
        ]
    });
};
