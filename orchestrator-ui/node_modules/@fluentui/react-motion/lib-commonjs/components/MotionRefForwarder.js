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
    MotionRefForwarder: function() {
        return MotionRefForwarder;
    },
    MotionRefForwarderReset: function() {
        return MotionRefForwarderReset;
    },
    useMotionForwardedRef: function() {
        return useMotionForwardedRef;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const MotionRefForwarderContext = /*#__PURE__*/ _react.createContext(undefined);
function useMotionForwardedRef() {
    return _react.useContext(MotionRefForwarderContext);
}
const MotionRefForwarder = /*#__PURE__*/ _react.forwardRef((props, ref)=>{
    return /*#__PURE__*/ _react.createElement(MotionRefForwarderContext.Provider, {
        value: ref
    }, props.children);
});
MotionRefForwarder.displayName = 'MotionRefForwarder';
const MotionRefForwarderReset = (props)=>{
    return /*#__PURE__*/ _react.createElement(MotionRefForwarderContext.Provider, {
        value: undefined
    }, props.children);
};
MotionRefForwarderReset.displayName = 'MotionRefForwarderReset';
