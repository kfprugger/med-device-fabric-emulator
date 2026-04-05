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
    useBreadcrumbDividerBase_unstable: function() {
        return useBreadcrumbDividerBase_unstable;
    },
    useBreadcrumbDivider_unstable: function() {
        return useBreadcrumbDivider_unstable;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactutilities = require("@fluentui/react-utilities");
const _reacticons = require("@fluentui/react-icons");
const _BreadcrumbContext = require("../Breadcrumb/BreadcrumbContext");
const _reactsharedcontexts = require("@fluentui/react-shared-contexts");
const useBreadcrumbDivider_unstable = (props, ref)=>{
    const { size } = (0, _BreadcrumbContext.useBreadcrumbContext_unstable)();
    const state = useBreadcrumbDividerBase_unstable(props, ref);
    const { dir } = (0, _reactsharedcontexts.useFluent_unstable)();
    return {
        ...state,
        root: {
            ...state.root,
            children: getDividerIcon(dir)
        },
        size
    };
};
const useBreadcrumbDividerBase_unstable = (props, ref)=>{
    return {
        components: {
            root: 'li'
        },
        root: _reactutilities.slot.always((0, _reactutilities.getIntrinsicElementProps)('li', {
            ref,
            'aria-hidden': true,
            ...props
        }), {
            elementType: 'li'
        })
    };
};
/**
 * Get icon of the divider
 *
 * @param dir - RTL or LTR
 */ function getDividerIcon(dir) {
    return dir === 'rtl' ? /*#__PURE__*/ _react.createElement(_reacticons.ChevronLeftRegular, null) : /*#__PURE__*/ _react.createElement(_reacticons.ChevronRightRegular, null);
}
