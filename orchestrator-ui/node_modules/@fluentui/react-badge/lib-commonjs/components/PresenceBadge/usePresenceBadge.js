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
    DEFAULT_STRINGS: function() {
        return DEFAULT_STRINGS;
    },
    usePresenceBadgeBase_unstable: function() {
        return usePresenceBadgeBase_unstable;
    },
    usePresenceBadge_unstable: function() {
        return usePresenceBadge_unstable;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactutilities = require("@fluentui/react-utilities");
const _presenceIcons = require("./presenceIcons");
const _index = require("../Badge/index");
const iconMap = (status, outOfOffice, size)=>{
    switch(status){
        case 'available':
            return outOfOffice ? _presenceIcons.presenceAvailableRegular[size] : _presenceIcons.presenceAvailableFilled[size];
        case 'away':
            return outOfOffice ? _presenceIcons.presenceOofRegular[size] : _presenceIcons.presenceAwayFilled[size];
        case 'blocked':
            return _presenceIcons.presenceBlockedRegular[size];
        case 'busy':
            return outOfOffice ? _presenceIcons.presenceUnknownRegular[size] : _presenceIcons.presenceBusyFilled[size];
        case 'do-not-disturb':
            return outOfOffice ? _presenceIcons.presenceDndRegular[size] : _presenceIcons.presenceDndFilled[size];
        case 'offline':
            return outOfOffice ? _presenceIcons.presenceOofRegular[size] : _presenceIcons.presenceOfflineRegular[size];
        case 'out-of-office':
            return _presenceIcons.presenceOofRegular[size];
        case 'unknown':
            return _presenceIcons.presenceUnknownRegular[size];
    }
};
const DEFAULT_STRINGS = {
    busy: 'busy',
    'out-of-office': 'out of office',
    away: 'away',
    available: 'available',
    offline: 'offline',
    'do-not-disturb': 'do not disturb',
    unknown: 'unknown',
    blocked: 'blocked'
};
const usePresenceBadge_unstable = (props, ref)=>{
    const { size = 'medium', outOfOffice = false, ...baseProps } = props;
    var _props_status;
    const status = (_props_status = props.status) !== null && _props_status !== void 0 ? _props_status : 'available';
    const IconElement = iconMap(status, outOfOffice, size);
    const state = {
        ...usePresenceBadgeBase_unstable(baseProps, ref),
        appearance: 'filled',
        color: 'brand',
        shape: 'circular',
        size,
        outOfOffice
    };
    if (state.icon) {
        var _state_icon;
        var _children;
        (_children = (_state_icon = state.icon).children) !== null && _children !== void 0 ? _children : _state_icon.children = /*#__PURE__*/ _react.createElement(IconElement, null);
    }
    return state;
};
const usePresenceBadgeBase_unstable = (props, ref)=>{
    const { status = 'available', outOfOffice = false } = props;
    const statusText = DEFAULT_STRINGS[status];
    const oofText = props.outOfOffice && props.status !== 'out-of-office' ? ` ${DEFAULT_STRINGS['out-of-office']}` : '';
    const state = {
        ...(0, _index.useBadgeBase_unstable)({
            'aria-label': statusText + oofText,
            role: 'img',
            ...props,
            icon: _reactutilities.slot.optional(props.icon, {
                renderByDefault: true,
                elementType: 'span'
            })
        }, ref),
        status,
        outOfOffice
    };
    return state;
};
