'use client';
import * as React from 'react';
import { slot } from '@fluentui/react-utilities';
import { presenceAvailableFilled, presenceAvailableRegular, presenceAwayFilled, presenceBlockedRegular, presenceBusyFilled, presenceDndFilled, presenceDndRegular, presenceOfflineRegular, presenceOofRegular, presenceUnknownRegular } from './presenceIcons';
import { useBadgeBase_unstable } from '../Badge/index';
const iconMap = (status, outOfOffice, size)=>{
    switch(status){
        case 'available':
            return outOfOffice ? presenceAvailableRegular[size] : presenceAvailableFilled[size];
        case 'away':
            return outOfOffice ? presenceOofRegular[size] : presenceAwayFilled[size];
        case 'blocked':
            return presenceBlockedRegular[size];
        case 'busy':
            return outOfOffice ? presenceUnknownRegular[size] : presenceBusyFilled[size];
        case 'do-not-disturb':
            return outOfOffice ? presenceDndRegular[size] : presenceDndFilled[size];
        case 'offline':
            return outOfOffice ? presenceOofRegular[size] : presenceOfflineRegular[size];
        case 'out-of-office':
            return presenceOofRegular[size];
        case 'unknown':
            return presenceUnknownRegular[size];
    }
};
export const DEFAULT_STRINGS = {
    busy: 'busy',
    'out-of-office': 'out of office',
    away: 'away',
    available: 'available',
    offline: 'offline',
    'do-not-disturb': 'do not disturb',
    unknown: 'unknown',
    blocked: 'blocked'
};
/**
 * Returns the props and state required to render the component
 */ export const usePresenceBadge_unstable = (props, ref)=>{
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
        (_children = (_state_icon = state.icon).children) !== null && _children !== void 0 ? _children : _state_icon.children = /*#__PURE__*/ React.createElement(IconElement, null);
    }
    return state;
};
/**
 * Base hook for PresenceBadge component, which manages state related to presence status and ARIA attributes.
 * Note: size is excluded from BaseProps as it is a design prop; icon selection uses the 'medium' size default.
 * To render size-specific icons, use the full usePresenceBadge_unstable hook.
 *
 * @param props - User provided props to the PresenceBadge component.
 * @param ref - User provided ref to be passed to the PresenceBadge component.
 */ export const usePresenceBadgeBase_unstable = (props, ref)=>{
    const { status = 'available', outOfOffice = false } = props;
    const statusText = DEFAULT_STRINGS[status];
    const oofText = props.outOfOffice && props.status !== 'out-of-office' ? ` ${DEFAULT_STRINGS['out-of-office']}` : '';
    const state = {
        ...useBadgeBase_unstable({
            'aria-label': statusText + oofText,
            role: 'img',
            ...props,
            icon: slot.optional(props.icon, {
                renderByDefault: true,
                elementType: 'span'
            })
        }, ref),
        status,
        outOfOffice
    };
    return state;
};
