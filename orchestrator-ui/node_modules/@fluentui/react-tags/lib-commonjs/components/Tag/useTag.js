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
    useTagBase_unstable: function() {
        return useTagBase_unstable;
    },
    useTag_unstable: function() {
        return useTag_unstable;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactutilities = require("@fluentui/react-utilities");
const _reacticons = require("@fluentui/react-icons");
const _keyboardkeys = require("@fluentui/keyboard-keys");
const _tagGroupContext = require("../../contexts/tagGroupContext");
const tagAvatarSizeMap = {
    medium: 28,
    small: 20,
    'extra-small': 16
};
const tagAvatarShapeMap = {
    rounded: 'square',
    circular: 'circular'
};
const useTagBase_unstable = (props, ref)=>{
    const { handleTagDismiss, disabled: contextDisabled, dismissible: contextDismissible, role: tagGroupRole } = (0, _tagGroupContext.useTagGroupContext_unstable)();
    const id = (0, _reactutilities.useId)('fui-Tag', props.id);
    const { disabled = false, dismissible = contextDismissible !== null && contextDismissible !== void 0 ? contextDismissible : false, selected, value = id } = props;
    const dismissOnClick = (0, _reactutilities.useEventCallback)((ev)=>{
        var _props_onClick;
        (_props_onClick = props.onClick) === null || _props_onClick === void 0 ? void 0 : _props_onClick.call(props, ev);
        if (!ev.defaultPrevented) {
            handleTagDismiss === null || handleTagDismiss === void 0 ? void 0 : handleTagDismiss(ev, {
                value
            });
        }
    });
    const dismissOnKeyDown = (0, _reactutilities.useEventCallback)((ev)=>{
        var _props_onKeyDown;
        props === null || props === void 0 ? void 0 : (_props_onKeyDown = props.onKeyDown) === null || _props_onKeyDown === void 0 ? void 0 : _props_onKeyDown.call(props, ev);
        if (!ev.defaultPrevented && (ev.key === _keyboardkeys.Delete || ev.key === _keyboardkeys.Backspace)) {
            handleTagDismiss === null || handleTagDismiss === void 0 ? void 0 : handleTagDismiss(ev, {
                value
            });
        }
    });
    const elementType = dismissible ? 'button' : 'span';
    const selectedProp = tagGroupRole === 'listbox' ? 'aria-selected' : 'aria-pressed';
    const selectable = typeof selected === 'boolean' || tagGroupRole === 'listbox';
    return {
        disabled: contextDisabled ? true : disabled,
        dismissible,
        selected: !!selected,
        components: {
            root: elementType,
            media: 'span',
            icon: 'span',
            primaryText: 'span',
            secondaryText: 'span',
            dismissIcon: 'span'
        },
        root: _reactutilities.slot.always((0, _reactutilities.getIntrinsicElementProps)(elementType, {
            ref,
            role: tagGroupRole === 'listbox' ? 'option' : undefined,
            [selectedProp]: selectable ? selected : undefined,
            ...props,
            disabled: contextDisabled ? true : disabled,
            id,
            ...dismissible && {
                onClick: dismissOnClick,
                onKeyDown: dismissOnKeyDown
            }
        }), {
            defaultProps: {
                type: elementType === 'button' ? 'button' : undefined
            },
            elementType
        }),
        media: _reactutilities.slot.optional(props.media, {
            elementType: 'span'
        }),
        icon: _reactutilities.slot.optional(props.icon, {
            elementType: 'span'
        }),
        primaryText: _reactutilities.slot.optional(props.primaryText, {
            renderByDefault: true,
            defaultProps: {
                children: props.children
            },
            elementType: 'span'
        }),
        secondaryText: _reactutilities.slot.optional(props.secondaryText, {
            elementType: 'span'
        }),
        dismissIcon: _reactutilities.slot.optional(props.dismissIcon, {
            renderByDefault: dismissible,
            elementType: 'span'
        })
    };
};
const useTag_unstable = (props, ref)=>{
    const { size: contextSize, appearance: contextAppearance } = (0, _tagGroupContext.useTagGroupContext_unstable)();
    const { appearance = contextAppearance !== null && contextAppearance !== void 0 ? contextAppearance : 'filled', shape = 'rounded', size = contextSize } = props;
    const baseState = useTagBase_unstable(props, ref);
    if (baseState.dismissIcon) {
        var _baseState_dismissIcon;
        var _children;
        (_children = (_baseState_dismissIcon = baseState.dismissIcon).children) !== null && _children !== void 0 ? _children : _baseState_dismissIcon.children = /*#__PURE__*/ _react.createElement(_reacticons.DismissRegular, null);
    }
    return {
        ...baseState,
        appearance,
        avatarShape: tagAvatarShapeMap[shape],
        avatarSize: tagAvatarSizeMap[size],
        shape,
        size
    };
};
