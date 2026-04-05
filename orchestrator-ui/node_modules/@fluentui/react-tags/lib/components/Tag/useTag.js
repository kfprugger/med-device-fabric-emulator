'use client';
import * as React from 'react';
import { getIntrinsicElementProps, useEventCallback, useId, slot } from '@fluentui/react-utilities';
import { DismissRegular } from '@fluentui/react-icons';
import { Delete, Backspace } from '@fluentui/keyboard-keys';
import { useTagGroupContext_unstable } from '../../contexts/tagGroupContext';
const tagAvatarSizeMap = {
    medium: 28,
    small: 20,
    'extra-small': 16
};
const tagAvatarShapeMap = {
    rounded: 'square',
    circular: 'circular'
};
/**
 * Create the base state required to render Tag, without design-only props.
 *
 * @param props - props from this instance of Tag (without appearance, size, shape)
 * @param ref - reference to root HTMLSpanElement or HTMLButtonElement of Tag
 */ export const useTagBase_unstable = (props, ref)=>{
    const { handleTagDismiss, disabled: contextDisabled, dismissible: contextDismissible, role: tagGroupRole } = useTagGroupContext_unstable();
    const id = useId('fui-Tag', props.id);
    const { disabled = false, dismissible = contextDismissible !== null && contextDismissible !== void 0 ? contextDismissible : false, selected, value = id } = props;
    const dismissOnClick = useEventCallback((ev)=>{
        var _props_onClick;
        (_props_onClick = props.onClick) === null || _props_onClick === void 0 ? void 0 : _props_onClick.call(props, ev);
        if (!ev.defaultPrevented) {
            handleTagDismiss === null || handleTagDismiss === void 0 ? void 0 : handleTagDismiss(ev, {
                value
            });
        }
    });
    const dismissOnKeyDown = useEventCallback((ev)=>{
        var _props_onKeyDown;
        props === null || props === void 0 ? void 0 : (_props_onKeyDown = props.onKeyDown) === null || _props_onKeyDown === void 0 ? void 0 : _props_onKeyDown.call(props, ev);
        if (!ev.defaultPrevented && (ev.key === Delete || ev.key === Backspace)) {
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
        root: slot.always(getIntrinsicElementProps(elementType, {
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
        media: slot.optional(props.media, {
            elementType: 'span'
        }),
        icon: slot.optional(props.icon, {
            elementType: 'span'
        }),
        primaryText: slot.optional(props.primaryText, {
            renderByDefault: true,
            defaultProps: {
                children: props.children
            },
            elementType: 'span'
        }),
        secondaryText: slot.optional(props.secondaryText, {
            elementType: 'span'
        }),
        dismissIcon: slot.optional(props.dismissIcon, {
            renderByDefault: dismissible,
            elementType: 'span'
        })
    };
};
/**
 * Create the state required to render Tag.
 *
 * The returned state can be modified with hooks such as useTagStyles_unstable,
 * before being passed to renderTag_unstable.
 *
 * @param props - props from this instance of Tag
 * @param ref - reference to root HTMLSpanElement or HTMLButtonElement of Tag
 */ export const useTag_unstable = (props, ref)=>{
    const { size: contextSize, appearance: contextAppearance } = useTagGroupContext_unstable();
    const { appearance = contextAppearance !== null && contextAppearance !== void 0 ? contextAppearance : 'filled', shape = 'rounded', size = contextSize } = props;
    const baseState = useTagBase_unstable(props, ref);
    if (baseState.dismissIcon) {
        var _baseState_dismissIcon;
        var _children;
        (_children = (_baseState_dismissIcon = baseState.dismissIcon).children) !== null && _children !== void 0 ? _children : _baseState_dismissIcon.children = /*#__PURE__*/ React.createElement(DismissRegular, null);
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
