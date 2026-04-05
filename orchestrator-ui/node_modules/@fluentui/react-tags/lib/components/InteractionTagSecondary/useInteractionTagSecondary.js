'use client';
import * as React from 'react';
import { getIntrinsicElementProps, useEventCallback, slot, useId } from '@fluentui/react-utilities';
import { Delete, Backspace } from '@fluentui/keyboard-keys';
import { DismissRegular } from '@fluentui/react-icons';
import { useInteractionTagContext_unstable } from '../../contexts/interactionTagContext';
/**
 * Create the base state required to render InteractionTagSecondary, without design-only props.
 *
 * @param props - props from this instance of InteractionTagSecondary
 * @param ref - reference to root HTMLButtonElement of InteractionTagSecondary
 */ export const useInteractionTagSecondaryBase_unstable = (props, ref)=>{
    const { disabled, handleTagDismiss, interactionTagPrimaryId, selected, value } = useInteractionTagContext_unstable();
    const id = useId('fui-InteractionTagSecondary-', props.id);
    const onClick = useEventCallback((ev)=>{
        var _props_onClick;
        props === null || props === void 0 ? void 0 : (_props_onClick = props.onClick) === null || _props_onClick === void 0 ? void 0 : _props_onClick.call(props, ev);
        if (!ev.defaultPrevented) {
            handleTagDismiss === null || handleTagDismiss === void 0 ? void 0 : handleTagDismiss(ev, {
                value
            });
        }
    });
    const onKeyDown = useEventCallback((ev)=>{
        var _props_onKeyDown;
        props === null || props === void 0 ? void 0 : (_props_onKeyDown = props.onKeyDown) === null || _props_onKeyDown === void 0 ? void 0 : _props_onKeyDown.call(props, ev);
        if (!ev.defaultPrevented && (ev.key === Delete || ev.key === Backspace)) {
            handleTagDismiss === null || handleTagDismiss === void 0 ? void 0 : handleTagDismiss(ev, {
                value
            });
        }
    });
    return {
        disabled,
        selected,
        components: {
            root: 'button'
        },
        root: slot.always(getIntrinsicElementProps('button', {
            type: 'button',
            disabled,
            ref,
            'aria-labelledby': `${interactionTagPrimaryId} ${id}`,
            ...props,
            id,
            onClick,
            onKeyDown
        }), {
            elementType: 'button'
        })
    };
};
/**
 * Create the state required to render InteractionTagSecondary.
 *
 * The returned state can be modified with hooks such as useInteractionTagSecondaryStyles_unstable,
 * before being passed to renderInteractionTagSecondary_unstable.
 *
 * @param props - props from this instance of InteractionTagSecondary
 * @param ref - reference to root HTMLButtonElement of InteractionTagSecondary
 */ export const useInteractionTagSecondary_unstable = (props, ref)=>{
    var _baseState_root;
    const { appearance, shape, size } = useInteractionTagContext_unstable();
    const baseState = useInteractionTagSecondaryBase_unstable(props, ref);
    var _children;
    (_children = (_baseState_root = baseState.root).children) !== null && _children !== void 0 ? _children : _baseState_root.children = /*#__PURE__*/ React.createElement(DismissRegular, null);
    return {
        ...baseState,
        appearance,
        shape,
        size
    };
};
