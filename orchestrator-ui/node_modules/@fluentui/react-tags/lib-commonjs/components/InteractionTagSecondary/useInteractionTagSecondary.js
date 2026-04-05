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
    useInteractionTagSecondaryBase_unstable: function() {
        return useInteractionTagSecondaryBase_unstable;
    },
    useInteractionTagSecondary_unstable: function() {
        return useInteractionTagSecondary_unstable;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactutilities = require("@fluentui/react-utilities");
const _keyboardkeys = require("@fluentui/keyboard-keys");
const _reacticons = require("@fluentui/react-icons");
const _interactionTagContext = require("../../contexts/interactionTagContext");
const useInteractionTagSecondaryBase_unstable = (props, ref)=>{
    const { disabled, handleTagDismiss, interactionTagPrimaryId, selected, value } = (0, _interactionTagContext.useInteractionTagContext_unstable)();
    const id = (0, _reactutilities.useId)('fui-InteractionTagSecondary-', props.id);
    const onClick = (0, _reactutilities.useEventCallback)((ev)=>{
        var _props_onClick;
        props === null || props === void 0 ? void 0 : (_props_onClick = props.onClick) === null || _props_onClick === void 0 ? void 0 : _props_onClick.call(props, ev);
        if (!ev.defaultPrevented) {
            handleTagDismiss === null || handleTagDismiss === void 0 ? void 0 : handleTagDismiss(ev, {
                value
            });
        }
    });
    const onKeyDown = (0, _reactutilities.useEventCallback)((ev)=>{
        var _props_onKeyDown;
        props === null || props === void 0 ? void 0 : (_props_onKeyDown = props.onKeyDown) === null || _props_onKeyDown === void 0 ? void 0 : _props_onKeyDown.call(props, ev);
        if (!ev.defaultPrevented && (ev.key === _keyboardkeys.Delete || ev.key === _keyboardkeys.Backspace)) {
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
        root: _reactutilities.slot.always((0, _reactutilities.getIntrinsicElementProps)('button', {
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
const useInteractionTagSecondary_unstable = (props, ref)=>{
    var _baseState_root;
    const { appearance, shape, size } = (0, _interactionTagContext.useInteractionTagContext_unstable)();
    const baseState = useInteractionTagSecondaryBase_unstable(props, ref);
    var _children;
    (_children = (_baseState_root = baseState.root).children) !== null && _children !== void 0 ? _children : _baseState_root.children = /*#__PURE__*/ _react.createElement(_reacticons.DismissRegular, null);
    return {
        ...baseState,
        appearance,
        shape,
        size
    };
};
