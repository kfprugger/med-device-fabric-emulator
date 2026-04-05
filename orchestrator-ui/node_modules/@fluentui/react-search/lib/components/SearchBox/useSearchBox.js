'use client';
import * as React from 'react';
import { isResolvedShorthand, mergeCallbacks, slot, useControllableState, useEventCallback, useMergedRefs } from '@fluentui/react-utilities';
import { useInput_unstable } from '@fluentui/react-input';
import { DismissRegular, SearchRegular } from '@fluentui/react-icons';
/**
 * Create the state required to render SearchBox.
 *
 * The returned state can be modified with hooks such as useSearchBoxStyles_unstable,
 * before being passed to renderSearchBox_unstable.
 *
 * @param props - props from this instance of SearchBox
 * @param ref - reference to root HTMLElement of SearchBox
 */ export const useSearchBox_unstable = (props, ref)=>{
    const { size = 'medium', appearance = 'outline', ...baseProps } = props;
    const state = useSearchBoxBase_unstable(baseProps, ref);
    if (state.contentBefore) {
        var _state_contentBefore;
        var _children;
        (_children = (_state_contentBefore = state.contentBefore).children) !== null && _children !== void 0 ? _children : _state_contentBefore.children = /*#__PURE__*/ React.createElement(SearchRegular, null);
    }
    if (state.dismiss) {
        var _state_dismiss;
        var _children1;
        (_children1 = (_state_dismiss = state.dismiss).children) !== null && _children1 !== void 0 ? _children1 : _state_dismiss.children = /*#__PURE__*/ React.createElement(DismissRegular, null);
    }
    return {
        ...state,
        size,
        appearance
    };
};
/**
 * Base hook for SearchBox component. Manages state related to controlled/uncontrolled
 * value, focus tracking, dismiss button click handling, search icon slot, and
 * input type="search" — without design props (size, appearance).
 *
 * @param props - props from this instance of SearchBox (without size, appearance)
 * @param ref - reference to root HTMLElement of SearchBox
 */ export const useSearchBoxBase_unstable = (props, ref)=>{
    const { disabled = false, root, contentBefore, dismiss, contentAfter, value, defaultValue, ...inputProps } = props;
    const searchBoxRootRef = React.useRef(null);
    const searchBoxRef = React.useRef(null);
    const [internalValue, setInternalValue] = useControllableState({
        state: value,
        defaultState: defaultValue,
        initialState: ''
    });
    // Tracks the focus of the component for the contentAfter and dismiss button
    const [focused, setFocused] = React.useState(false);
    const onFocus = React.useCallback(()=>{
        setFocused(true);
    }, [
        setFocused
    ]);
    const onBlur = React.useCallback((ev)=>{
        var _searchBoxRootRef_current;
        setFocused(!!((_searchBoxRootRef_current = searchBoxRootRef.current) === null || _searchBoxRootRef_current === void 0 ? void 0 : _searchBoxRootRef_current.contains(ev.relatedTarget)));
    }, [
        setFocused
    ]);
    const rootProps = slot.resolveShorthand(root);
    const handleDismissClick = useEventCallback((event)=>{
        var _props_onChange, _searchBoxRef_current;
        if (isResolvedShorthand(dismiss)) {
            var _dismiss_onClick;
            (_dismiss_onClick = dismiss.onClick) === null || _dismiss_onClick === void 0 ? void 0 : _dismiss_onClick.call(dismiss, event);
        }
        const newValue = '';
        setInternalValue(newValue);
        (_props_onChange = props.onChange) === null || _props_onChange === void 0 ? void 0 : _props_onChange.call(props, event, {
            value: newValue
        });
        (_searchBoxRef_current = searchBoxRef.current) === null || _searchBoxRef_current === void 0 ? void 0 : _searchBoxRef_current.focus();
    });
    const inputState = useInput_unstable({
        type: 'search',
        disabled,
        value: internalValue,
        root: slot.always({
            ...rootProps,
            ref: useMergedRefs(rootProps === null || rootProps === void 0 ? void 0 : rootProps.ref, searchBoxRootRef),
            onFocus: mergeCallbacks(rootProps === null || rootProps === void 0 ? void 0 : rootProps.onFocus, onFocus),
            onBlur: mergeCallbacks(rootProps === null || rootProps === void 0 ? void 0 : rootProps.onBlur, onBlur)
        }, {
            elementType: 'span'
        }),
        contentBefore: slot.optional(contentBefore, {
            renderByDefault: true,
            elementType: 'span'
        }),
        contentAfter: slot.optional(contentAfter, {
            renderByDefault: true,
            elementType: 'span'
        }),
        ...inputProps,
        onChange: useEventCallback((ev)=>{
            var _props_onChange;
            const newValue = ev.target.value;
            (_props_onChange = props.onChange) === null || _props_onChange === void 0 ? void 0 : _props_onChange.call(props, ev, {
                value: newValue
            });
            setInternalValue(newValue);
        })
    }, useMergedRefs(searchBoxRef, ref));
    const state = {
        ...inputState,
        components: {
            // eslint-disable-next-line @typescript-eslint/no-deprecated
            ...inputState.components,
            dismiss: 'span'
        },
        dismiss: slot.optional(dismiss, {
            defaultProps: {
                role: 'button',
                'aria-label': 'clear',
                tabIndex: -1
            },
            renderByDefault: true,
            elementType: 'span'
        }),
        disabled,
        focused
    };
    if (state.dismiss) {
        state.dismiss.onClick = handleDismissClick;
    }
    return state;
};
