'use client';
import * as React from 'react';
import { useFieldControlProps_unstable } from '@fluentui/react-field';
import { getPartitionedNativeProps, useEventCallback, slot } from '@fluentui/react-utilities';
import { ChevronDownRegular } from '@fluentui/react-icons';
import { useOverrides_unstable as useOverrides } from '@fluentui/react-shared-contexts';
/**
 * Create the state required to render Select.
 *
 * The returned state can be modified with hooks such as useSelectStyles,
 * before being passed to renderSelect.
 *
 * @param props - props from this instance of Select
 * @param ref - reference to the `<select>` element in Select
 */ export const useSelect_unstable = (props, ref)=>{
    // Merge props from surrounding <Field>, if any
    props = useFieldControlProps_unstable(props, {
        supportsLabelFor: true,
        supportsRequired: true,
        supportsSize: true
    });
    const overrides = useOverrides();
    var _overrides_inputDefaultAppearance;
    const { appearance = (_overrides_inputDefaultAppearance = overrides.inputDefaultAppearance) !== null && _overrides_inputDefaultAppearance !== void 0 ? _overrides_inputDefaultAppearance : 'outline', size = 'medium', ...baseProps } = props;
    const state = useSelectBase_unstable(baseProps, ref);
    if (state.icon) {
        var _state_icon;
        var _children;
        (_children = (_state_icon = state.icon).children) !== null && _children !== void 0 ? _children : _state_icon.children = /*#__PURE__*/ React.createElement(ChevronDownRegular, null);
    }
    return {
        ...state,
        appearance,
        size
    };
};
/**
 * Create the base state required to render Select without design-specific props.
 *
 * @param props - props from this instance of Select (without appearance/size)
 * @param ref - reference to the `<select>` element in Select
 */ export const useSelectBase_unstable = (props, ref)=>{
    const { defaultValue, value, select, icon, root, onChange } = props;
    const nativeProps = getPartitionedNativeProps({
        props,
        primarySlotTagName: 'select',
        excludedPropNames: [
            'defaultValue',
            'onChange',
            'value'
        ]
    });
    const state = {
        components: {
            root: 'span',
            select: 'select',
            icon: 'span'
        },
        select: slot.always(select, {
            defaultProps: {
                defaultValue,
                value,
                ref,
                ...nativeProps.primary
            },
            elementType: 'select'
        }),
        icon: slot.optional(icon, {
            renderByDefault: true,
            elementType: 'span'
        }),
        root: slot.always(root, {
            defaultProps: nativeProps.root,
            elementType: 'span'
        })
    };
    state.select.onChange = useEventCallback((event)=>{
        onChange === null || onChange === void 0 ? void 0 : onChange(event, {
            value: event.target.value
        });
    });
    return state;
};
