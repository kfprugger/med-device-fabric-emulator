'use client';
import * as React from 'react';
import { useFieldControlProps_unstable } from '@fluentui/react-field';
import { getPartitionedNativeProps, useControllableState, mergeCallbacks, useId, useIsomorphicLayoutEffect, useMergedRefs, slot } from '@fluentui/react-utilities';
import { Checkmark12Filled, Checkmark16Filled, Square12Filled, Square16Filled, CircleFilled } from '@fluentui/react-icons';
import { Label } from '@fluentui/react-label';
import { useFocusWithin } from '@fluentui/react-tabster';
/**
 * Create the state required to render Checkbox.
 *
 * The returned state can be modified with hooks such as useCheckboxStyles_unstable,
 * before being passed to renderCheckbox_unstable.
 *
 * @param props - props from this instance of Checkbox
 * @param ref - reference to `<input>` element of Checkbox
 */ export const useCheckbox_unstable = (props, ref)=>{
    'use no memo';
    // Merge props from surrounding <Field>, if any
    props = useFieldControlProps_unstable(props, {
        supportsLabelFor: true,
        supportsRequired: true
    });
    const { shape = 'square', size = 'medium', ...checkboxProps } = props;
    const state = useCheckboxBase_unstable(checkboxProps, ref);
    // Override indicator children with size+shape-appropriate icon
    const mixed = state.checked === 'mixed';
    let checkmarkIcon;
    if (mixed) {
        if (shape === 'circular') {
            checkmarkIcon = /*#__PURE__*/ React.createElement(CircleFilled, null);
        } else {
            checkmarkIcon = size === 'large' ? /*#__PURE__*/ React.createElement(Square16Filled, null) : /*#__PURE__*/ React.createElement(Square12Filled, null);
        }
    } else if (state.checked) {
        checkmarkIcon = size === 'large' ? /*#__PURE__*/ React.createElement(Checkmark16Filled, null) : /*#__PURE__*/ React.createElement(Checkmark12Filled, null);
    }
    if (state.indicator) {
        var _state_indicator;
        var _children;
        (_children = (_state_indicator = state.indicator).children) !== null && _children !== void 0 ? _children : _state_indicator.children = checkmarkIcon;
    }
    return {
        shape,
        size,
        ...state,
        components: {
            // eslint-disable-next-line @typescript-eslint/no-deprecated
            ...state.components,
            label: Label
        },
        label: slot.optional(props.label, {
            defaultProps: {
                ...state.label,
                size: 'medium'
            },
            elementType: Label
        })
    };
};
/**
 * Base hook for Checkbox component, which manages state related to checked state, ARIA attributes,
 * focus management, and slot structure without design props.
 *
 * @param props - props from this instance of Checkbox
 * @param ref - reference to `<input>` element of Checkbox
 */ export const useCheckboxBase_unstable = (props, ref)=>{
    'use no memo';
    const { disabled = false, required, labelPosition = 'after', onChange } = props;
    const [checked, setChecked] = useControllableState({
        defaultState: props.defaultChecked,
        state: props.checked,
        initialState: false
    });
    const nativeProps = getPartitionedNativeProps({
        props,
        primarySlotTagName: 'input',
        excludedPropNames: [
            'checked',
            'defaultChecked',
            'onChange'
        ]
    });
    const mixed = checked === 'mixed';
    const id = useId('checkbox-', nativeProps.primary.id);
    const state = {
        checked,
        disabled,
        labelPosition,
        components: {
            root: 'span',
            input: 'input',
            indicator: 'div',
            label: 'label'
        },
        root: slot.always(props.root, {
            defaultProps: {
                ref: useFocusWithin(),
                ...nativeProps.root
            },
            elementType: 'span'
        }),
        input: slot.always(props.input, {
            defaultProps: {
                type: 'checkbox',
                id,
                ref,
                checked: checked === true,
                ...nativeProps.primary
            },
            elementType: 'input'
        }),
        label: slot.optional(props.label, {
            defaultProps: {
                htmlFor: id,
                disabled,
                required
            },
            elementType: 'label'
        }),
        indicator: slot.optional(props.indicator, {
            renderByDefault: true,
            defaultProps: {
                'aria-hidden': true
            },
            elementType: 'div'
        })
    };
    state.input.onChange = mergeCallbacks(state.input.onChange, (ev)=>{
        const val = ev.currentTarget.indeterminate ? 'mixed' : ev.currentTarget.checked;
        onChange === null || onChange === void 0 ? void 0 : onChange(ev, {
            checked: val
        });
        setChecked(val);
    });
    // Create a ref object for the input element so we can use it to set the indeterminate prop.
    // Use useMergedRefs, since the ref might be undefined or a function-ref (no .current)
    const inputRef = useMergedRefs(state.input.ref);
    state.input.ref = inputRef;
    // Set the <input> element's checked and indeterminate properties based on our tri-state property.
    // Since indeterminate can only be set via javascript, it has to be done in a layout effect.
    useIsomorphicLayoutEffect(()=>{
        if (inputRef.current) {
            inputRef.current.indeterminate = mixed;
        }
    }, [
        inputRef,
        mixed
    ]);
    return state;
};
