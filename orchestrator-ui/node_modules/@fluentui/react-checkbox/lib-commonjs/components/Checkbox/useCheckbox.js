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
    useCheckboxBase_unstable: function() {
        return useCheckboxBase_unstable;
    },
    useCheckbox_unstable: function() {
        return useCheckbox_unstable;
    }
});
const _interop_require_wildcard = require("@swc/helpers/_/_interop_require_wildcard");
const _react = /*#__PURE__*/ _interop_require_wildcard._(require("react"));
const _reactfield = require("@fluentui/react-field");
const _reactutilities = require("@fluentui/react-utilities");
const _reacticons = require("@fluentui/react-icons");
const _reactlabel = require("@fluentui/react-label");
const _reacttabster = require("@fluentui/react-tabster");
const useCheckbox_unstable = (props, ref)=>{
    'use no memo';
    // Merge props from surrounding <Field>, if any
    props = (0, _reactfield.useFieldControlProps_unstable)(props, {
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
            checkmarkIcon = /*#__PURE__*/ _react.createElement(_reacticons.CircleFilled, null);
        } else {
            checkmarkIcon = size === 'large' ? /*#__PURE__*/ _react.createElement(_reacticons.Square16Filled, null) : /*#__PURE__*/ _react.createElement(_reacticons.Square12Filled, null);
        }
    } else if (state.checked) {
        checkmarkIcon = size === 'large' ? /*#__PURE__*/ _react.createElement(_reacticons.Checkmark16Filled, null) : /*#__PURE__*/ _react.createElement(_reacticons.Checkmark12Filled, null);
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
            label: _reactlabel.Label
        },
        label: _reactutilities.slot.optional(props.label, {
            defaultProps: {
                ...state.label,
                size: 'medium'
            },
            elementType: _reactlabel.Label
        })
    };
};
const useCheckboxBase_unstable = (props, ref)=>{
    'use no memo';
    const { disabled = false, required, labelPosition = 'after', onChange } = props;
    const [checked, setChecked] = (0, _reactutilities.useControllableState)({
        defaultState: props.defaultChecked,
        state: props.checked,
        initialState: false
    });
    const nativeProps = (0, _reactutilities.getPartitionedNativeProps)({
        props,
        primarySlotTagName: 'input',
        excludedPropNames: [
            'checked',
            'defaultChecked',
            'onChange'
        ]
    });
    const mixed = checked === 'mixed';
    const id = (0, _reactutilities.useId)('checkbox-', nativeProps.primary.id);
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
        root: _reactutilities.slot.always(props.root, {
            defaultProps: {
                ref: (0, _reacttabster.useFocusWithin)(),
                ...nativeProps.root
            },
            elementType: 'span'
        }),
        input: _reactutilities.slot.always(props.input, {
            defaultProps: {
                type: 'checkbox',
                id,
                ref,
                checked: checked === true,
                ...nativeProps.primary
            },
            elementType: 'input'
        }),
        label: _reactutilities.slot.optional(props.label, {
            defaultProps: {
                htmlFor: id,
                disabled,
                required
            },
            elementType: 'label'
        }),
        indicator: _reactutilities.slot.optional(props.indicator, {
            renderByDefault: true,
            defaultProps: {
                'aria-hidden': true
            },
            elementType: 'div'
        })
    };
    state.input.onChange = (0, _reactutilities.mergeCallbacks)(state.input.onChange, (ev)=>{
        const val = ev.currentTarget.indeterminate ? 'mixed' : ev.currentTarget.checked;
        onChange === null || onChange === void 0 ? void 0 : onChange(ev, {
            checked: val
        });
        setChecked(val);
    });
    // Create a ref object for the input element so we can use it to set the indeterminate prop.
    // Use useMergedRefs, since the ref might be undefined or a function-ref (no .current)
    const inputRef = (0, _reactutilities.useMergedRefs)(state.input.ref);
    state.input.ref = inputRef;
    // Set the <input> element's checked and indeterminate properties based on our tri-state property.
    // Since indeterminate can only be set via javascript, it has to be done in a layout effect.
    (0, _reactutilities.useIsomorphicLayoutEffect)(()=>{
        if (inputRef.current) {
            inputRef.current.indeterminate = mixed;
        }
    }, [
        inputRef,
        mixed
    ]);
    return state;
};
