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
    toggleButtonClassNames: function() {
        return toggleButtonClassNames;
    },
    useToggleButtonStyles_unstable: function() {
        return useToggleButtonStyles_unstable;
    }
});
const _reacticons = require("@fluentui/react-icons");
const _reacttabster = require("@fluentui/react-tabster");
const _reacttheme = require("@fluentui/react-theme");
const _react = require("@griffel/react");
const _useButtonStylesstyles = require("../Button/useButtonStyles.styles");
const toggleButtonClassNames = {
    root: 'fui-ToggleButton',
    icon: 'fui-ToggleButton__icon'
};
const useRootCheckedStyles = (0, _react.makeStyles)({
    // Base styles
    base: {
        backgroundColor: _reacttheme.tokens.colorNeutralBackground1Selected,
        ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStroke1),
        color: _reacttheme.tokens.colorNeutralForeground1Selected,
        ..._react.shorthands.borderWidth(_reacttheme.tokens.strokeWidthThin),
        [`& .${_reacticons.iconFilledClassName}`]: {
            display: 'inline'
        },
        [`& .${_reacticons.iconRegularClassName}`]: {
            display: 'none'
        },
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorNeutralBackground1Hover,
            ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStroke1Hover),
            color: _reacttheme.tokens.colorNeutralForeground1Hover
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorNeutralBackground1Pressed,
            ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStroke1Pressed),
            color: _reacttheme.tokens.colorNeutralForeground1Pressed
        }
    },
    // High contrast styles
    highContrast: {
        '@media (forced-colors: active)': {
            backgroundColor: 'Highlight',
            ..._react.shorthands.borderColor('Highlight'),
            color: 'HighlightText',
            forcedColorAdjust: 'none',
            ':hover': {
                backgroundColor: 'HighlightText',
                ..._react.shorthands.borderColor('Highlight'),
                color: 'Highlight'
            },
            ':hover:active,:active:focus-visible': {
                backgroundColor: 'HighlightText',
                ..._react.shorthands.borderColor('Highlight'),
                color: 'Highlight'
            },
            ':focus': {
                border: '1px solid HighlightText',
                outlineColor: 'Highlight'
            }
        }
    },
    // Appearance variations
    outline: {
        backgroundColor: _reacttheme.tokens.colorTransparentBackgroundSelected,
        ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStroke1),
        ..._react.shorthands.borderWidth(_reacttheme.tokens.strokeWidthThicker),
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundHover
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundPressed
        },
        ...(0, _reacttabster.createCustomFocusIndicatorStyle)({
            ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStroke1)
        })
    },
    primary: {
        backgroundColor: _reacttheme.tokens.colorBrandBackgroundSelected,
        ..._react.shorthands.borderColor('transparent'),
        color: _reacttheme.tokens.colorNeutralForegroundOnBrand,
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorBrandBackgroundHover,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForegroundOnBrand
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorBrandBackgroundPressed,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForegroundOnBrand
        }
    },
    secondary: {},
    subtle: {
        backgroundColor: _reacttheme.tokens.colorSubtleBackgroundSelected,
        ..._react.shorthands.borderColor('transparent'),
        color: _reacttheme.tokens.colorNeutralForeground2Selected,
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorSubtleBackgroundHover,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForeground2Hover
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorSubtleBackgroundPressed,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForeground2Pressed
        }
    },
    transparent: {
        backgroundColor: _reacttheme.tokens.colorTransparentBackgroundSelected,
        ..._react.shorthands.borderColor('transparent'),
        color: _reacttheme.tokens.colorNeutralForeground2BrandSelected,
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundHover,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForeground2BrandHover
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundPressed,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForeground2BrandPressed
        }
    }
});
const useCheckedAccessibleStyles = (0, _react.makeStyles)({
    // Base styles
    base: {
        backgroundColor: _reacttheme.tokens.colorBrandBackground,
        ..._react.shorthands.borderColor('transparent'),
        color: _reacttheme.tokens.colorNeutralForegroundOnBrand,
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorBrandBackgroundHover,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForegroundOnBrand
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorBrandBackgroundPressed,
            ..._react.shorthands.borderColor('transparent'),
            color: _reacttheme.tokens.colorNeutralForegroundOnBrand
        }
    },
    // Appearance variations
    outline: {
        // There's no longer a reason to thicken the outline variant's border
        ..._react.shorthands.borderWidth(_reacttheme.tokens.strokeWidthThin)
    },
    primary: {
        // primary has an inner stroke for the checked style
        outline: `${_reacttheme.tokens.strokeWidthThin} solid ${_reacttheme.tokens.colorNeutralForegroundOnBrand}`,
        outlineOffset: `calc(${_reacttheme.tokens.strokeWidthThicker} * -1)`,
        // need to not have the default focus style that removes the outline
        ...(0, _reacttabster.createCustomFocusIndicatorStyle)({
            outline: `${_reacttheme.tokens.strokeWidthThin} solid ${_reacttheme.tokens.colorNeutralForegroundOnBrand}`,
            outlineOffset: `calc(${_reacttheme.tokens.strokeWidthThickest} * -1)`
        })
    },
    subtle: {
        // override subtle-appearance-specific icon color on hover
        ':hover': {
            [`& .${toggleButtonClassNames.icon}`]: {
                color: _reacttheme.tokens.colorNeutralForegroundOnBrand
            }
        }
    },
    transparent: {},
    secondary: {}
});
const useRootDisabledStyles = (0, _react.makeStyles)({
    // Base styles
    base: {
        backgroundColor: _reacttheme.tokens.colorNeutralBackgroundDisabled,
        ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStrokeDisabled),
        color: _reacttheme.tokens.colorNeutralForegroundDisabled,
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorNeutralBackgroundDisabled,
            ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStrokeDisabled),
            color: _reacttheme.tokens.colorNeutralForegroundDisabled
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorNeutralBackgroundDisabled,
            ..._react.shorthands.borderColor(_reacttheme.tokens.colorNeutralStrokeDisabled),
            color: _reacttheme.tokens.colorNeutralForegroundDisabled
        }
    },
    // Appearance variations
    outline: {},
    primary: {
        ..._react.shorthands.borderColor('transparent'),
        ':hover': {
            ..._react.shorthands.borderColor('transparent')
        },
        ':hover:active,:active:focus-visible': {
            ..._react.shorthands.borderColor('transparent')
        }
    },
    secondary: {},
    subtle: {
        backgroundColor: _reacttheme.tokens.colorTransparentBackground,
        ..._react.shorthands.borderColor('transparent'),
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundHover,
            ..._react.shorthands.borderColor('transparent')
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundPressed,
            ..._react.shorthands.borderColor('transparent')
        }
    },
    transparent: {
        backgroundColor: _reacttheme.tokens.colorTransparentBackground,
        ..._react.shorthands.borderColor('transparent'),
        ':hover': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundHover,
            ..._react.shorthands.borderColor('transparent')
        },
        ':hover:active,:active:focus-visible': {
            backgroundColor: _reacttheme.tokens.colorTransparentBackgroundPressed,
            ..._react.shorthands.borderColor('transparent')
        }
    }
});
const useIconCheckedStyles = (0, _react.makeStyles)({
    // Appearance variations with isAccessible=false
    subtleOrTransparent: {
        color: _reacttheme.tokens.colorNeutralForeground2BrandSelected
    },
    // High contrast styles
    highContrast: {
        '@media (forced-colors: active)': {
            forcedColorAdjust: 'auto'
        }
    }
});
const usePrimaryHighContrastStyles = (0, _react.makeStyles)({
    // Do not use primary variant high contrast styles for toggle buttons
    // otherwise there isn't enough difference between on/off states
    base: {
        '@media (forced-colors: active)': {
            backgroundColor: 'ButtonFace',
            ..._react.shorthands.borderColor('ButtonBorder'),
            color: 'ButtonText',
            forcedColorAdjust: 'auto'
        }
    },
    disabled: {
        '@media (forced-colors: active)': {
            ..._react.shorthands.borderColor('GrayText'),
            color: 'GrayText',
            ':focus': {
                ..._react.shorthands.borderColor('GrayText')
            }
        }
    }
});
const useToggleButtonStyles_unstable = (state)=>{
    'use no memo';
    const rootCheckedStyles = useRootCheckedStyles();
    const accessibleCheckedStyles = useCheckedAccessibleStyles();
    const rootDisabledStyles = useRootDisabledStyles();
    const iconCheckedStyles = useIconCheckedStyles();
    const primaryHighContrastStyles = usePrimaryHighContrastStyles();
    const { appearance, checked, disabled, disabledFocusable, isAccessible } = state;
    state.root.className = (0, _react.mergeClasses)(toggleButtonClassNames.root, appearance === 'primary' && primaryHighContrastStyles.base, appearance === 'primary' && (disabled || disabledFocusable) && primaryHighContrastStyles.disabled, checked && rootCheckedStyles.base, checked && rootCheckedStyles.highContrast, appearance && checked && rootCheckedStyles[appearance], isAccessible && checked && accessibleCheckedStyles.base, isAccessible && appearance && checked && accessibleCheckedStyles[appearance], (disabled || disabledFocusable) && rootDisabledStyles.base, appearance && (disabled || disabledFocusable) && rootDisabledStyles[appearance], state.root.className);
    if (state.icon) {
        state.icon.className = (0, _react.mergeClasses)(toggleButtonClassNames.icon, checked && !isAccessible && (appearance === 'subtle' || appearance === 'transparent') && iconCheckedStyles.subtleOrTransparent, iconCheckedStyles.highContrast, state.icon.className);
    }
    (0, _useButtonStylesstyles.useButtonStyles_unstable)(state);
    return state;
};
