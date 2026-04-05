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
    progressBarClassNames: function() {
        return progressBarClassNames;
    },
    useProgressBarStyles_unstable: function() {
        return useProgressBarStyles_unstable;
    }
});
const _react = require("@griffel/react");
const _reacttheme = require("@fluentui/react-theme");
const progressBarClassNames = {
    root: 'fui-ProgressBar',
    bar: 'fui-ProgressBar__bar'
};
// If the percentComplete is near 0, don't animate it.
// This prevents animations on reset to 0 scenarios.
const ZERO_THRESHOLD = 0.01;
const barThicknessValues = {
    medium: '2px',
    large: '4px'
};
/**
 * Styles for the root slot
 */ const useRootStyles = (0, _react.makeStyles)({
    root: {
        display: 'block',
        backgroundColor: _reacttheme.tokens.colorNeutralBackground6,
        width: '100%',
        overflow: 'hidden',
        '@media screen and (forced-colors: active)': {
            backgroundColor: 'CanvasText'
        }
    },
    rounded: {
        borderRadius: _reacttheme.tokens.borderRadiusMedium
    },
    square: {
        borderRadius: _reacttheme.tokens.borderRadiusNone
    },
    medium: {
        height: barThicknessValues.medium
    },
    large: {
        height: barThicknessValues.large
    }
});
/**
 * Styles for the ProgressBar bar
 */ const useBarStyles = (0, _react.makeStyles)({
    base: {
        '@media screen and (forced-colors: active)': {
            backgroundColor: 'Highlight'
        },
        borderRadius: 'inherit',
        height: '100%'
    },
    nonZeroDeterminate: {
        transitionProperty: 'width',
        transitionDuration: '0.3s',
        transitionTimingFunction: 'ease'
    },
    indeterminate: {
        maxWidth: '33%',
        position: 'relative',
        backgroundImage: `linear-gradient(
      to right,
      ${_reacttheme.tokens.colorNeutralBackground6} 0%,
      ${_reacttheme.tokens.colorTransparentBackground} 50%,
      ${_reacttheme.tokens.colorNeutralBackground6} 100%
    )`,
        '@media screen and (prefers-reduced-motion: reduce)': {
            // Reduced motion: bar is sized here, and the opacity is pulsed by ProgressBarIndeterminateMotion
            maxWidth: '100%'
        }
    },
    brand: {
        backgroundColor: _reacttheme.tokens.colorCompoundBrandBackground
    },
    error: {
        backgroundColor: _reacttheme.tokens.colorPaletteRedBackground3
    },
    warning: {
        backgroundColor: _reacttheme.tokens.colorPaletteDarkOrangeBackground3
    },
    success: {
        backgroundColor: _reacttheme.tokens.colorPaletteGreenBackground3
    }
});
const useProgressBarStyles_unstable = (state)=>{
    'use no memo';
    const { color, max, shape, thickness, value } = state;
    const rootStyles = useRootStyles();
    const barStyles = useBarStyles();
    state.root.className = (0, _react.mergeClasses)(progressBarClassNames.root, rootStyles.root, rootStyles[shape], rootStyles[thickness], state.root.className);
    if (state.bar) {
        state.bar.className = (0, _react.mergeClasses)(progressBarClassNames.bar, barStyles.base, barStyles.brand, value === undefined && barStyles.indeterminate, value !== undefined && value > ZERO_THRESHOLD && barStyles.nonZeroDeterminate, color && value !== undefined && barStyles[color], state.bar.className);
    }
    if (state.bar && value !== undefined) {
        state.bar.style = {
            width: Math.min(100, Math.max(0, value / max * 100)) + '%',
            ...state.bar.style
        };
    }
    return state;
};
