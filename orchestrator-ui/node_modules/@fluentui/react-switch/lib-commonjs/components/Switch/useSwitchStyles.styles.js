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
    switchClassName: function() {
        return switchClassName;
    },
    switchClassNames: function() {
        return switchClassNames;
    },
    useSwitchStyles_unstable: function() {
        return useSwitchStyles_unstable;
    }
});
const _react = require("@griffel/react");
const switchClassNames = {
    root: 'fui-Switch',
    indicator: 'fui-Switch__indicator',
    input: 'fui-Switch__input',
    label: 'fui-Switch__label'
};
const switchClassName = switchClassNames.root;
// Thumb and track sizes used by the component.
const spaceBetweenThumbAndTrack = 2;
// Medium size dimensions
const trackHeightMedium = 20;
const trackWidthMedium = 40;
const thumbSizeMedium = trackHeightMedium - spaceBetweenThumbAndTrack;
// Small size dimensions (from design mockup)
const trackHeightSmall = 16;
const trackWidthSmall = 32;
const thumbSizeSmall = trackHeightSmall - spaceBetweenThumbAndTrack;
const useRootBaseClassName = /*#__PURE__*/ (0, _react.__resetStyles)("r2i81i2", "rofhmb8", {
    r: [
        ".r2i81i2{align-items:flex-start;box-sizing:border-box;display:inline-flex;position:relative;}",
        ".r2i81i2:focus{outline-style:none;}",
        ".r2i81i2:focus-visible{outline-style:none;}",
        ".r2i81i2[data-fui-focus-within]:focus-within{border-top-color:transparent;border-right-color:transparent;border-bottom-color:transparent;border-left-color:transparent;}",
        ".r2i81i2[data-fui-focus-within]:focus-within::after{content:\"\";position:absolute;pointer-events:none;z-index:1;border:2px solid var(--colorStrokeFocus2);border-radius:var(--borderRadiusMedium);top:calc(2px * -1);right:calc(2px * -1);bottom:calc(2px * -1);left:calc(2px * -1);}",
        ".rofhmb8{align-items:flex-start;box-sizing:border-box;display:inline-flex;position:relative;}",
        ".rofhmb8:focus{outline-style:none;}",
        ".rofhmb8:focus-visible{outline-style:none;}",
        ".rofhmb8[data-fui-focus-within]:focus-within{border-top-color:transparent;border-left-color:transparent;border-bottom-color:transparent;border-right-color:transparent;}",
        ".rofhmb8[data-fui-focus-within]:focus-within::after{content:\"\";position:absolute;pointer-events:none;z-index:1;border:2px solid var(--colorStrokeFocus2);border-radius:var(--borderRadiusMedium);top:calc(2px * -1);left:calc(2px * -1);bottom:calc(2px * -1);right:calc(2px * -1);}"
    ],
    s: [
        "@media (forced-colors: active){.r2i81i2[data-fui-focus-within]:focus-within::after{border-top-color:Highlight;border-right-color:Highlight;border-bottom-color:Highlight;border-left-color:Highlight;}}",
        "@media (forced-colors: active){.rofhmb8[data-fui-focus-within]:focus-within::after{border-top-color:Highlight;border-left-color:Highlight;border-bottom-color:Highlight;border-right-color:Highlight;}}"
    ]
});
const useRootStyles = /*#__PURE__*/ (0, _react.__styles)({
    vertical: {
        Beiy3e4: "f1vx9l62"
    }
}, {
    d: [
        ".f1vx9l62{flex-direction:column;}"
    ]
});
const useIndicatorBaseClassName = /*#__PURE__*/ (0, _react.__resetStyles)("r1c3hft5", null, {
    r: [
        ".r1c3hft5{border-radius:var(--borderRadiusCircular);border:1px solid;line-height:0;box-sizing:border-box;fill:currentColor;flex-shrink:0;font-size:18px;height:20px;margin:var(--spacingVerticalS) var(--spacingHorizontalS);pointer-events:none;transition-duration:var(--durationNormal);transition-timing-function:var(--curveEasyEase);transition-property:background,border,color;width:40px;}",
        ".r1c3hft5>*{transition-duration:var(--durationNormal);transition-timing-function:var(--curveEasyEase);transition-property:transform;}"
    ],
    s: [
        "@media screen and (prefers-reduced-motion: reduce){.r1c3hft5{transition-duration:0.01ms;}}",
        "@media (forced-colors: active){.r1c3hft5{color:CanvasText;}.r1c3hft5>i{forced-color-adjust:none;}}",
        "@media screen and (prefers-reduced-motion: reduce){.r1c3hft5>*{transition-duration:0.01ms;}}"
    ]
});
const useIndicatorStyles = /*#__PURE__*/ (0, _react.__styles)({
    labelAbove: {
        B6of3ja: "f1hu3pq6"
    },
    sizeSmall: {
        Be2twd7: "fses1vf",
        Bqenvij: "fd461yt",
        a9b677: "f1szoe96"
    }
}, {
    d: [
        ".f1hu3pq6{margin-top:0;}",
        ".fses1vf{font-size:14px;}",
        ".fd461yt{height:16px;}",
        ".f1szoe96{width:32px;}"
    ]
});
const useInputBaseClassName = /*#__PURE__*/ (0, _react.__resetStyles)("r12ojtgy", "rhc0e9x", {
    r: [
        ".r12ojtgy{box-sizing:border-box;cursor:pointer;height:100%;margin:0;opacity:0;position:absolute;width:calc(40px + 2 * var(--spacingHorizontalS));}",
        ".r12ojtgy:checked~.fui-Switch__indicator>*{transform:translateX(20px);}",
        ".r12ojtgy:disabled,.r12ojtgy[aria-disabled=\"true\"]{cursor:default;}",
        ".r12ojtgy:disabled~.fui-Switch__indicator,.r12ojtgy[aria-disabled=\"true\"]~.fui-Switch__indicator{color:var(--colorNeutralForegroundDisabled);}",
        ".r12ojtgy:disabled~.fui-Switch__label,.r12ojtgy[aria-disabled=\"true\"]~.fui-Switch__label{cursor:default;color:var(--colorNeutralForegroundDisabled);}",
        ".r12ojtgy:enabled:not(:checked):not([aria-disabled=\"true\"])~.fui-Switch__indicator{color:var(--colorNeutralStrokeAccessible);border-color:var(--colorNeutralStrokeAccessible);}",
        ".r12ojtgy:enabled:not(:checked):not([aria-disabled=\"true\"])~.fui-Switch__label{color:var(--colorNeutralForeground1);}",
        ".r12ojtgy:enabled:not(:checked):not([aria-disabled=\"true\"]):hover~.fui-Switch__indicator{color:var(--colorNeutralStrokeAccessibleHover);border-color:var(--colorNeutralStrokeAccessibleHover);}",
        ".r12ojtgy:enabled:not(:checked):not([aria-disabled=\"true\"]):hover:active~.fui-Switch__indicator{color:var(--colorNeutralStrokeAccessiblePressed);border-color:var(--colorNeutralStrokeAccessiblePressed);}",
        ".r12ojtgy:enabled:checked:not([aria-disabled=\"true\"])~.fui-Switch__indicator{background-color:var(--colorCompoundBrandBackground);color:var(--colorNeutralForegroundInverted);border-color:var(--colorTransparentStroke);}",
        ".r12ojtgy:enabled:checked:not([aria-disabled=\"true\"]):hover~.fui-Switch__indicator{background-color:var(--colorCompoundBrandBackgroundHover);border-color:var(--colorTransparentStrokeInteractive);}",
        ".r12ojtgy:enabled:checked:not([aria-disabled=\"true\"]):hover:active~.fui-Switch__indicator{background-color:var(--colorCompoundBrandBackgroundPressed);border-color:var(--colorTransparentStrokeInteractive);}",
        ".r12ojtgy:disabled:not(:checked)~.fui-Switch__indicator,.r12ojtgy[aria-disabled=\"true\"]:not(:checked)~.fui-Switch__indicator{border-color:var(--colorNeutralStrokeDisabled);}",
        ".r12ojtgy:disabled:checked~.fui-Switch__indicator,.r12ojtgy[aria-disabled=\"true\"]:checked~.fui-Switch__indicator{background-color:var(--colorNeutralBackgroundDisabled);border-color:var(--colorTransparentStrokeDisabled);}",
        ".rhc0e9x{box-sizing:border-box;cursor:pointer;height:100%;margin:0;opacity:0;position:absolute;width:calc(40px + 2 * var(--spacingHorizontalS));}",
        ".rhc0e9x:checked~.fui-Switch__indicator>*{transform:translateX(-20px);}",
        ".rhc0e9x:disabled,.rhc0e9x[aria-disabled=\"true\"]{cursor:default;}",
        ".rhc0e9x:disabled~.fui-Switch__indicator,.rhc0e9x[aria-disabled=\"true\"]~.fui-Switch__indicator{color:var(--colorNeutralForegroundDisabled);}",
        ".rhc0e9x:disabled~.fui-Switch__label,.rhc0e9x[aria-disabled=\"true\"]~.fui-Switch__label{cursor:default;color:var(--colorNeutralForegroundDisabled);}",
        ".rhc0e9x:enabled:not(:checked):not([aria-disabled=\"true\"])~.fui-Switch__indicator{color:var(--colorNeutralStrokeAccessible);border-color:var(--colorNeutralStrokeAccessible);}",
        ".rhc0e9x:enabled:not(:checked):not([aria-disabled=\"true\"])~.fui-Switch__label{color:var(--colorNeutralForeground1);}",
        ".rhc0e9x:enabled:not(:checked):not([aria-disabled=\"true\"]):hover~.fui-Switch__indicator{color:var(--colorNeutralStrokeAccessibleHover);border-color:var(--colorNeutralStrokeAccessibleHover);}",
        ".rhc0e9x:enabled:not(:checked):not([aria-disabled=\"true\"]):hover:active~.fui-Switch__indicator{color:var(--colorNeutralStrokeAccessiblePressed);border-color:var(--colorNeutralStrokeAccessiblePressed);}",
        ".rhc0e9x:enabled:checked:not([aria-disabled=\"true\"])~.fui-Switch__indicator{background-color:var(--colorCompoundBrandBackground);color:var(--colorNeutralForegroundInverted);border-color:var(--colorTransparentStroke);}",
        ".rhc0e9x:enabled:checked:not([aria-disabled=\"true\"]):hover~.fui-Switch__indicator{background-color:var(--colorCompoundBrandBackgroundHover);border-color:var(--colorTransparentStrokeInteractive);}",
        ".rhc0e9x:enabled:checked:not([aria-disabled=\"true\"]):hover:active~.fui-Switch__indicator{background-color:var(--colorCompoundBrandBackgroundPressed);border-color:var(--colorTransparentStrokeInteractive);}",
        ".rhc0e9x:disabled:not(:checked)~.fui-Switch__indicator,.rhc0e9x[aria-disabled=\"true\"]:not(:checked)~.fui-Switch__indicator{border-color:var(--colorNeutralStrokeDisabled);}",
        ".rhc0e9x:disabled:checked~.fui-Switch__indicator,.rhc0e9x[aria-disabled=\"true\"]:checked~.fui-Switch__indicator{background-color:var(--colorNeutralBackgroundDisabled);border-color:var(--colorTransparentStrokeDisabled);}"
    ],
    s: [
        "@media (forced-colors: active){.r12ojtgy:disabled~.fui-Switch__indicator,.r12ojtgy[aria-disabled=\"true\"]~.fui-Switch__indicator{color:GrayText;border-color:GrayText;}.r12ojtgy:disabled~.fui-Switch__label,.r12ojtgy[aria-disabled=\"true\"]~.fui-Switch__label{color:GrayText;}.r12ojtgy:hover{color:CanvasText;}.r12ojtgy:hover:active{color:CanvasText;}.r12ojtgy:enabled:checked:not([aria-disabled=\"true\"]):hover~.fui-Switch__indicator{background-color:Highlight;color:Canvas;}.r12ojtgy:enabled:checked:not([aria-disabled=\"true\"]):hover:active~.fui-Switch__indicator{background-color:Highlight;color:Canvas;}.r12ojtgy:enabled:checked:not([aria-disabled=\"true\"])~.fui-Switch__indicator{background-color:Highlight;color:Canvas;}}",
        "@media (forced-colors: active){.rhc0e9x:disabled~.fui-Switch__indicator,.rhc0e9x[aria-disabled=\"true\"]~.fui-Switch__indicator{color:GrayText;border-color:GrayText;}.rhc0e9x:disabled~.fui-Switch__label,.rhc0e9x[aria-disabled=\"true\"]~.fui-Switch__label{color:GrayText;}.rhc0e9x:hover{color:CanvasText;}.rhc0e9x:hover:active{color:CanvasText;}.rhc0e9x:enabled:checked:not([aria-disabled=\"true\"]):hover~.fui-Switch__indicator{background-color:Highlight;color:Canvas;}.rhc0e9x:enabled:checked:not([aria-disabled=\"true\"]):hover:active~.fui-Switch__indicator{background-color:Highlight;color:Canvas;}.rhc0e9x:enabled:checked:not([aria-disabled=\"true\"])~.fui-Switch__indicator{background-color:Highlight;color:Canvas;}}"
    ]
});
const useInputStyles = /*#__PURE__*/ (0, _react.__styles)({
    before: {
        j35jbq: [
            "f1e31b4d",
            "f1vgc2s3"
        ],
        Bhzewxz: "f15twtuk"
    },
    after: {
        oyh7mz: [
            "f1vgc2s3",
            "f1e31b4d"
        ],
        Bhzewxz: "f15twtuk"
    },
    above: {
        B5kzvoi: "f1yab3r1",
        Bqenvij: "f1aar7gd",
        a9b677: "fly5x3f"
    },
    sizeSmall: {
        a9b677: "f83td6f",
        oedwrj: [
            "f1t5zc5r",
            "fmy58zi"
        ]
    }
}, {
    d: [
        ".f1e31b4d{right:0;}",
        ".f1vgc2s3{left:0;}",
        ".f15twtuk{top:0;}",
        ".f1yab3r1{bottom:0;}",
        ".f1aar7gd{height:calc(20px + var(--spacingVerticalS));}",
        ".fly5x3f{width:100%;}",
        ".f83td6f{width:calc(32px + 2 * var(--spacingHorizontalS));}",
        ".f1t5zc5r:checked~.fui-Switch__indicator>*{transform:translateX(16px);}",
        ".fmy58zi:checked~.fui-Switch__indicator>*{transform:translateX(-16px);}"
    ]
});
// Can't use makeResetStyles here because Label is a component that may itself use makeResetStyles.
const useLabelStyles = /*#__PURE__*/ (0, _react.__styles)({
    base: {
        Bceei9c: "f1k6fduh",
        jrapky: "f49ad5g",
        B6of3ja: "f1xlvstr",
        Byoj8tv: 0,
        uwmqm3: 0,
        z189sj: 0,
        z8tnut: 0,
        B0ocmuz: "f1f5q0n8"
    },
    sizeSmall: {
        Be2twd7: "fy9rknc",
        Bg96gwp: "fwrc4pm",
        jrapky: "f1suqah5",
        B6of3ja: "f26bnac"
    },
    above: {
        z8tnut: "f1ywm7hm",
        Byoj8tv: "f14wxoun",
        a9b677: "fly5x3f"
    },
    after: {
        uwmqm3: [
            "fruq291",
            "f7x41pl"
        ]
    },
    before: {
        z189sj: [
            "f7x41pl",
            "fruq291"
        ]
    }
}, {
    d: [
        ".f1k6fduh{cursor:pointer;}",
        ".f49ad5g{margin-bottom:calc((20px - var(--lineHeightBase300)) / 2);}",
        ".f1xlvstr{margin-top:calc((20px - var(--lineHeightBase300)) / 2);}",
        [
            ".f1f5q0n8{padding:var(--spacingVerticalS) var(--spacingHorizontalS);}",
            {
                p: -1
            }
        ],
        ".fy9rknc{font-size:var(--fontSizeBase200);}",
        ".fwrc4pm{line-height:var(--lineHeightBase200);}",
        ".f1suqah5{margin-bottom:calc((16px - var(--lineHeightBase200)) / 2);}",
        ".f26bnac{margin-top:calc((16px - var(--lineHeightBase200)) / 2);}",
        ".f1ywm7hm{padding-top:var(--spacingVerticalXS);}",
        ".f14wxoun{padding-bottom:var(--spacingVerticalXS);}",
        ".fly5x3f{width:100%;}",
        ".fruq291{padding-left:var(--spacingHorizontalXS);}",
        ".f7x41pl{padding-right:var(--spacingHorizontalXS);}"
    ]
});
const useSwitchStyles_unstable = (state)=>{
    'use no memo';
    const rootBaseClassName = useRootBaseClassName();
    const rootStyles = useRootStyles();
    const indicatorBaseClassName = useIndicatorBaseClassName();
    const indicatorStyles = useIndicatorStyles();
    const inputBaseClassName = useInputBaseClassName();
    const inputStyles = useInputStyles();
    const labelStyles = useLabelStyles();
    const { label, labelPosition, size } = state;
    state.root.className = (0, _react.mergeClasses)(switchClassNames.root, rootBaseClassName, labelPosition === 'above' && rootStyles.vertical, state.root.className);
    state.indicator.className = (0, _react.mergeClasses)(switchClassNames.indicator, indicatorBaseClassName, label && labelPosition === 'above' && indicatorStyles.labelAbove, size === 'small' && indicatorStyles.sizeSmall, state.indicator.className);
    state.input.className = (0, _react.mergeClasses)(switchClassNames.input, inputBaseClassName, label && inputStyles[labelPosition], size === 'small' && inputStyles.sizeSmall, state.input.className);
    if (state.label) {
        state.label.className = (0, _react.mergeClasses)(switchClassNames.label, labelStyles.base, labelStyles[labelPosition], size === 'small' && labelStyles.sizeSmall, state.label.className);
    }
    return state;
};
