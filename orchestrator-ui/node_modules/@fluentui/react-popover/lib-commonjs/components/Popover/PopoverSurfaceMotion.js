"use strict";
Object.defineProperty(exports, "__esModule", {
    value: true
});
Object.defineProperty(exports, "PopoverSurfaceMotion", {
    enumerable: true,
    get: function() {
        return PopoverSurfaceMotion;
    }
});
const _reactmotion = require("@fluentui/react-motion");
const _reactmotioncomponentspreview = require("@fluentui/react-motion-components-preview");
const _reactpositioning = require("@fluentui/react-positioning");
// Shared timing constants for the enter animation.
const duration = _reactmotion.motionTokens.durationSlower;
const easing = _reactmotion.motionTokens.curveDecelerateMid;
const PopoverSurfaceMotion = (0, _reactmotion.createPresenceComponent)(({ distance = 10 })=>({
        enter: [
            (0, _reactmotioncomponentspreview.fadeAtom)({
                duration,
                easing,
                direction: 'enter'
            }),
            {
                // slideAtom produces translate keyframes from `outX`/`outY` → `0px`.
                // The `outX`/`outY` values read the positioning-provided CSS variables and scale
                // them by `distance` so the surface slides in from the correct direction.
                ...(0, _reactmotioncomponentspreview.slideAtom)({
                    duration,
                    easing,
                    direction: 'enter',
                    outX: `calc(var(${_reactpositioning.POSITIONING_SLIDE_DIRECTION_VAR_X}, 0px) * ${distance})`,
                    outY: `calc(var(${_reactpositioning.POSITIONING_SLIDE_DIRECTION_VAR_Y}, 0px) * ${distance})`
                }),
                // 'accumulate' compositing adds this effect's transform on top of the element's
                // existing transform, preserving any transform applied by the positioning engine.
                composite: 'accumulate'
            }
        ],
        // No exit animation — the surface unmounts immediately on close.
        exit: []
    }));
