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
    MotionBehaviourProvider: function() {
        return _MotionBehaviourContext.MotionBehaviourProvider;
    },
    MotionRefForwarder: function() {
        return _MotionRefForwarder.MotionRefForwarder;
    },
    MotionRefForwarderReset: function() {
        return _MotionRefForwarder.MotionRefForwarderReset;
    },
    PresenceGroup: function() {
        return _PresenceGroup.PresenceGroup;
    },
    PresenceGroupChildProvider: function() {
        return _PresenceGroupChildContext.PresenceGroupChildProvider;
    },
    createMotionComponent: function() {
        return _createMotionComponent.createMotionComponent;
    },
    createMotionComponentVariant: function() {
        return _createMotionComponentVariant.createMotionComponentVariant;
    },
    createPresenceComponent: function() {
        return _createPresenceComponent.createPresenceComponent;
    },
    createPresenceComponentVariant: function() {
        return _createPresenceComponentVariant.createPresenceComponentVariant;
    },
    curves: function() {
        return _motionTokens.curves;
    },
    durations: function() {
        return _motionTokens.durations;
    },
    motionSlot: function() {
        return _motionSlot.motionSlot;
    },
    motionTokens: function() {
        return _motionTokens.motionTokens;
    },
    presenceMotionSlot: function() {
        return _presenceMotionSlot.presenceMotionSlot;
    },
    useMotionForwardedRef: function() {
        return _MotionRefForwarder.useMotionForwardedRef;
    },
    usePresenceGroupChildContext: function() {
        return _PresenceGroupChildContext.usePresenceGroupChildContext;
    }
});
const _motionTokens = require("./motions/motionTokens");
const _createMotionComponent = require("./factories/createMotionComponent");
const _createMotionComponentVariant = require("./factories/createMotionComponentVariant");
const _createPresenceComponent = require("./factories/createPresenceComponent");
const _createPresenceComponentVariant = require("./factories/createPresenceComponentVariant");
const _PresenceGroup = require("./components/PresenceGroup");
const _MotionRefForwarder = require("./components/MotionRefForwarder");
const _motionSlot = require("./slots/motionSlot");
const _presenceMotionSlot = require("./slots/presenceMotionSlot");
const _PresenceGroupChildContext = require("./contexts/PresenceGroupChildContext");
const _MotionBehaviourContext = require("./contexts/MotionBehaviourContext");
