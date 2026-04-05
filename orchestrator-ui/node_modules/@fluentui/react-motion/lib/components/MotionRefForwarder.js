'use client';
import * as React from 'react';
const MotionRefForwarderContext = /*#__PURE__*/ React.createContext(undefined);
/**
 * A hook that reads the ref forwarded by `MotionRefForwarder` from context.
 * Used in child components to merge the motion ref into the root slot ref.
 *
 * @internal
 */ export function useMotionForwardedRef() {
    return React.useContext(MotionRefForwarderContext);
}
/**
 * A component that forwards a ref to its children via a React context.
 * This is used to pass a motion component's ref through to the actual surface element,
 * since motion components wrap their children and the ref needs to reach the inner element.
 *
 * @internal
 */ export const MotionRefForwarder = /*#__PURE__*/ React.forwardRef((props, ref)=>{
    return /*#__PURE__*/ React.createElement(MotionRefForwarderContext.Provider, {
        value: ref
    }, props.children);
});
MotionRefForwarder.displayName = 'MotionRefForwarder';
/**
 * Resets the MotionRefForwarder context to `undefined` for its children.
 * Render this in components that consume `useMotionForwardedRef()` and render
 * arbitrary user content, to prevent the context from leaking to descendants.
 *
 * @internal
 */ export const MotionRefForwarderReset = (props)=>{
    return /*#__PURE__*/ React.createElement(MotionRefForwarderContext.Provider, {
        value: undefined
    }, props.children);
};
MotionRefForwarderReset.displayName = 'MotionRefForwarderReset';
