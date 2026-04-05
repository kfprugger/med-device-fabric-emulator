'use client';
import * as React from 'react';
import { PresenceGroupChildContext } from '../contexts/PresenceGroupChildContext';
/**
 * Provides context for a single child of a `PresenceGroup`. Exists only to make a stable context value for a child.
 * Not intended for direct use.
 *
 * @internal
 */ export const PresenceGroupItemProvider = (props)=>{
    const { appear, childKey, onExit, visible, unmountOnExit } = props;
    const contextValue = React.useMemo(()=>({
            appear,
            visible,
            onExit: ()=>onExit(childKey),
            unmountOnExit
        }), [
        appear,
        childKey,
        onExit,
        visible,
        unmountOnExit
    ]);
    return /*#__PURE__*/ React.createElement(PresenceGroupChildContext.Provider, {
        value: contextValue
    }, props.children);
};
