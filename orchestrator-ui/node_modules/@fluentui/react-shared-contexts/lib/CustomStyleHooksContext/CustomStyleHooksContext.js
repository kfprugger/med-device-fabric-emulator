'use client';
import * as React from 'react';
/* eslint-enable */ /**
 * @internal
 */ export const CustomStyleHooksContext = React.createContext(undefined);
// eslint-disable-next-line @typescript-eslint/no-empty-function
const noop = ()=>{};
/**
 * @internal
 */ export const CustomStyleHooksProvider = CustomStyleHooksContext.Provider;
/**
 * Gets a custom style hook
 * @param hook - One of the hook properties in CustomStyleHooksContextValue
 * @returns The corresponding hook when defined, otherwise a no-op function.
 */ export const useCustomStyleHook = (hook)=>{
    var _React_useContext;
    var _React_useContext_hook;
    return (_React_useContext_hook = (_React_useContext = React.useContext(CustomStyleHooksContext)) === null || _React_useContext === void 0 ? void 0 : _React_useContext[hook]) !== null && _React_useContext_hook !== void 0 ? _React_useContext_hook : noop;
};
