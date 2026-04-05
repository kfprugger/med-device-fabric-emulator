'use client';
import * as React from 'react';
const buttonContext = React.createContext(undefined);
const buttonContextDefaultValue = {};
/**
 * Internal context provider used to update default values between internal components
 *
 * @internal
 */ export const ButtonContextProvider = buttonContext.Provider;
/**
 * Internal context hook used to update default values between internal components
 *
 * @internal
 */ export const useButtonContext = ()=>{
    var _React_useContext;
    return (_React_useContext = React.useContext(buttonContext)) !== null && _React_useContext !== void 0 ? _React_useContext : buttonContextDefaultValue;
};
