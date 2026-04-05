  import { jsx as _jsx, jsxs as _jsxs } from "@fluentui/react-jsx-runtime/jsx-runtime";
import * as React from 'react';
import { MotionRefForwarder } from '@fluentui/react-motion';
import { assertSlots } from '@fluentui/react-utilities';
import { MenuProvider } from '../../contexts/menuContext';
/**
 * Render the final JSX of Menu
 */ export const renderMenu_unstable = (state, contextValues)=>{
    assertSlots(state);
    return /*#__PURE__*/ _jsxs(MenuProvider, {
        value: contextValues.menu,
        children: [
            state.menuTrigger,
            state.menuPopover && /*#__PURE__*/ _jsx(state.surfaceMotion, {
                children: /*#__PURE__*/ _jsx(MotionRefForwarder, {
                    children: state.menuPopover
                })
            })
        ]
    });
};
