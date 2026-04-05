import * as React from 'react';
import { CheckmarkCircleFilled, InfoFilled, WarningFilled, DiamondDismissFilled } from '@fluentui/react-icons';
export function getIntentIcon(intent) {
    switch(intent){
        case 'info':
            return /*#__PURE__*/ React.createElement(InfoFilled, null);
        case 'warning':
            return /*#__PURE__*/ React.createElement(WarningFilled, null);
        case 'error':
            return /*#__PURE__*/ React.createElement(DiamondDismissFilled, null);
        case 'success':
            return /*#__PURE__*/ React.createElement(CheckmarkCircleFilled, null);
        default:
            return null;
    }
}
