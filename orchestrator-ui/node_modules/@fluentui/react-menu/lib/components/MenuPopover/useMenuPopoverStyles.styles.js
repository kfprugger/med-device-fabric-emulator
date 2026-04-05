'use client';

import { mergeClasses, __styles } from '@griffel/react';
import { tokens, typographyStyles } from '@fluentui/react-theme';
export const menuPopoverClassNames = {
  root: 'fui-MenuPopover'
};
const useStyles = /*#__PURE__*/__styles({
  root: {
    Beyfa6y: 0,
    Bbmb7ep: 0,
    Btl43ni: 0,
    B7oj6ja: 0,
    Dimara: "ft85np5",
    De3pzq: "fxugw4r",
    sj55zd: "f19n0e5",
    B7ck84d: "f1ewtqcl",
    Bf4jedk: "fl8fusi",
    B2u0y6b: "f1kaai3v",
    B68tc82: "f1p9o1ba",
    a9b677: "f1ahpp82",
    E5pizo: "f1hg901r",
    Byoj8tv: 0,
    uwmqm3: 0,
    z189sj: 0,
    z8tnut: 0,
    B0ocmuz: "fd3pd8h",
    Bgfg5da: 0,
    B9xav0g: 0,
    oivjwe: 0,
    Bn0qgzm: 0,
    B4g9neb: 0,
    zhjwy3: 0,
    wvpqe5: 0,
    ibv6hh: 0,
    u1mtju: 0,
    h3c5rm: 0,
    vrafjx: 0,
    Bekrc4i: 0,
    i8vvqc: 0,
    g2u3we: 0,
    icvyot: 0,
    B4j52fo: 0,
    irswps: "f9ggezi",
    Bahqtrf: "fk6fouc",
    Be2twd7: "fkhj508",
    Bhrd7zp: "figsok6",
    Bg96gwp: "f1i3iumi"
  }
}, {
  d: [[".ft85np5{border-radius:var(--borderRadiusMedium);}", {
    p: -1
  }], ".fxugw4r{background-color:var(--colorNeutralBackground1);}", ".f19n0e5{color:var(--colorNeutralForeground1);}", ".f1ewtqcl{box-sizing:border-box;}", ".fl8fusi{min-width:138px;}", ".f1kaai3v{max-width:300px;}", ".f1p9o1ba{overflow-x:hidden;}", ".f1ahpp82{width:max-content;}", ".f1hg901r{box-shadow:var(--shadow16);}", [".fd3pd8h{padding:4px;}", {
    p: -1
  }], [".f9ggezi{border:1px solid var(--colorTransparentStroke);}", {
    p: -2
  }], ".fk6fouc{font-family:var(--fontFamilyBase);}", ".fkhj508{font-size:var(--fontSizeBase300);}", ".figsok6{font-weight:var(--fontWeightRegular);}", ".f1i3iumi{line-height:var(--lineHeightBase300);}"]
});
/**
 * Apply styling to the Menu slots based on the state
 */
export const useMenuPopoverStyles_unstable = state => {
  'use no memo';

  const styles = useStyles();
  state.root.className = mergeClasses(menuPopoverClassNames.root, styles.root, state.root.className);
  return state;
};