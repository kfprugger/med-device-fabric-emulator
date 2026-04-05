/**
 * Brand icons for Microsoft Azure, Microsoft Fabric, Healthcare Data Solutions,
 * GitHub, and YouTube. Uses official SVG marks rendered inline for reliability.
 * Per the Fabric theming guide: use Fluent tokens for colors, not raw hex.
 */

import { makeStyles, tokens } from "@fluentui/react-components";

const useStyles = makeStyles({
  icon: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    flexShrink: 0,
  },
  link: {
    display: "inline-flex",
    alignItems: "center",
    textDecoration: "none",
    color: "inherit",
    transition: "opacity 0.15s ease",
    ":hover": {
      opacity: 0.8,
    },
  },
});

interface IconProps {
  size?: number;
}

/** Microsoft Azure icon — official SVG from Azure Public Service Icons */
export function AzureIcon({ size = 20 }: IconProps) {
  const styles = useStyles();
  return (
    <span className={styles.icon} title="Microsoft Azure">
      <img
        src="/azure_logo.svg"
        alt="Microsoft Azure"
        width={size}
        height={size}
        style={{ display: "block" }}
      />
    </span>
  );
}

/** Microsoft Fabric icon — official SVG from @fabric-msft/svg-icons */
export function FabricIcon({ size = 20 }: IconProps) {
  const styles = useStyles();
  return (
    <span className={styles.icon} title="Microsoft Fabric">
      <img
        src="/fabric_16_color.svg"
        alt="Microsoft Fabric"
        width={size}
        height={size}
        style={{ display: "block" }}
      />
    </span>
  );
}

/** Healthcare Data Solutions icon — official SVG from @fabric-msft/svg-icons */
export function HdsIcon({ size = 20 }: IconProps) {
  const styles = useStyles();
  return (
    <span className={styles.icon} title="Healthcare Data Solutions">
      <img
        src="/healthcare_20_item.svg"
        alt="Healthcare Data Solutions"
        width={size}
        height={size}
        style={{ display: "block" }}
      />
    </span>
  );
}

/** GitHub icon */
export function GitHubIcon({ size = 20 }: IconProps) {
  const styles = useStyles();
  return (
    <span className={styles.icon}>
      <svg width={size} height={size} viewBox="0 0 24 24" fill={tokens.colorNeutralForeground2}>
        <path d="M12 2C6.477 2 2 6.477 2 12c0 4.418 2.865 8.166 6.839 9.489.5.092.682-.217.682-.482 0-.237-.009-.866-.013-1.7-2.782.604-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.463-1.11-1.463-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.646.35-1.086.636-1.337-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.269 2.75 1.025A9.578 9.578 0 0112 6.836a9.59 9.59 0 012.504.337c1.909-1.294 2.747-1.025 2.747-1.025.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.163 22 16.418 22 12c0-5.523-4.477-10-10-10z" />
      </svg>
    </span>
  );
}

/** YouTube play icon */
export function YouTubeIcon({ size = 20 }: IconProps) {
  const styles = useStyles();
  return (
    <span className={styles.icon}>
      <svg width={size} height={size} viewBox="0 0 24 24" fill="none">
        <path
          d="M23.498 6.186a3.016 3.016 0 00-2.122-2.136C19.505 3.545 12 3.545 12 3.545s-7.505 0-9.377.505A3.017 3.017 0 00.502 6.186C0 8.07 0 12 0 12s0 3.93.502 5.814a3.016 3.016 0 002.122 2.136c1.871.505 9.376.505 9.376.505s7.505 0 9.377-.505a3.015 3.015 0 002.122-2.136C24 15.93 24 12 24 12s0-3.93-.502-5.814z"
          fill="#FF0000"
        />
        <path d="M9.545 15.568V8.432L15.818 12l-6.273 3.568z" fill="white" />
      </svg>
    </span>
  );
}

interface BrandLinkProps {
  href: string;
  label: string;
  children: React.ReactNode;
}

/** Accessible external link wrapper */
export function BrandLink({ href, label, children }: BrandLinkProps) {
  const styles = useStyles();
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={styles.link}
      aria-label={label}
      title={label}
    >
      {children}
    </a>
  );
}
