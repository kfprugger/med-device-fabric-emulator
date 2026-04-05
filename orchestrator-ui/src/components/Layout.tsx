import { Outlet, useNavigate, useLocation } from "react-router-dom";
import {
  Tab,
  TabList,
  Text,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import {
  RocketRegular,
  HistoryRegular,
  DeleteRegular,
} from "@fluentui/react-icons";
import { spacing } from "../theme";
import {
  AzureIcon,
  FabricIcon,
  HdsIcon,
  GitHubIcon,
  YouTubeIcon,
} from "./BrandIcons";
import { AnimatedBackground } from "./AnimatedBackground";

const useStyles = makeStyles({
  root: {
    display: "flex",
    flexDirection: "column",
    minHeight: "100vh",
    backgroundImage: "url('/bg-dataflow.svg')",
    backgroundSize: "cover",
    backgroundPosition: "center",
    backgroundRepeat: "no-repeat",
    backgroundAttachment: "fixed",
    backgroundColor: tokens.colorNeutralBackground2,
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: spacing.l,
    paddingTop: spacing.m,
    paddingBottom: spacing.m,
    paddingLeft: spacing.xxl,
    paddingRight: spacing.xxl,
    backgroundColor: tokens.colorNeutralBackground1,
    borderBottom: `2px solid ${tokens.colorBrandForeground1}`,
  },
  headerLeft: {
    display: "flex",
    alignItems: "center",
    gap: spacing.s,
    flex: 1,
  },
  headerTitle: {
    color: tokens.colorNeutralForeground1,
    fontWeight: tokens.fontWeightSemibold,
    fontSize: tokens.fontSizeBase500,
    lineHeight: tokens.lineHeightBase500,
  },
  brandAccent: {
    color: tokens.colorBrandForeground1,
    fontWeight: tokens.fontWeightBold,
    fontSize: tokens.fontSizeBase500,
    lineHeight: tokens.lineHeightBase500,
  },
  headerIcons: {
    display: "flex",
    alignItems: "center",
    gap: spacing.s,
  },
  iconPill: {
    display: "inline-flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalXS,
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
    borderRadius: tokens.borderRadiusMedium,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    backgroundColor: tokens.colorNeutralBackground1,
    fontSize: tokens.fontSizeBase200,
    fontWeight: tokens.fontWeightSemibold,
    color: tokens.colorNeutralForeground2,
    textDecoration: "none",
    transition: "all 0.15s ease",
    cursor: "pointer",
    ":hover": {
      backgroundColor: tokens.colorNeutralBackground1Hover,
      border: `1px solid ${tokens.colorBrandForeground1}`,
      color: tokens.colorBrandForeground1,
      boxShadow: tokens.shadow4,
      transform: "translateY(-1px)",
    },
  },
  iconDivider: {
    width: "1px",
    height: "24px",
    backgroundColor: tokens.colorNeutralStroke2,
    marginLeft: tokens.spacingHorizontalXS,
    marginRight: tokens.spacingHorizontalXS,
  },
  nav: {
    paddingLeft: spacing.xxl,
    paddingRight: spacing.xxl,
    backgroundColor: tokens.colorNeutralBackground1,
    borderBottom: `1px solid ${tokens.colorNeutralStroke2}`,
  },
  content: {
    flex: 1,
    padding: spacing.xxl,
    maxWidth: "1200px",
    margin: "0 auto",
    width: "100%",
  },
  footer: {
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: spacing.m,
    padding: `${spacing.s} ${spacing.xxl}`,
    backgroundColor: tokens.colorNeutralBackground1,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
    fontSize: tokens.fontSizeBase200,
    color: tokens.colorNeutralForeground3,
  },
  footerLink: {
    display: "inline-flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalXS,
    padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalS}`,
    borderRadius: tokens.borderRadiusMedium,
    color: tokens.colorNeutralForeground2,
    textDecoration: "none",
    fontWeight: tokens.fontWeightSemibold,
    transition: "all 0.15s ease",
    ":hover": {
      color: tokens.colorBrandForeground1,
      backgroundColor: tokens.colorNeutralBackground1Hover,
    },
  },
});

export function Layout() {
  const styles = useStyles();
  const navigate = useNavigate();
  const location = useLocation();

  const currentTab =
    location.pathname.startsWith("/monitor")
      ? "/deploy"
      : location.pathname.startsWith("/deploy")
        ? "/deploy"
        : location.pathname.startsWith("/history")
          ? "/history"
          : location.pathname.startsWith("/teardown")
            ? "/teardown"
            : "/deploy";

  return (
    <div className={styles.root}>
      <AnimatedBackground />
      <div className={styles.header}>
        <div className={styles.headerLeft}>
          <Text className={styles.brandAccent}>Fabric</Text>
          <Text className={styles.headerTitle}>
            Medical Device FHIR Platform — Deployment Orchestrator
          </Text>
        </div>
        <div className={styles.headerIcons}>
          <a
            href="https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/overview"
            target="_blank"
            rel="noopener noreferrer"
            className={styles.iconPill}
            title="Healthcare Data Solutions"
          >
            <HdsIcon size={18} /> HDS
          </a>
          <a
            href="https://portal.azure.com"
            target="_blank"
            rel="noopener noreferrer"
            className={styles.iconPill}
            title="Azure Portal"
            style={{ color: "#0078D4", borderColor: "#0078D4" }}
          >
            <AzureIcon size={18} /> Azure
          </a>
          <a
            href="https://app.fabric.microsoft.com/home?experience=fabric-developer"
            target="_blank"
            rel="noopener noreferrer"
            className={styles.iconPill}
            title="Microsoft Fabric"
            style={{ color: "#117865", borderColor: "#117865" }}
          >
            <FabricIcon size={18} /> Fabric
          </a>
          <div className={styles.iconDivider} />
          <a
            href="https://github.com/yourrepo/med-device-fabric-emulator"
            target="_blank"
            rel="noopener noreferrer"
            className={styles.iconPill}
            title="View source on GitHub"
          >
            <GitHubIcon size={16} /> GitHub
          </a>
          <a
            href="https://aka.ms/fabrichlsrti"
            target="_blank"
            rel="noopener noreferrer"
            className={styles.iconPill}
            title="Watch demo video"
          >
            <YouTubeIcon size={16} /> Demo
          </a>
        </div>
      </div>

      <div className={styles.nav}>
        <TabList
          selectedValue={currentTab}
          onTabSelect={(_, data) => navigate(data.value as string)}
        >
          <Tab value="/deploy" icon={<RocketRegular />}>
            Deploy
          </Tab>
          <Tab value="/history" icon={<HistoryRegular />}>
            History
          </Tab>
          <Tab value="/teardown" icon={<DeleteRegular />}>
            Teardown
          </Tab>
        </TabList>
      </div>

      <div className={styles.content}>
        <Outlet />
      </div>

      <div className={styles.footer}>
        <Text size={200}>Medical Device FHIR Integration Platform</Text>
        <span className={styles.iconDivider} />
        <a
          href="https://github.com/yourrepo/med-device-fabric-emulator"
          target="_blank"
          rel="noopener noreferrer"
          className={styles.footerLink}
        >
          <GitHubIcon size={14} /> Source
        </a>
        <a
          href="https://aka.ms/fabrichlsrti"
          target="_blank"
          rel="noopener noreferrer"
          className={styles.footerLink}
        >
          <YouTubeIcon size={14} /> Demo
        </a>
      </div>
    </div>
  );
}
