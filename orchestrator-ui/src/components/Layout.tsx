import { Outlet, useNavigate, useLocation } from "react-router-dom";
import {
  Badge,
  Button,
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
  ShieldTaskRegular,
  WeatherMoonRegular,
  WeatherSunnyRegular,
} from "@fluentui/react-icons";
import { spacing } from "../theme";
import { useAppState } from "../AppState";
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
    justifyContent: "space-between",
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
  headerLogo: {
    width: "28px",
    height: "28px",
    flexShrink: 0,
    filter: "drop-shadow(0 1px 2px rgba(0,0,0,0.3))",
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
    position: "relative",
  },
  resourcesMenu: {
    position: "relative",
  },
  resourcesSummary: {
    listStyleType: "none",
    "::-webkit-details-marker": { display: "none" },
  },
  resourcesPanel: {
    position: "absolute",
    top: "calc(100% + 6px)",
    right: 0,
    zIndex: 50,
    minWidth: "180px",
    display: "grid",
    gap: tokens.spacingVerticalXS,
    padding: tokens.spacingHorizontalS,
    borderRadius: tokens.borderRadiusMedium,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    backgroundColor: tokens.colorNeutralBackground1,
    boxShadow: tokens.shadow16,
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
  navInner: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.s,
    flexWrap: "wrap",
    minHeight: "48px",
  },
  contextStrip: {
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    gap: tokens.spacingHorizontalXS,
    flexWrap: "wrap",
    paddingTop: tokens.spacingVerticalXXS,
    paddingBottom: tokens.spacingVerticalXXS,
  },
  contextPill: {
    display: "flex",
    flexDirection: "column",
    gap: "1px",
    minWidth: "124px",
    maxWidth: "220px",
    padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalS}`,
    borderRadius: tokens.borderRadiusSmall,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    backgroundColor: tokens.colorNeutralBackground2,
  },
  contextTitle: {
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase100,
    textTransform: "uppercase",
    letterSpacing: "0.04em",
    lineHeight: "12px",
  },
  contextValue: {
    color: tokens.colorNeutralForeground1,
    fontSize: tokens.fontSizeBase100,
    fontWeight: tokens.fontWeightSemibold,
    overflow: "hidden",
    textOverflow: "ellipsis",
    display: "-webkit-box",
    WebkitLineClamp: "1",
    WebkitBoxOrient: "vertical",
    lineHeight: "14px",
    maxHeight: "14px",
  },
  contextSubtext: {
    color: tokens.colorNeutralForeground2,
    fontSize: "11px",
    overflow: "hidden",
    textOverflow: "ellipsis",
    display: "-webkit-box",
    WebkitLineClamp: "1",
    WebkitBoxOrient: "vertical",
    lineHeight: "13px",
    maxHeight: "13px",
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
  const { selectedSubscription, subscriptions, authContext, authContextLoading } = useAppState();

  const selectedSubscriptionInfo = subscriptions.find((subscription) => subscription.id === selectedSubscription);
  const selectedSubscriptionLabel =
    selectedSubscriptionInfo?.name || authContext?.cli.subscriptionName || authContext?.pwsh.subscriptionName || "Not selected";
  const selectedSubscriptionId =
    selectedSubscriptionInfo?.id || authContext?.cli.subscriptionId || authContext?.pwsh.subscriptionId || "";
  const selectedSubscriptionShortId = selectedSubscriptionId ? selectedSubscriptionId.slice(0, 8) : "";
  const cliLabel = authContextLoading
    ? "Checking Azure CLI..."
    : authContext?.cli.loggedIn
      ? authContext.cli.user
      : authContext?.cli.error || "Not logged in";
  const pwshLabel = authContextLoading
    ? "Checking Az PowerShell..."
    : authContext?.pwsh.loggedIn
      ? authContext.pwsh.user
      : authContext?.pwsh.error || "Not logged in";
  const contextReady = !!authContext?.ready;
  const contextAligned = !!authContext?.aligned.subscription && !!authContext?.aligned.tenant;
  const isDarkTheme = typeof window !== "undefined" && window.localStorage.getItem("orchestrator-theme") === "dark";
  const toggleTheme = () => {
    if (typeof window === "undefined") return;
    const current = window.localStorage.getItem("orchestrator-theme");
    window.localStorage.setItem("orchestrator-theme", current === "dark" ? "light" : "dark");
    window.dispatchEvent(new Event("orchestrator-theme-change"));
  };

  const currentTab =
    location.pathname.startsWith("/monitor")
      ? "/deploy"
      : location.pathname.startsWith("/preflight")
        ? "/preflight"
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
          <img src="/favicon.svg" alt="" className={styles.headerLogo} />
          <Text className={styles.brandAccent}>Fabric</Text>
          <Text className={styles.headerTitle}>
            Medical Device FHIR Platform — Deployment Orchestrator
          </Text>
        </div>
        <div className={styles.headerIcons}>
          <Button
            appearance="subtle"
            size="small"
            icon={isDarkTheme ? <WeatherSunnyRegular /> : <WeatherMoonRegular />}
            onClick={toggleTheme}
          >
            {isDarkTheme ? "Light" : "Dark"}
          </Button>
          <details className={styles.resourcesMenu}>
            <summary className={`${styles.iconPill} ${styles.resourcesSummary}`}>Resources</summary>
            <div className={styles.resourcesPanel}>
              <a
                href="https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/overview"
                target="_blank"
                rel="noopener noreferrer"
                className={styles.iconPill}
                title="Healthcare Data Solutions"
              >
                <HdsIcon size={18} /> HDS docs
              </a>
              <a
                href="https://portal.azure.com"
                target="_blank"
                rel="noopener noreferrer"
                className={styles.iconPill}
                title="Azure Portal"
                style={{ color: tokens.colorPaletteBlueForeground2, borderColor: tokens.colorPaletteBlueBorderActive }}
              >
                <AzureIcon size={18} /> Azure Portal
              </a>
              <a
                href="https://app.fabric.microsoft.com/home?experience=fabric-developer"
                target="_blank"
                rel="noopener noreferrer"
                className={styles.iconPill}
                title="Microsoft Fabric"
              >
                <FabricIcon size={18} /> Fabric Portal
              </a>
              <a
                href="https://github.com/kfprugger/med-device-fabric-emulator"
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
          </details>
        </div>
      </div>

      <div className={styles.nav}>
        <div className={styles.navInner}>
          <TabList
            selectedValue={currentTab}
            onTabSelect={(_, data) => navigate(data.value as string)}
          >
            <Tab value="/preflight" icon={<ShieldTaskRegular />}>
              Preflight
            </Tab>
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

          <div className={styles.contextStrip}>
            <div className={styles.contextPill}>
              <Text className={styles.contextTitle}>Selected Subscription</Text>
              <Text className={styles.contextValue} title={selectedSubscriptionLabel}>{selectedSubscriptionLabel}</Text>
              <Text className={styles.contextSubtext} title={selectedSubscriptionId}>
                {selectedSubscriptionShortId ? `Sub ${selectedSubscriptionShortId}` : selectedSubscriptionId || "No subscription selected"}
              </Text>
            </div>

            <div className={styles.contextPill}>
              <Text className={styles.contextTitle}>Azure CLI</Text>
              <Text className={styles.contextValue}>{cliLabel}</Text>
              <Text className={styles.contextSubtext}>
                {authContext?.cli.subscriptionName || (selectedSubscriptionShortId ? `Sub ${selectedSubscriptionShortId}` : "No active context")}
              </Text>
            </div>

            <div className={styles.contextPill}>
              <Text className={styles.contextTitle}>Az PowerShell</Text>
              <Text className={styles.contextValue}>{pwshLabel}</Text>
              <Text className={styles.contextSubtext}>
                {authContext?.pwsh.subscriptionName || (selectedSubscriptionShortId ? `Sub ${selectedSubscriptionShortId}` : "No active context")}
              </Text>
            </div>

            <Badge size="small" color={contextReady && contextAligned ? "success" : authContextLoading ? "informative" : "warning"}>
              {authContextLoading ? "Checking context" : contextReady && contextAligned ? "Contexts aligned" : "Context needs attention"}
            </Badge>
          </div>
        </div>
      </div>

      <div className={styles.content}>
        <Outlet />
      </div>

      <div className={styles.footer}>
        <Text size={200}>Medical Device FHIR Integration Platform</Text>
        <span className={styles.iconDivider} />
        <a
          href="https://github.com/kfprugger/med-device-fabric-emulator"
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
