import { useMemo, useState } from "react";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  MessageBar,
  MessageBarBody,
  Subtitle1,
  Text,
  Title2,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import {
  ArrowSyncRegular,
  CheckmarkCircleRegular,
  ClipboardRegular,
  DismissCircleRegular,
  OpenRegular,
  WarningRegular,
} from "@fluentui/react-icons";
import { useAppState } from "../AppState";

const useStyles = makeStyles({
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: tokens.spacingHorizontalL,
    marginBottom: tokens.spacingVerticalL,
  },
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
    gap: tokens.spacingHorizontalL,
  },
  cardBody: {
    padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalL}`,
    display: "grid",
    gap: tokens.spacingVerticalS,
  },
  kv: {
    display: "grid",
    gridTemplateColumns: "112px 1fr",
    gap: tokens.spacingHorizontalS,
    fontSize: tokens.fontSizeBase200,
  },
  label: {
    color: tokens.colorNeutralForeground3,
    fontWeight: tokens.fontWeightSemibold,
  },
  value: {
    overflowWrap: "anywhere",
    fontFamily: "'Cascadia Code', 'Consolas', monospace",
  },
  command: {
    padding: tokens.spacingHorizontalM,
    borderRadius: tokens.borderRadiusMedium,
    backgroundColor: tokens.colorNeutralBackground3,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    fontFamily: "'Cascadia Code', 'Consolas', monospace",
    fontSize: tokens.fontSizeBase200,
    whiteSpace: "pre-wrap",
  },
  actionRow: {
    display: "flex",
    gap: tokens.spacingHorizontalS,
    flexWrap: "wrap",
    alignItems: "center",
  },
});

const PREFLIGHT_ANIMATION_CSS = `
@keyframes preflight-pulse {
  0% { opacity: 0.65; transform: scale(0.98); }
  50% { opacity: 1; transform: scale(1.02); }
  100% { opacity: 0.65; transform: scale(0.98); }
}
@keyframes preflight-spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
.preflight-loading-icon {
  animation: preflight-spin 1.4s linear infinite !important;
}
.preflight-card-pulse {
  animation: preflight-pulse 1.8s ease-in-out infinite !important;
}
`;

function statusBadge(ok: boolean, pending = false) {
  if (pending) {
    return (
      <Badge
        color="informative"
        icon={<ArrowSyncRegular className="preflight-loading-icon" />}
        className="preflight-card-pulse"
        size="large"
        style={{ padding: "6px 12px", fontSize: tokens.fontSizeBase300 }}
      >
        Validating context...
      </Badge>
    );
  }
  return ok
    ? <Badge color="success" icon={<CheckmarkCircleRegular />} size="large" style={{ padding: "6px 12px", fontSize: tokens.fontSizeBase300 }}>Ready for Deployment</Badge>
    : <Badge color="danger" icon={<DismissCircleRegular />} size="large" style={{ padding: "6px 12px", fontSize: tokens.fontSizeBase300 }}>Needs attention</Badge>;
}

function checkBadge(ok: boolean, pending = false) {
  if (pending) {
    return (
      <Badge
        color="informative"
        icon={<ArrowSyncRegular className="preflight-loading-icon" />}
        className="preflight-card-pulse"
      >
        Checking
      </Badge>
    );
  }
  return ok
    ? <Badge color="success" icon={<CheckmarkCircleRegular />}>Pass</Badge>
    : <Badge color="warning" icon={<WarningRegular />}>Fix</Badge>;
}

function copy(text: string) {
  navigator.clipboard?.writeText(text).catch(() => undefined);
}

export function Preflight() {
  const styles = useStyles();
  const {
    authContext,
    authContextLoading,
    refreshAuthContext,
    subscriptions,
    capacities,
    selectedSubscription,
  } = useAppState();
  const [refreshing, setRefreshing] = useState(false);

  const cliOk = !!authContext?.cli.installed && !!authContext?.cli.loggedIn;
  const pwshOk = !!authContext?.pwsh.installed && !!authContext?.pwsh.loggedIn;
  const aligned = !!authContext?.aligned.subscription && !!authContext?.aligned.tenant;
  const targetSubscriptionId = selectedSubscription || authContext?.cli.subscriptionId || authContext?.pwsh.subscriptionId || "<subscription-id>";
  const targetTenantId = authContext?.cli.tenantId || authContext?.pwsh.tenantId || "<tenant-id>";
  const targetContextReady = cliOk && pwshOk && aligned && !!selectedSubscription;
  const allReady = !!authContext?.ready && aligned && !!selectedSubscription;

  const selectedSubName = subscriptions.find((s) => s.id === selectedSubscription)?.name || authContext?.cli.subscriptionName || "Not selected";
  const readinessChecks = useMemo(() => [
    { label: "Azure CLI installed and logged in", ok: cliOk, detail: authContext?.cli.error || authContext?.cli.user || "" },
    { label: "Az PowerShell installed and logged in", ok: pwshOk, detail: authContext?.pwsh.error || authContext?.pwsh.user || "" },
    { label: "CLI and PowerShell tenant aligned", ok: aligned, detail: authContext?.cli.tenantId || authContext?.pwsh.tenantId || "" },
    { label: "Target subscription selected", ok: !!selectedSubscription, detail: selectedSubName },
    { label: "Selected context ready for deployment", ok: targetContextReady, detail: targetSubscriptionId },
    { label: "Subscriptions loaded", ok: subscriptions.length > 0, detail: `${subscriptions.length} subscription(s)` },
    { label: "Fabric capacities discoverable", ok: capacities.length > 0, detail: `${capacities.length} capacity candidate(s)` },
  ], [aligned, authContext, capacities.length, cliOk, pwshOk, selectedSubName, selectedSubscription, subscriptions.length, targetContextReady, targetSubscriptionId]);

  const isolationCommand = `# Run from the repository root. Optional: isolate Azure CLI state for this project.\ncd /path/to/med-device-fabric-emulator\nexport AZURE_CONFIG_DIR="$PWD/.pi-run/azure-profile"\nmkdir -p "$AZURE_CONFIG_DIR"\n\n# Sign in or reuse cached credentials for your tenant/subscription.\naz login --tenant ${targetTenantId}\naz account set --subscription ${targetSubscriptionId}\n\n# Align Az PowerShell to the same tenant/subscription used by Azure CLI.\npwsh -NoProfile -Command 'Connect-AzAccount -Tenant ${targetTenantId}; Set-AzContext -Tenant ${targetTenantId} -Subscription ${targetSubscriptionId}'`;

  const onRefresh = async () => {
    setRefreshing(true);
    try { await refreshAuthContext(); }
    finally { setRefreshing(false); }
  };

  return (
    <div>
      <style>{PREFLIGHT_ANIMATION_CSS}</style>
      <div className={styles.header}>
        <div>
          <Title2>Deployment Preflight</Title2>
          <Text block style={{ color: tokens.colorNeutralForeground2, marginTop: tokens.spacingVerticalXS }}>
            Validate Azure CLI, Az PowerShell, tenant/subscription alignment, and Fabric discovery before starting a deployment.
          </Text>
        </div>
        <div className={styles.actionRow}>
          {statusBadge(allReady, authContextLoading || refreshing)}
          <Button icon={<ArrowSyncRegular />} onClick={onRefresh} disabled={authContextLoading || refreshing}>
            Refresh context
          </Button>
        </div>
      </div>

      {!allReady && (
        <MessageBar intent="warning" style={{ marginBottom: tokens.spacingVerticalL }}>
          <MessageBarBody>
            Preflight found items to fix before a real deployment. Use the remediation command below from an isolated terminal.
          </MessageBarBody>
        </MessageBar>
      )}

      <div className={styles.grid}>
        <Card>
          <CardHeader header={<Subtitle1>Azure context</Subtitle1>} action={statusBadge(cliOk && pwshOk && aligned, authContextLoading || refreshing)} />
          <div className={styles.cardBody}>
            <div className={styles.kv}><span className={styles.label}>CLI user</span><span className={styles.value}>{authContext?.cli.user || "Not logged in"}</span></div>
            <div className={styles.kv}><span className={styles.label}>Pwsh user</span><span className={styles.value}>{authContext?.pwsh.user || "Not logged in"}</span></div>
            <div className={styles.kv}><span className={styles.label}>Tenant</span><span className={styles.value}>{authContext?.cli.tenantId || authContext?.pwsh.tenantId || "Unknown"}</span></div>
            <div className={styles.kv}><span className={styles.label}>Subscription</span><span className={styles.value}>{selectedSubName}</span></div>
          </div>
        </Card>

        <Card>
          <CardHeader header={<Subtitle1>Readiness checklist</Subtitle1>} />
          <div className={styles.cardBody}>
            {readinessChecks.map((check) => (
              <div key={check.label} className={styles.actionRow} style={{ justifyContent: "space-between" }}>
                <div>
                  <Text weight="semibold" size={200} block>{check.label}</Text>
                  <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>{check.detail || "—"}</Text>
                </div>
                {checkBadge(check.ok, authContextLoading || refreshing)}
              </div>
            ))}
          </div>
        </Card>

        <Card>
          <CardHeader header={<Subtitle1>Operator links</Subtitle1>} />
          <div className={styles.cardBody}>
            <Button as="a" href="https://portal.azure.com/#view/Microsoft_Azure_Billing/SubscriptionsBlade" target="_blank" icon={<OpenRegular />}>Azure subscriptions</Button>
            <Button as="a" href="https://app.fabric.microsoft.com/home?experience=fabric-developer" target="_blank" icon={<OpenRegular />}>Fabric portal</Button>
            <Button as="a" href="https://learn.microsoft.com/en-us/powershell/azure/authenticate-azureps" target="_blank" icon={<OpenRegular />}>Az PowerShell auth docs</Button>
          </div>
        </Card>
      </div>

      <Card style={{ marginTop: tokens.spacingVerticalL }}>
        <CardHeader
          header={<Subtitle1>Isolated Azure terminal command</Subtitle1>}
          action={<Button icon={<ClipboardRegular />} onClick={() => copy(isolationCommand)}>Copy</Button>}
        />
        <div className={styles.cardBody}>
          <div className={styles.command}>{isolationCommand}</div>
          <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
            This optional command uses a project-local Azure CLI profile under <code>.pi-run/azure-profile</code> and aligns Az PowerShell to the same tenant/subscription. Replace placeholders if the context above is not loaded yet.
          </Text>
        </div>
      </Card>
    </div>
  );
}
