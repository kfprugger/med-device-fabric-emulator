import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Checkbox,
  Dropdown,
  Field,
  InfoLabel,
  Input,
  Option,
  SpinButton,
  Subtitle1,
  Text,
  Title2,
  Tooltip,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import { RocketRegular, BeakerRegular, AddRegular, DismissRegular, ArrowSyncRegular, PlayRegular } from "@fluentui/react-icons";
import { startDeployment, listCapacities, checkExistingDeployment, resumeCapacity, type DeploymentConfig, type FabricCapacity, type ExistingDeploymentInfo } from "../api";
import { startMockDeployment, getMockSubscriptions } from "../mockDeployment";
import { useAppState } from "../AppState";
import { MockDataBanner } from "../components/MockDataBanner";
import { HistoryInput } from "../components/HistoryInput";
import { getTagHistory, addTagToHistory } from "../formHistory";

const useStyles = makeStyles({
  form: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalL,
    marginTop: tokens.spacingVerticalL,
  },
  section: {
    marginBottom: "0",
    transition: "box-shadow 0.2s ease",
    overflow: "visible",
    ":hover": {
      boxShadow: tokens.shadow8,
    },
  },
  sectionFullWidth: {
    gridColumn: "1 / -1",
  },
  sectionHeader: {
    cursor: "default",
  },
  fieldGroup: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalM,
    padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalM}`,
    overflow: "visible",
  },
  subscriptionRow: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: tokens.spacingHorizontalM,
    overflow: "visible",
  },
  capacityFieldRow: {
    display: "flex",
    alignItems: "flex-end",
    gap: tokens.spacingHorizontalXS,
  },
  fieldLabelWithIcon: {
    display: "inline-flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
  },
  labelSeparator: {
    width: "1px",
    height: "14px",
    backgroundColor: tokens.colorNeutralStroke2,
    flexShrink: 0,
  },
  actions: {
    display: "flex",
    gap: tokens.spacingHorizontalM,
    marginTop: tokens.spacingVerticalXXL,
  },
  error: {
    color: tokens.colorStatusDangerForeground1,
    fontSize: tokens.fontSizeBase200,
    marginTop: tokens.spacingVerticalS,
  },
  checkboxGroup: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXS,
    padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalM}`,
  },
});

function TagHistoryPanel({ onSelect }: { onSelect: (tags: Record<string, string>) => void }) {
  const [tagHistory, setTagHistory] = useState<Array<Record<string, string>>>([]);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    getTagHistory()
      .then((h) => {
        setTagHistory(h.filter((t) => Object.keys(t).length > 0));
        setLoaded(true);
      })
      .catch(() => setLoaded(true));
  }, []);

  if (!loaded || tagHistory.length === 0) return null;

  return (
    <div style={{
      marginBottom: tokens.spacingVerticalS,
      padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
      backgroundColor: tokens.colorNeutralBackground3,
      borderRadius: tokens.borderRadiusMedium,
      fontSize: tokens.fontSizeBase200,
    }}>
      <Text size={200} weight="semibold" style={{ marginBottom: tokens.spacingVerticalXXS, display: "block" }}>
        Previously used tags:
      </Text>
      <div style={{ display: "flex", flexWrap: "wrap", gap: tokens.spacingHorizontalXS }}>
        {tagHistory.map((tags, i) => {
          const label = Object.entries(tags).map(([k, v]) => `${k}:${v}`).join(", ");
          return (
            <Button
              key={i}
              appearance="subtle"
              size="small"
              onClick={() => onSelect(tags)}
              style={{
                fontSize: tokens.fontSizeBase200,
                padding: `2px ${tokens.spacingHorizontalS}`,
                border: `1px solid ${tokens.colorNeutralStroke2}`,
                borderRadius: tokens.borderRadiusMedium,
              }}
            >
              {label}
            </Button>
          );
        })}
      </div>
    </div>
  );
}

export function DeployWizard() {
  const styles = useStyles();
  const navigate = useNavigate();
  const { selectedSubscription, setSelectedSubscription } = useAppState();
  const [subscriptions, setSubscriptions] = useState(getMockSubscriptions());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [usingMock, setUsingMock] = useState(true);
  const [capacities, setCapacities] = useState<FabricCapacity[]>([]);
  const [selectedCapacity, setSelectedCapacity] = useState<string>("");
  const [pauseAfterDeploy, setPauseAfterDeploy] = useState(false);
  const [capacityRefreshing, setCapacityRefreshing] = useState(false);
  const [resumingCapacity, setResumingCapacity] = useState(false);

  const refreshCapacities = () => {
    if (usingMock || subscriptions.length === 0) return;
    setCapacityRefreshing(true);
    Promise.all(subscriptions.map((s) => listCapacities(s.id)))
      .then((results) => {
        const allCaps = results.flat();
        setCapacities(allCaps);
        // Update selected capacity state if it still exists
        if (selectedCapacity) {
          const updated = allCaps.find((c) => c.name === selectedCapacity);
          if (!updated) setSelectedCapacity("");
        }
      })
      .catch(() => {})
      .finally(() => setCapacityRefreshing(false));
  };

  // Fetch real subscriptions on mount
  useEffect(() => {
    fetch("/api/scan/subscriptions")
      .then((r) => r.json())
      .then((subs: Array<{ id: string; name: string }>) => {
        if (subs.length > 0) {
          setSubscriptions(subs);
          setUsingMock(false);
          if (!selectedSubscription) {
            setSelectedSubscription(subs[0].id);
          }
        }
      })
      .catch(() => { setUsingMock(true); });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch Fabric capacities across all subscriptions
  useEffect(() => {
    if (usingMock || subscriptions.length === 0) return;
    setCapacityRefreshing(true);
    // Scan all subscriptions since capacity may be in a different sub
    Promise.all(subscriptions.map((s) => listCapacities(s.id)))
      .then((results) => {
        const allCaps = results.flat();
        setCapacities(allCaps);
        if (!selectedCapacity) {
          const active = allCaps.find((c) => c.state === "Active");
          if (active) setSelectedCapacity(active.name);
          else if (allCaps.length > 0) setSelectedCapacity(allCaps[0].name);
        }
      })
      .catch(() => setCapacities([]))
      .finally(() => setCapacityRefreshing(false));
  }, [subscriptions, usingMock]); // eslint-disable-line react-hooks/exhaustive-deps

  const [config, setConfig] = useState<DeploymentConfig>({
    resource_group_name: "",
    location: "eastus",
    admin_security_group: "",
    fabric_workspace_name: "",
    patient_count: 100,
    tags: {},
    skip_base_infra: false,
    skip_fhir: false,
    skip_dicom: false,
    skip_fabric: false,
    alert_email: "",
    capacity_subscription_id: "",
    capacity_resource_group: "",
    capacity_name: "",
    pause_capacity_after_deploy: false,
    reuse_patients: false,
  });

  const [useNamingConvention, setUseNamingConvention] = useState(true);
  const [useTags, setUseTags] = useState(false);
  const [tagRows, setTagRows] = useState<Array<{ name: string; value: string }>>([
    { name: "", value: "" },
  ]);
  const [namingPrefix, setNamingPrefix] = useState("");
  const [existingDeploy, setExistingDeploy] = useState<ExistingDeploymentInfo | null>(null);
  const [checkingExisting, setCheckingExisting] = useState(false);
  const [overridePriorSettings, setOverridePriorSettings] = useState(false);

  // Determine which card needs attention next
  const activeCardIndex = useNamingConvention && !namingPrefix ? 0
    : !useNamingConvention && (!config.resource_group_name || !config.fabric_workspace_name) ? 1
    : !selectedCapacity || !config.admin_security_group ? 1
    : !config.fabric_workspace_name && !useNamingConvention ? 2
    : -1; // all filled — no glow

  const update = (field: keyof DeploymentConfig, value: unknown) =>
    setConfig((prev) => ({ ...prev, [field]: value }));

  // Check for existing deployment when workspace/RG names are set
  useEffect(() => {
    const ws = config.fabric_workspace_name;
    const rg = config.resource_group_name;
    if (!ws && !rg) {
      setExistingDeploy(null);
      return;
    }
    const timer = setTimeout(() => {
      setCheckingExisting(true);
      checkExistingDeployment(ws, rg)
        .then((info) => {
          setExistingDeploy(info);
          if (info) {
            update("reuse_patients", true);
            setOverridePriorSettings(false);
            // Auto-populate config fields from prior deployment
            const pc = info.priorConfig;
            if (pc) {
              setConfig((prev) => ({
                ...prev,
                location: pc.location || prev.location,
                admin_security_group: pc.admin_security_group || prev.admin_security_group,
                alert_email: pc.alert_email || prev.alert_email,
                patient_count: pc.patient_count || prev.patient_count,
                reuse_patients: true,
              }));
              // Auto-select capacity if it was used before
              if (pc.capacity_name) {
                setSelectedCapacity(pc.capacity_name);
              }
              // Restore tags
              if (pc.tags && Object.keys(pc.tags).length > 0) {
                setUseTags(true);
                setTagRows(
                  Object.entries(pc.tags).map(([name, value]) => ({ name, value }))
                );
              }
            }
          }
        })
        .catch(() => setExistingDeploy(null))
        .finally(() => setCheckingExisting(false));
    }, 500); // debounce
    return () => clearTimeout(timer);
  }, [config.fabric_workspace_name, config.resource_group_name]); // eslint-disable-line react-hooks/exhaustive-deps

  // When naming prefix changes, auto-derive RG and workspace names
  const handleNamingChange = (prefix: string) => {
    // Azure resource names: max 90 chars, alphanumeric + dashes
    const sanitized = prefix.replace(/[^a-zA-Z0-9-]/g, "").substring(0, 40);
    setNamingPrefix(sanitized);
    if (useNamingConvention && sanitized) {
      setConfig((prev) => ({
        ...prev,
        resource_group_name: `rg-${sanitized}`,
        fabric_workspace_name: sanitized,
      }));
    }
  };

  const handleNamingToggle = (checked: boolean) => {
    setUseNamingConvention(checked);
    if (checked && namingPrefix) {
      setConfig((prev) => ({
        ...prev,
        resource_group_name: `rg-${namingPrefix}`,
        fabric_workspace_name: namingPrefix,
      }));
    }
  };

  // Sync tagRows → config.tags
  const syncTags = (rows: Array<{ name: string; value: string }>) => {
    const parsed: Record<string, string> = {};
    for (const row of rows) {
      if (row.name.trim()) {
        parsed[row.name.trim()] = row.value.trim();
      }
    }
    update("tags", parsed);
  };

  const updateTagRow = (index: number, field: "name" | "value", val: string) => {
    setTagRows((prev) => {
      const next = [...prev];
      next[index] = { ...next[index], [field]: val };
      syncTags(next);
      return next;
    });
  };

  const addTagRow = () => {
    setTagRows((prev) => [...prev, { name: "", value: "" }]);
  };

  const removeTagRow = (index: number) => {
    setTagRows((prev) => {
      const next = prev.filter((_, i) => i !== index);
      if (next.length === 0) next.push({ name: "", value: "" });
      syncTags(next);
      return next;
    });
  };

  const handleSubmit = async () => {
    if (!config.fabric_workspace_name) {
      setError("Fabric workspace name is required.");
      return;
    }
    setLoading(true);
    setError("");

    try {
      // Save tags to history before deploying
      if (Object.keys(config.tags).length > 0) {
        addTagToHistory(config.tags);
      }
      // Inject capacity fields from state
      const cap = capacities.find((c) => c.name === selectedCapacity);
      const deployConfig: DeploymentConfig = {
        ...config,
        capacity_name: selectedCapacity,
        capacity_resource_group: cap?.resourceGroup ?? "",
        capacity_subscription_id: cap?.subscription ?? selectedSubscription,
        pause_capacity_after_deploy: pauseAfterDeploy,
      };
      const { instanceId } = await startDeployment(deployConfig);
      navigate(`/monitor/${instanceId}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  };

  const handleMockDeploy = () => {
    // For mock mode, auto-fill workspace name if empty
    const mockConfig = {
      ...config,
      fabric_workspace_name: config.fabric_workspace_name || "med-device-rti-hds-demo",
    };
    const instanceId = startMockDeployment(mockConfig);
    navigate(`/monitor/${instanceId}`);
  };

  return (
    <div>
      {usingMock && <MockDataBanner />}
      <Title2>Deployment Settings</Title2>

      {/* Responsive grid: 2 columns on wide screens, 1 column on narrow */}
      <style>{`
        .deploy-form-grid {
          display: grid;
          grid-template-columns: 1fr;
          gap: 16px;
          margin-top: 16px;
          transition: grid-template-columns 0.35s ease;
        }
        .deploy-form-grid > * {
          animation: deploy-card-in 0.5s ease both;
          align-self: start;
        }
        .deploy-form-grid > *:nth-child(1) { animation-delay: 0s; }
        .deploy-form-grid > *:nth-child(2) { animation-delay: 0.07s; }
        .deploy-form-grid > *:nth-child(3) { animation-delay: 0.14s; }
        .deploy-form-grid > *:nth-child(4) { animation-delay: 0.21s; }
        .deploy-form-grid > *:nth-child(5) { animation-delay: 0.28s; }
        .deploy-form-grid > *:nth-child(6) { animation-delay: 0.35s; }
        @keyframes deploy-card-in {
          from { opacity: 0; transform: translateY(16px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        .deploy-form-grid .deploy-full-width {
          grid-column: 1 / -1;
        }
        .deploy-card-active {
          outline: 3px solid #0f6cbd !important;
          outline-offset: 4px;
          animation: deploy-card-in 0.5s ease both, deploy-card-pulse 2s ease-in-out 0.6s infinite !important;
        }
        @keyframes deploy-card-pulse {
          0%, 100% { box-shadow: 0 0 16px rgba(15, 108, 189, 0.3); outline-color: #0f6cbd; }
          50%      { box-shadow: 0 0 36px rgba(15, 108, 189, 0.6); outline-color: #78b9eb; }
        }
        @media (min-width: 1200px) {
          .deploy-form-grid {
            grid-template-columns: 1fr 1fr;
            gap: 20px;
          }
        }
        @media (prefers-reduced-motion: reduce) {
          .deploy-form-grid > *,
          .deploy-card-active {
            animation: none !important;
          }
        }
      `}</style>

      <div className={`${styles.form} deploy-form-grid`}>
        {/* Naming Convention */}
        <Card className={`${styles.section}${activeCardIndex === 0 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Naming Convention</Subtitle1>}
            description="Auto-generate consistent names for Azure and Fabric resources"
          />
          <div className={styles.fieldGroup}>
            <Checkbox
              checked={useNamingConvention}
              onChange={(_, d) => handleNamingToggle(!!d.checked)}
              label="Use naming convention (recommended)"
            />
            {useNamingConvention && (
              <>
                <Field
                  label={
                    <InfoLabel info="Enter a short prefix like 'rojo-0404'. The Resource Group will be 'rg-rojo-0404' and the Fabric Workspace will be 'rojo-0404'." infoButton={{ popover: { positioning: "after" } }}>
                      <span className={styles.fieldLabelWithIcon}>
                        <img src="/icon-deployment.svg" alt="" width={16} height={16} />
                        <span className={styles.labelSeparator} />
                        Deployment Name
                      </span>
                    </InfoLabel>
                  }
                  required
                >
                  <HistoryInput
                    field="naming-prefix"
                    value={namingPrefix}
                    onChange={(v) => handleNamingChange(v)}
                    placeholder="e.g. rojo-0404"
                  />
                </Field>
                {namingPrefix && (
                  <div style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: tokens.spacingVerticalXXS,
                    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
                    backgroundColor: tokens.colorNeutralBackground3,
                    borderRadius: tokens.borderRadiusMedium,
                    fontSize: tokens.fontSizeBase200,
                  }}>
                    <Text size={200}>
                      <Text weight="semibold" size={200}>Resource Group:</Text> rg-{namingPrefix}
                    </Text>
                    <Text size={200}>
                      <Text weight="semibold" size={200}>Fabric Workspace:</Text> {namingPrefix}
                    </Text>
                  </div>
                )}
              </>
            )}
          </div>
        </Card>

        {/* Azure Configuration */}
        <Card className={`${styles.section}${activeCardIndex === 1 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Azure Configuration</Subtitle1>}
            description="Target Azure subscription and resource group settings"
          />
          <div className={styles.fieldGroup}>

            {existingDeploy?.priorConfig && (
              <div style={{
                padding: tokens.spacingHorizontalM,
                backgroundColor: tokens.colorNeutralBackground4,
                borderLeft: `4px solid ${tokens.colorBrandStroke1}`,
                borderRadius: tokens.borderRadiusMedium,
                marginBottom: tokens.spacingVerticalS,
              }}>
                <Text size={200} block>
                  <Badge color="informative" size="small" style={{ marginRight: 6 }}>Auto-populated</Badge>
                  Settings restored from prior deployment <strong>{existingDeploy.instanceId}</strong>
                </Text>
                <Checkbox
                  checked={overridePriorSettings}
                  onChange={(_, d) => setOverridePriorSettings(!!d.checked)}
                  label="Override previous settings"
                  style={{ marginTop: tokens.spacingVerticalXS }}
                />
              </div>
            )}
            <div className={styles.subscriptionRow}>
              <Field
                label={
                  <InfoLabel info="Azure subscription where infrastructure resources will be deployed. This selection also applies to the Teardown tab." infoButton={{ popover: { positioning: "after" } }}>
                    <span className={styles.fieldLabelWithIcon}>
                      <img src="/azure_logo.svg" alt="" width={16} height={16} />
                      <span className={styles.labelSeparator} />
                      Azure Subscription
                    </span>
                  </InfoLabel>
                }
              >
                <Dropdown
                  value={subscriptions.find((s) => s.id === selectedSubscription)?.name ?? "Select…"}
                  selectedOptions={[selectedSubscription]}
                  onOptionSelect={(_, data) => setSelectedSubscription(data.optionValue as string)}
                >
                  {subscriptions.map((s) => (
                    <Option key={s.id} value={s.id}>{s.name}</Option>
                  ))}
                </Dropdown>
              </Field>
              <Field
                label={
                  <InfoLabel info="The Fabric capacity backing the workspace. Used to pause billing after deployment. Capacities are scanned across all subscriptions." infoButton={{ popover: { positioning: "after" } }}>
                    <span className={styles.fieldLabelWithIcon}>
                      <img src="/fabric_16_color.svg" alt="" width={16} height={16} />
                      <span className={styles.labelSeparator} />
                      Fabric Capacity
                    </span>
                  </InfoLabel>
                }
              >
                <div className={styles.capacityFieldRow}>
                  <Dropdown
                    style={{ flex: 1 }}
                    value={
                      capacityRefreshing
                        ? "Refreshing capacity status…"
                        : selectedCapacity
                          ? (() => {
                              const cap = capacities.find((c) => c.name === selectedCapacity);
                              return cap ? `${cap.name} — ${cap.sku} (${cap.state ?? "Unknown"})` : selectedCapacity;
                            })()
                          : capacities.length === 0 ? "No capacities found" : "Select…"
                    }
                    selectedOptions={selectedCapacity ? [selectedCapacity] : []}
                    onOptionSelect={(_, data) => setSelectedCapacity(data.optionValue as string)}
                    disabled={capacities.length === 0 || capacityRefreshing}
                  >
                    {capacities.map((c) => (
                      <Option key={c.name} value={c.name} text={`${c.name} — ${c.sku} (${c.state ?? "Unknown"})`}>
                        {c.name} — {c.sku} ({c.state ?? "Unknown"})
                      </Option>
                    ))}
                  </Dropdown>
                  <Tooltip content="Refresh capacity status" relationship="label">
                    <Button
                      appearance="subtle"
                      icon={<ArrowSyncRegular />}
                      size="small"
                      onClick={refreshCapacities}
                      disabled={capacityRefreshing}
                      style={capacityRefreshing ? { animation: "spin 1s linear infinite" } : undefined}
                    />
                  </Tooltip>
                  {(() => {
                    const cap = capacities.find((c) => c.name === selectedCapacity);
                    if (!cap || cap.state === "Active") return null;
                    return (
                      <Tooltip content={`Resume capacity "${cap.name}" (currently ${cap.state})`} relationship="label">
                        <Button
                          appearance="primary"
                          icon={<PlayRegular />}
                          size="small"
                          disabled={resumingCapacity}
                          onClick={async () => {
                            setResumingCapacity(true);
                            setError("");
                            try {
                              await resumeCapacity(cap.subscription, cap.resourceGroup, cap.name);
                              // Poll capacity status until Active (async LRO)
                              const pollInterval = setInterval(() => {
                                refreshCapacities();
                              }, 5000);
                              // Stop polling after 2 min
                              setTimeout(() => clearInterval(pollInterval), 120000);
                            } catch (e) {
                              setError(e instanceof Error ? e.message : "Failed to resume capacity");
                            } finally {
                              setResumingCapacity(false);
                            }
                          }}
                        >
                          {resumingCapacity ? "Starting…" : cap.state === "Resuming" ? "Resuming…" : "Resume"}
                        </Button>
                      </Tooltip>
                    );
                  })()}
                </div>
              </Field>
            </div>
            <Field
              label={
                <InfoLabel info="Azure resource group where Event Hub, ACR, FHIR Service, and ACI containers are deployed." infoButton={{ popover: { positioning: "after" } }}>
                  <span className={styles.fieldLabelWithIcon}>
                    <img src="/icon-resource-group.svg" alt="" width={16} height={16} />
                    <span className={styles.labelSeparator} />
                    Resource Group Name
                  </span>
                </InfoLabel>
              }
            >
              <HistoryInput
                field="resource-group"
                value={config.resource_group_name}
                onChange={(v) => update("resource_group_name", v)}
                disabled={useNamingConvention}
                placeholder={useNamingConvention ? "Set via naming convention above" : "e.g. rg-medtech-rti-fhir"}
              />
            </Field>
            <Field
              label={
                <InfoLabel info="Azure region for all resources. Must support FHIR Service and Event Hubs." infoButton={{ popover: { positioning: "after" } }}>
                  <span className={styles.fieldLabelWithIcon}>
                    <img src="/icon-location.svg" alt="" width={16} height={16} />
                    <span className={styles.labelSeparator} />
                    Location
                  </span>
                </InfoLabel>
              }
            >
              <HistoryInput
                field="location"
                value={config.location}
                onChange={(v) => update("location", v)}
                disabled={!!existingDeploy?.priorConfig && !overridePriorSettings}
              />
            </Field>
            <Field
              label={
                <InfoLabel info="Entra ID security group granted admin access to FHIR Service and Key Vault." infoButton={{ popover: { positioning: "after" } }}>
                  <span className={styles.fieldLabelWithIcon}>
                    <img src="/icon-groups.svg" alt="" width={16} height={16} />
                    <span className={styles.labelSeparator} />
                    Admin Security Group
                  </span>
                </InfoLabel>
              }
            >
              <HistoryInput
                field="admin-security-group"
                value={config.admin_security_group}
                onChange={(v) => update("admin_security_group", v)}
                disabled={!!existingDeploy?.priorConfig && !overridePriorSettings}
              />
            </Field>
            <Checkbox
              checked={useTags}
              onChange={(_, d) => {
                setUseTags(!!d.checked);
                if (!d.checked) {
                  update("tags", {});
                  setTagRows([{ name: "", value: "" }]);
                }
              }}
              label="Add resource tags"
            />
            {useTags && (
              <div>
                <TagHistoryPanel
                  onSelect={(tags) => {
                    const rows = Object.entries(tags).map(([name, value]) => ({ name, value }));
                    if (rows.length === 0) rows.push({ name: "", value: "" });
                    setTagRows(rows);
                    syncTags(rows);
                  }}
                />
                <div style={{
                  display: "grid",
                  gridTemplateColumns: "1fr auto 1fr auto",
                  gap: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
                  alignItems: "center",
                  marginBottom: tokens.spacingVerticalXS,
                }}>
                  <Text weight="semibold" size={200}>Name</Text>
                  <span />
                  <Text weight="semibold" size={200}>Value</Text>
                  <span />
                </div>
                {tagRows.map((row, i) => (
                  <div
                    key={i}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr auto 1fr auto",
                      gap: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
                      alignItems: "center",
                      marginBottom: tokens.spacingVerticalXS,
                    }}
                  >
                    <Input
                      value={row.name}
                      onChange={(_, d) => updateTagRow(i, "name", d.value)}
                      placeholder="e.g. SecurityControl"
                      size="small"
                    />
                    <Text size={300} style={{ color: tokens.colorNeutralForeground3 }}>:</Text>
                    <Input
                      value={row.value}
                      onChange={(_, d) => updateTagRow(i, "value", d.value)}
                      placeholder="e.g. Ignore"
                      size="small"
                    />
                    <Button
                      appearance="subtle"
                      icon={<DismissRegular />}
                      size="small"
                      onClick={() => removeTagRow(i)}
                      disabled={tagRows.length === 1 && !row.name && !row.value}
                    />
                  </div>
                ))}
                <Button
                  appearance="subtle"
                  icon={<AddRegular />}
                  size="small"
                  onClick={addTagRow}
                  style={{ marginTop: tokens.spacingVerticalXS }}
                >
                  Add tag
                </Button>
              </div>
            )}
          </div>
        </Card>

        {/* Fabric Configuration */}
        <Card className={`${styles.section}${activeCardIndex === 2 ? " deploy-card-active" : ""}`}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Fabric Configuration</Subtitle1>}
            description="Microsoft Fabric workspace where RTI, Lakehouses, and Data Agents are deployed"
          />
          <div className={styles.fieldGroup}>
            <Field
              label={
                <InfoLabel info="The Fabric workspace must already exist. Eventhouse, KQL databases, Eventstream, Lakehouses, and Data Agents will be created here." infoButton={{ popover: { positioning: "after" } }}>
                  <span className={styles.fieldLabelWithIcon}>
                    <img src="/fabric_16_color.svg" alt="" width={16} height={16} />
                    <span className={styles.labelSeparator} />
                    Fabric Workspace Name
                  </span>
                </InfoLabel>
              }
              required={!useNamingConvention}
            >
              <HistoryInput
                field="fabric-workspace"
                value={config.fabric_workspace_name}
                onChange={(v) => update("fabric_workspace_name", v)}
                disabled={useNamingConvention}
                placeholder={useNamingConvention ? "Set via naming convention above" : "e.g. med-device-rti-hds"}
              />
            </Field>
            {selectedCapacity && (
              <Checkbox
                checked={pauseAfterDeploy}
                onChange={(_, d) => setPauseAfterDeploy(!!d.checked)}
                label={`Pause capacity "${selectedCapacity}" after successful deployment`}
              />
            )}
          </div>
        </Card>

        {/* Data Configuration */}
        <Card className={`${styles.section} deploy-full-width${activeCardIndex === 3 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Data Configuration</Subtitle1>}
            description="Synthetic patient data generation and alerting"
          />
          <div className={styles.fieldGroup}>

            {/* Existing deployment detection banner */}
            {checkingExisting && (
              <div style={{
                padding: tokens.spacingHorizontalM,
                backgroundColor: tokens.colorNeutralBackground4,
                borderLeft: `4px solid ${tokens.colorBrandStroke1}`,
                borderRadius: tokens.borderRadiusMedium,
                marginBottom: tokens.spacingVerticalM,
                display: "flex",
                alignItems: "center",
                gap: tokens.spacingHorizontalS,
              }}>
                <Badge color="informative" size="small">Checking</Badge>
                <Text size={200}>Querying Azure for existing deployment resources...</Text>
              </div>
            )}

            {existingDeploy && (
              <div style={{
                padding: tokens.spacingHorizontalM,
                backgroundColor: tokens.colorStatusWarningBackground1,
                borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
                borderRadius: tokens.borderRadiusMedium,
                marginBottom: tokens.spacingVerticalM,
              }}>
                <Text weight="semibold" block>
                  Previous deployment detected
                </Text>
                <Text size={200} block style={{ marginTop: 4, color: tokens.colorNeutralForeground2 }}>
                  Workspace <strong>{existingDeploy.workspaceName}</strong> was deployed on{" "}
                  {new Date(existingDeploy.createdTime).toLocaleString()}
                </Text>
                {existingDeploy.azureRgExists && (
                  <>
                  <Text size={200} block style={{ marginTop: 4 }}>
                    FHIR: <strong>{existingDeploy.fhirPatientCount}</strong> patients,{" "}
                    <strong>{existingDeploy.fhirDeviceCount}</strong> Masimo devices
                  </Text>
                  <Tooltip
                    content="FHIR $export writes NDJSON files to ADLS Gen2. HDS pipelines, Bronze Lakehouse shortcuts, and Silver/Gold tables all depend on this data. If 0, the $export has not run yet — it will be triggered automatically on deploy."
                    relationship="description"
                    positioning="above"
                  >
                    <Text size={200} block style={{
                      marginTop: 4,
                      padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalS}`,
                      borderRadius: tokens.borderRadiusMedium,
                      backgroundColor: (existingDeploy.exportedFiles ?? 0) === 0
                        ? tokens.colorStatusDangerBackground1
                        : "transparent",
                      cursor: "help",
                    }}>
                      {(existingDeploy.exportedFiles ?? 0) === 0 && (
                        <Badge color="danger" size="small" style={{ marginRight: 6 }}>Critical</Badge>
                      )}
                      Storage: <strong>{existingDeploy.exportedFiles ?? 0}</strong> exported FHIR files,{" "}
                      <strong>{existingDeploy.dicomStudies ?? 0}</strong> DICOM imaging blobs
                      {(existingDeploy.exportedFiles ?? 0) === 0 && (
                        <span style={{ color: tokens.colorStatusDangerForeground1, marginLeft: 6 }}>
                          — $export required for HDS pipelines
                        </span>
                      )}
                    </Text>
                  </Tooltip>
                  <Text size={200} block style={{ marginTop: 2 }}>
                    {existingDeploy.emulatorRunning ? (
                      <>Emulator: <strong style={{ color: tokens.colorPaletteGreenForeground1 }}>running</strong> ({existingDeploy.emulatorDeviceCount ?? 100} devices streaming telemetry)</>
                    ) : (
                      <>Emulator: <strong style={{ color: tokens.colorStatusDangerForeground1 }}>stopped</strong></>
                    )}
                  </Text>
                  </>
                )}
                <div style={{ marginTop: tokens.spacingVerticalS, display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS }}>
                  <Checkbox
                    checked={config.reuse_patients}
                    onChange={(_, d) => update("reuse_patients", !!d.checked)}
                    label={`Reuse existing ${existingDeploy.fhirPatientCount} patients and ${existingDeploy.fhirDeviceCount} devices`}
                  />
                  <Text size={200} style={{ color: tokens.colorNeutralForeground3, paddingLeft: 28 }}>
                    {config.reuse_patients
                      ? "Synthea generation, FHIR Loader, and DICOM Loader will be skipped. Emulator stays running."
                      : `New batch of ${config.patient_count} patients will be generated with ${config.patient_count} new device associations. Existing data will be cleared and replaced.`}
                  </Text>
                </div>
              </div>
            )}

            <Field
              label={
                <InfoLabel info="Number of synthetic patients generated by Synthea. More patients = longer FHIR load time. 100 patients ≈ 15 min." infoButton={{ popover: { positioning: "after" } }}>
                  <span className={styles.fieldLabelWithIcon}>
                    <img src="/icon-patient.svg" alt="" width={14} height={14} />
                    <span className={styles.labelSeparator} />
                    Patient Count{config.reuse_patients ? " (ignored — reusing existing)" : existingDeploy ? " (new batch)" : " (to be generated)"}
                  </span>
                </InfoLabel>
              }
            >
              <SpinButton
                value={config.patient_count}
                min={10}
                max={10000}
                step={10}
                onChange={(_, d) => update("patient_count", d.value ?? 100)}
                disabled={config.reuse_patients}
              />
            </Field>
            <Field
              label={
                <InfoLabel info="Email address for clinical alert notifications via Data Activator (Reflex). Leave blank to skip." infoButton={{ popover: { positioning: "after" } }}>
                  Alert Email (optional)
                </InfoLabel>
              }
            >
              <HistoryInput
                field="alert-email"
                value={config.alert_email}
                onChange={(v) => update("alert_email", v)}
                placeholder="joey@example.com"
                type="email"
              />
            </Field>
          </div>
        </Card>

        {/* Phase Control */}
        <Card className={`${styles.section} deploy-full-width`}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Phase Control</Subtitle1>}
            description={
              <span className={styles.fieldLabelWithIcon}>
                <img src="/icon-phases.svg" alt="" width={14} height={14} />
                <span className={styles.labelSeparator} />
                Skip phases that are already deployed to resume from a checkpoint
              </span>
            }
          />
          <div className={styles.checkboxGroup}>
            <Tooltip
              content="Skip Event Hub, ACR, emulator ACI, and Bicep infra deployment"
              relationship="description"
              positioning="after"
            >
              <Checkbox
                label={`Skip Base Infrastructure${existingDeploy ? " (already deployed)" : ""}`}
                checked={config.skip_base_infra}
                onChange={(_, d) => update("skip_base_infra", d.checked)}
              />
            </Tooltip>
            <Tooltip
              content="Skip FHIR Service, Synthea patient generation, and FHIR data loading"
              relationship="description"
              positioning="after"
            >
              <Checkbox
                label={`Skip FHIR / Synthea${existingDeploy ? " (data already loaded)" : ""}`}
                checked={config.skip_fhir}
                onChange={(_, d) => update("skip_fhir", d.checked)}
              />
            </Tooltip>
            <Tooltip
              content="Skip DICOM infrastructure, TCIA download, and imaging study upload"
              relationship="description"
              positioning="after"
            >
              <Checkbox
                label="Skip DICOM"
                checked={config.skip_dicom}
                onChange={(_, d) => update("skip_dicom", d.checked)}
              />
            </Tooltip>
            <Tooltip
              content="Skip Eventhouse, KQL Database, Eventstream, and dashboard creation"
              relationship="description"
              positioning="after"
            >
              <Checkbox
                label="Skip Fabric RTI"
                checked={config.skip_fabric}
                onChange={(_, d) => update("skip_fabric", d.checked)}
              />
            </Tooltip>
          </div>
        </Card>

        {/* Actions */}
        <div className={`${styles.actions} deploy-full-width`}>
          <Tooltip content="Launch the full deployment pipeline" relationship="description">
            <Button
              appearance="primary"
              icon={<RocketRegular />}
              onClick={handleSubmit}
              disabled={loading}
            >
              {loading ? "Starting…" : "Start Deployment"}
            </Button>
          </Tooltip>
          <Tooltip content="Run a simulated deployment to preview the UI (no Azure/Fabric resources created)" relationship="description">
            <Button
              appearance="outline"
              icon={<BeakerRegular />}
              onClick={handleMockDeploy}
              size="small"
            >
              Mock Deploy
            </Button>
          </Tooltip>
        </div>

        {error && <div className={styles.error} ref={(el) => el?.scrollIntoView({ behavior: "smooth" })}>{error}</div>}
      </div>
    </div>
  );
}
