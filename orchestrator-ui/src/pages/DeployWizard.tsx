import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
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
import { RocketRegular, BeakerRegular, AddRegular, DismissRegular, ArrowSyncRegular } from "@fluentui/react-icons";
import { startDeployment, listCapacities, type DeploymentConfig, type FabricCapacity } from "../api";
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
    maxWidth: "720px",
    marginTop: tokens.spacingVerticalL,
  },
  section: {
    marginBottom: tokens.spacingVerticalL,
    transition: "box-shadow 0.2s ease, transform 0.15s ease",
    overflow: "visible",
    ":hover": {
      boxShadow: tokens.shadow8,
      transform: "translateY(-1px)",
    },
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
  });

  const [useNamingConvention, setUseNamingConvention] = useState(true);
  const [useTags, setUseTags] = useState(false);
  const [tagRows, setTagRows] = useState<Array<{ name: string; value: string }>>([
    { name: "", value: "" },
  ]);
  const [namingPrefix, setNamingPrefix] = useState("");

  const update = (field: keyof DeploymentConfig, value: unknown) =>
    setConfig((prev) => ({ ...prev, [field]: value }));

  // When naming prefix changes, auto-derive RG and workspace names
  const handleNamingChange = (prefix: string) => {
    setNamingPrefix(prefix);
    if (useNamingConvention && prefix) {
      setConfig((prev) => ({
        ...prev,
        resource_group_name: `rg-${prefix}`,
        fabric_workspace_name: prefix,
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
      <Title2>New Deployment</Title2>

      <div className={styles.form}>
        {/* Naming Convention */}
        <Card className={styles.section}>
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
        <Card className={styles.section}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Azure Configuration</Subtitle1>}
            description="Target Azure subscription and resource group settings"
          />
          <div className={styles.fieldGroup}>
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
        <Card className={styles.section}>
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
        <Card className={styles.section}>
          <CardHeader
            className={styles.sectionHeader}
            header={<Subtitle1>Data Configuration</Subtitle1>}
            description="Synthetic patient data generation and alerting"
          />
          <div className={styles.fieldGroup}>
            <Field
              label={
                <InfoLabel info="Number of synthetic patients generated by Synthea. More patients = longer FHIR load time. 100 patients \u2248 15 min." infoButton={{ popover: { positioning: "after" } }}>
                  <span className={styles.fieldLabelWithIcon}>
                    <img src="/icon-patient.svg" alt="" width={14} height={14} />
                    <span className={styles.labelSeparator} />
                    Patient Count
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
        <Card className={styles.section}>
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
                label="Skip Base Infrastructure (already deployed)"
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
                label="Skip FHIR / Synthea (data already loaded)"
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
        <div className={styles.actions}>
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
              appearance="secondary"
              icon={<BeakerRegular />}
              onClick={handleMockDeploy}
            >
              Mock Deploy
            </Button>
          </Tooltip>
        </div>

        {error && <div className={styles.error}>{error}</div>}
      </div>
    </div>
  );
}
