import { useState, useEffect, useMemo } from "react";
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
import { RocketRegular, BeakerRegular, AddRegular, DismissRegular, ArrowSyncRegular, PlayRegular, ChevronDownRegular, ChevronUpRegular, CheckmarkCircleRegular, CircleRegular, SettingsRegular } from "@fluentui/react-icons";
import { startDeployment, listCapacities, checkExistingDeployment, resumeCapacity, listAhdsRegions, type DeploymentConfig, type FabricCapacity, type ExistingDeploymentInfo } from "../api";
import { startMockDeployment, getMockSubscriptions, getMockCapacities } from "../mockDeployment";
import { useAppState } from "../AppState";
import { MockDataBanner } from "../components/MockDataBanner";
import { HistoryInput } from "../components/HistoryInput";
import { getTagHistory, addTagToHistory } from "../formHistory";
import { useReducedMotion } from "../hooks/useReducedMotion";

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
    alignItems: "center",
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
  stickyHeader: {
    position: "sticky",
    top: 0,
    zIndex: 10,
    backgroundColor: tokens.colorNeutralBackground1,
    paddingBottom: tokens.spacingVerticalXS,
  },
  summarySidebar: {
    position: "sticky",
    top: tokens.spacingVerticalXXL,
    height: "fit-content",
    maxHeight: "calc(100vh - 100px)",
    overflowY: "auto",
  },
  compactField: {
    "@media (min-width: 1200px)": {
      padding: `0 ${tokens.spacingHorizontalM} ${tokens.spacingVerticalS}`,
    },
  },
  cardRequired: {
    borderLeft: `3px solid ${tokens.colorBrandStroke1}`,
  },
  cardOptional: {
    borderLeft: `3px solid ${tokens.colorNeutralStroke2}`,
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
  const reducedMotion = useReducedMotion();
  const navigate = useNavigate();
  const { selectedSubscription, setSelectedSubscription, subscriptions: ctxSubscriptions, capacities: ctxCapacities } = useAppState();
  const [subscriptions, setSubscriptions] = useState(getMockSubscriptions());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [usingMock, setUsingMock] = useState(true);
  const [capacities, setCapacities] = useState<FabricCapacity[]>([]);
  const [selectedCapacity, setSelectedCapacity] = useState<string>("");
  const [pauseAfterDeploy, setPauseAfterDeploy] = useState(false);
  const [capacityRefreshing, setCapacityRefreshing] = useState(false);
  const [resumingCapacity, setResumingCapacity] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [existingDeployCollapsed, setExistingDeployCollapsed] = useState(false);
  const [showSummary, setShowSummary] = useState(true);
  const [initializing, setInitializing] = useState(true);
  const [loadWarning, setLoadWarning] = useState("");
  const [ahdsRegions, setAhdsRegions] = useState<string[] | null>(null); // null = not loaded yet

  const refreshCapacities = () => {
    if (subscriptions.length === 0) return;
    if (usingMock) {
      // In mock mode, simulate resume completing after a few refreshes
      setCapacities((prev) => prev.map((c) =>
        c.state === "Resuming" ? { ...c, state: "Active" } : c
      ));
      return;
    }
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
      .catch(() => {
        setError("Failed to refresh capacity state. Try again.");
      })
      .finally(() => setCapacityRefreshing(false));
  };

  // Fetch real subscriptions on mount — prefer context prefetch if available
  useEffect(() => {
    if (ctxSubscriptions.length > 0) {
      setSubscriptions(ctxSubscriptions);
      setUsingMock(false);
      if (!selectedSubscription) setSelectedSubscription(ctxSubscriptions[0].id);
      return;
    }
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
      .catch(() => {
        setUsingMock(true);
        setLoadWarning("Live Azure subscription scan unavailable. Using mock data.");
      });
  }, [ctxSubscriptions]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch Fabric capacities across all subscriptions
  useEffect(() => {
    if (subscriptions.length === 0) return;
    if (usingMock) {
      // Use mock capacities in mock mode
      const mockCaps = getMockCapacities() as FabricCapacity[];
      setCapacities(mockCaps);
      const active = mockCaps.find((c) => c.state === "Active");
      if (active && !selectedCapacity) setSelectedCapacity(active.name);
      setInitializing(false);
      return;
    }
    // Seed from the app-wide prefetch if available, then refresh in background
    if (ctxCapacities.length > 0 && capacities.length === 0) {
      setCapacities(ctxCapacities);
      if (!selectedCapacity) {
        const active = ctxCapacities.find((c) => c.state === "Active");
        if (active) setSelectedCapacity(active.name);
        else setSelectedCapacity(ctxCapacities[0].name);
      }
      setInitializing(false);
    }
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
      .catch(() => {
        setCapacities([]);
        setLoadWarning("Unable to load Fabric capacities right now.");
      })
      .finally(() => {
        setCapacityRefreshing(false);
        setInitializing(false);
      });
  }, [subscriptions, usingMock]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch AHDS-supported regions once on mount (independent of mock mode).
  // Falls back to a known-good list when the backend is unreachable so that
  // validation still works in mock / offline mode.
  const AHDS_FALLBACK_REGIONS = [
    "australiaeast", "canadacentral", "eastus", "eastus2",
    "northcentralus", "northeurope", "southcentralus",
    "southeastasia", "uksouth", "westeurope", "westus2", "westus3",
  ];
  useEffect(() => {
    listAhdsRegions().then((regions) => {
      setAhdsRegions(regions.length > 0 ? regions : AHDS_FALLBACK_REGIONS);
    });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

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
    // Granular component toggles
    skip_synthea: false,
    skip_device_assoc: false,
    skip_fhir_export: false,
    skip_rti_phase2: false,
    skip_hds_pipelines: false,
    skip_data_agents: false,
    skip_imaging: false,
    skip_ontology: false,
    skip_activator: false,
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

  // Calculate completion status for cards
  const getCardCompletion = (cardIndex: number): { complete: number; total: number } => {
    switch (cardIndex) {
      case 0: // Naming
        return { complete: (!useNamingConvention || !!namingPrefix) ? 1 : 0, total: 1 };
      case 1: // Azure Config
        const azureFields = [selectedSubscription, selectedCapacity, config.admin_security_group];
        if (!useNamingConvention) azureFields.push(config.resource_group_name);
        return { complete: azureFields.filter(Boolean).length, total: azureFields.length };
      case 2: // Fabric Config
        return { complete: config.fabric_workspace_name ? 1 : 0, total: 1 };
      case 3: // Data Config
        return { complete: [config.patient_count, config.alert_email?.trim()].filter(Boolean).length, total: 2 };
      default:
        return { complete: 0, total: 0 };
    }
  };

  // Calculate estimated duration
  const getEstimatedDuration = (): string => {
    let minutes = 5; // Base infrastructure
    if (!config.skip_fhir && !config.skip_synthea) minutes += Math.ceil(config.patient_count / 10);
    if (!config.skip_dicom) minutes += 20;
    if (!config.skip_fabric) minutes += 15;
    if (!config.skip_hds_pipelines) minutes += 10;
    if (!config.skip_data_agents) minutes += 5;
    if (!config.skip_imaging) minutes += 5;
    return minutes >= 60 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m` : `${minutes}m`;
  };

  const update = (field: keyof DeploymentConfig, value: unknown) => {
    setConfig((prev) => {
      const next = { ...prev, [field]: value };

      // ── Dependency auto-toggle rules ──
      // When a component is disabled, auto-disable its dependents.
      // When re-enabled, dependents stay as-is (user re-enables manually).

      // skip_fhir → forces skip_synthea, skip_device_assoc, skip_fhir_export
      if (field === "skip_fhir" && value) {
        next.skip_synthea = true;
        next.skip_device_assoc = true;
        next.skip_fhir_export = true;
      }
      // skip_synthea → forces skip_device_assoc (no patients = no devices)
      if (field === "skip_synthea" && value) {
        next.skip_device_assoc = true;
      }
      // skip_dicom → forces skip_hds_pipelines, skip_imaging
      if (field === "skip_dicom" && value) {
        next.skip_hds_pipelines = true;
        next.skip_imaging = true;
      }
      // skip_fabric (RTI) → forces skip_fhir_export, skip_rti_phase2, skip_activator
      if (field === "skip_fabric" && value) {
        next.skip_fhir_export = true;
        next.skip_rti_phase2 = true;
        next.skip_activator = true;
      }
      // skip_base_infra → forces skip_synthea, skip_fhir, skip_dicom, skip_device_assoc
      if (field === "skip_base_infra" && value) {
        next.skip_fhir = true;
        next.skip_synthea = true;
        next.skip_device_assoc = true;
        next.skip_dicom = true;
        next.skip_fhir_export = true;
        next.skip_hds_pipelines = true;
        next.skip_imaging = true;
      }

      // Re-enabling a parent → unblock children (restore to not-skipped)
      if (field === "skip_fhir" && !value) {
        next.skip_synthea = false;
        next.skip_device_assoc = false;
        next.skip_fhir_export = false;
      }
      if (field === "skip_synthea" && !value) {
        next.skip_device_assoc = false;
      }
      if (field === "skip_dicom" && !value) {
        next.skip_hds_pipelines = false;
        next.skip_imaging = false;
      }
      if (field === "skip_fabric" && !value) {
        next.skip_fhir_export = false;
        next.skip_rti_phase2 = false;
        next.skip_activator = false;
      }
      if (field === "skip_base_infra" && !value) {
        next.skip_fhir = false;
        next.skip_synthea = false;
        next.skip_device_assoc = false;
        next.skip_dicom = false;
        next.skip_fhir_export = false;
        next.skip_hds_pipelines = false;
        next.skip_imaging = false;
      }

      return next;
    });
  };

  // Check for existing deployment when workspace/RG names are set
  useEffect(() => {
    const ws = config.fabric_workspace_name;
    const rg = config.resource_group_name;
    if (!ws && !rg) {
      setExistingDeploy(null);
      return;
    }
    const abortController = new AbortController();
    const timer = setTimeout(() => {
      setCheckingExisting(true);
      checkExistingDeployment(ws, rg, abortController.signal)
        .then((info) => {
          if (abortController.signal.aborted) return;
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
        .catch((err) => {
          if (!abortController.signal.aborted) setExistingDeploy(null);
        })
        .finally(() => {
          if (!abortController.signal.aborted) setCheckingExisting(false);
        });
    }, 500); // debounce
    return () => {
      clearTimeout(timer);
      abortController.abort();
    };
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
    if (validationErrors.length > 0) {
      setError(validationErrors[0]);
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

  const locationUnsupported = ahdsRegions !== null &&
    !config.skip_fhir &&
    !ahdsRegions.includes(config.location.replace(/\s/g, "").toLowerCase());

  const validationErrors = useMemo(() => {
    const issues: string[] = [];
    if (!selectedSubscription) issues.push("Select an Azure subscription.");
    if (!selectedCapacity) issues.push("Select a Fabric capacity.");
    if (!config.admin_security_group?.trim()) issues.push("Admin Security Group is required.");
    if (useNamingConvention && !namingPrefix.trim()) issues.push("Deployment Name is required when naming convention is enabled.");
    if (!useNamingConvention && !config.resource_group_name?.trim()) issues.push("Resource Group Name is required.");
    if (!config.fabric_workspace_name?.trim()) issues.push("Fabric workspace name is required.");
    if (!config.alert_email?.trim()) issues.push("Alert email is required.");
    if (!config.patient_count || config.patient_count < 1) issues.push("Patient count must be at least 1.");
    if (locationUnsupported) issues.push(`Not an acceptable region. Please select a supported AHDS region.`);
    return issues;
  }, [
    selectedSubscription,
    selectedCapacity,
    config.admin_security_group,
    config.resource_group_name,
    config.fabric_workspace_name,
    config.alert_email,
    config.patient_count,
    config.location,
    config.skip_fhir,
    useNamingConvention,
    namingPrefix,
    locationUnsupported,
  ]);

  const canStartDeployment = validationErrors.length === 0 && !loading;

  return (
    <div style={{ display: "flex", gap: "24px", position: "relative" }}>
      {/* Main content area */}
      <div style={{ flex: 1, minWidth: 0 }}>
        {usingMock && <MockDataBanner />}
        {loadWarning && (
          <div style={{
            marginBottom: tokens.spacingVerticalS,
            padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
            backgroundColor: tokens.colorStatusWarningBackground1,
            borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
            borderRadius: tokens.borderRadiusMedium,
          }}>
            <Text size={200}>{loadWarning}</Text>
          </div>
        )}
        <div className={styles.stickyHeader}>
          <Title2>Deployment Settings</Title2>
        </div>

        {/* Responsive grid: auto-flow dense packing eliminates wasted space */}
        <style>{`
        .deploy-form-grid {
          display: grid;
          grid-template-columns: 1fr;
          gap: 16px;
          margin-top: 16px;
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
        .deploy-card-active {
          outline: 3px solid #0f6cbd !important;
          outline-offset: 4px;
          animation: deploy-card-in 0.5s ease both, deploy-card-pulse 2s ease-in-out 0.6s infinite !important;
        }
        @keyframes deploy-card-pulse {
          0%, 100% { box-shadow: 0 0 16px rgba(15, 108, 189, 0.3); outline-color: #0f6cbd; }
          50%      { box-shadow: 0 0 36px rgba(15, 108, 189, 0.6); outline-color: #78b9eb; }
        }
        /* Masonry layout on wide screens to avoid empty holes between variable-height cards */
        @media (min-width: 1200px) {
          .deploy-form-grid {
            display: block;
            column-count: 2;
            column-gap: 20px;
          }
          .deploy-form-grid > * {
            display: inline-block;
            width: 100%;
            margin-bottom: 20px;
            break-inside: avoid;
            -webkit-column-break-inside: avoid;
          }
        }
        /* 3-column layout for extra-wide screens to maximize space usage */
        @media (min-width: 1800px) {
          .deploy-form-grid {
            column-count: 3;
          }
        }
        /* Compact padding on wide screens */
        @media (min-width: 1400px) {
          .deploy-compact-padding .fui-CardHeader {
            padding: 12px 16px;
          }
          .deploy-compact-padding .deploy-field-group {
            padding: 0 16px 12px;
          }
        }
        /* Tall cards span 2 rows for better visual hierarchy */
        .deploy-card-tall {
          grid-row: span 2;
        }
        /* Sticky card headers */
        .deploy-card-sticky-header .fui-CardHeader {
          position: sticky;
          top: 0;
          z-index: 5;
          background-color: inherit;
          border-bottom: 1px solid #e0e0e0;
        }
        /* Collapsible section animation */
        .deploy-collapsible-content {
          overflow: hidden;
          transition: max-height 0.3s ease, opacity 0.3s ease;
        }
        .deploy-collapsible-collapsed {
          max-height: 0 !important;
          opacity: 0;
        }
        /* Advanced options toggle */
        .deploy-advanced-toggle {
          margin: 12px 0;
          padding: 10px 16px;
          background: ${tokens.colorNeutralBackground3};
          border: 1px solid ${tokens.colorNeutralStroke2};
          border-radius: 6px;
          cursor: pointer;
          display: flex;
          align-items: center;
          justify-content: space-between;
          transition: all 0.2s;
          color: ${tokens.colorNeutralForeground1};
        }
        .deploy-advanced-toggle:hover {
          background: ${tokens.colorNeutralBackground4};
          border-color: ${tokens.colorBrandStroke1};
        }
        .deploy-advanced-toggle svg {
          color: ${tokens.colorNeutralForeground2};
        }
        ${reducedMotion ? `
        .deploy-form-grid > *,
        .deploy-card-active,
        .deploy-collapsible-content,
        .deploy-advanced-toggle {
          animation: none !important;
          transition: none !important;
        }
        ` : ""}
        @media (prefers-reduced-motion: reduce) {
          .deploy-form-grid > *,
          .deploy-card-active,
          .deploy-collapsible-content {
            animation: none !important;
            transition: none !important;
          }
        }
      `}</style>

      {initializing ? (
        <div className={`${styles.form} deploy-form-grid`}>
          <Card className={styles.section}><CardHeader header={<Subtitle1>Loading deployment configuration...</Subtitle1>} /></Card>
          <Card className={styles.section}><CardHeader header={<Subtitle1>Loading capacities and subscriptions...</Subtitle1>} /></Card>
          <Card className={styles.section}><CardHeader header={<Subtitle1>Preparing defaults...</Subtitle1>} /></Card>
        </div>
      ) : (

      <div className={`${styles.form} deploy-form-grid deploy-compact-padding`}>
        {/* Naming Convention */}
        <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 0 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
          <CardHeader
            className={styles.sectionHeader}
            header={
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Subtitle1>Naming Convention</Subtitle1>
                {(() => {
                  const { complete, total } = getCardCompletion(0);
                  return complete === total ? (
                    <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                  ) : (
                    <Badge color="informative" size="small" icon={<CircleRegular />}>{complete}/{total}</Badge>
                  );
                })()}
              </div>
            }
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
        <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 1 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
          <CardHeader
            className={styles.sectionHeader}
            header={
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Subtitle1>Azure Configuration</Subtitle1>
                {(() => {
                  const { complete, total } = getCardCompletion(1);
                  return complete === total ? (
                    <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                  ) : (
                    <Badge color="informative" size="small" icon={<CircleRegular />}>{complete}/{total}</Badge>
                  );
                })()}
              </div>
            }
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
                <div
                  onClick={() => setExistingDeployCollapsed(!existingDeployCollapsed)}
                  style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}
                >
                  <Text size={200} weight="semibold">
                    <Badge color="informative" size="small" style={{ marginRight: 6 }}>Auto-populated</Badge>
                    Settings from prior deployment
                  </Text>
                  {existingDeployCollapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                </div>
                <div className={`deploy-collapsible-content${existingDeployCollapsed ? " deploy-collapsible-collapsed" : ""}`}
                  style={{ maxHeight: existingDeployCollapsed ? "0" : "200px" }}
                >
                  <Text size={200} block style={{ marginTop: 8 }}>
                    Restored from deployment <strong>{existingDeploy.instanceId}</strong>
                  </Text>
                  <Checkbox
                    checked={overridePriorSettings}
                    onChange={(_, d) => setOverridePriorSettings(!!d.checked)}
                    label="Override previous settings"
                    style={{ marginTop: tokens.spacingVerticalXS }}
                  />
                </div>
              </div>
            )}
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
                    if (!cap) return null;
                    const isActive = cap.state === "Active";
                    const isPaused = cap.state === "Paused" || cap.state === "Suspended";
                    const isResuming = cap.state === "Resuming" || resumingCapacity;
                    return (
                      <>
                      {/* Status badge — always visible */}
                      {isActive && !resumingCapacity && (
                        <Badge color="success" size="small">Active</Badge>
                      )}
                      {isResuming && (
                        <Badge color="warning" size="small" style={{ animation: "pulse 1.5s ease-in-out infinite" }}>
                          {cap.state === "Active" ? "Active ✓" : "Resuming…"}
                        </Badge>
                      )}
                      {isPaused && !resumingCapacity && (
                        <Badge color="danger" size="small">{cap.state}</Badge>
                      )}
                      {/* Resume button — only when not active and not already resuming */}
                      {!isActive && (
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
                                if (usingMock) {
                                  // Mock: set state to Resuming, then Active after a delay
                                  setCapacities((prev) => prev.map((c) =>
                                    c.name === cap.name ? { ...c, state: "Resuming" } : c
                                  ));
                                  setTimeout(() => {
                                    setCapacities((prev) => prev.map((c) =>
                                      c.name === cap.name ? { ...c, state: "Active" } : c
                                    ));
                                    setResumingCapacity(false);
                                  }, 5000);
                                  return;
                                }
                                await resumeCapacity(cap.subscription, cap.resourceGroup, cap.name);
                                // Poll capacity status until Active (max 3 min)
                                let elapsed = 0;
                                const capName = cap.name;
                                const poll = setInterval(() => {
                                  elapsed += 5;
                                  refreshCapacities();
                                  if (elapsed >= 180) {
                                    clearInterval(poll);
                                    setResumingCapacity(false);
                                  }
                                }, 5000);
                                const checkActive = setInterval(() => {
                                  setCapacities((current) => {
                                    const fresh = current.find((c) => c.name === capName);
                                    if (fresh?.state === "Active") {
                                      clearInterval(poll);
                                      clearInterval(checkActive);
                                      setResumingCapacity(false);
                                    }
                                    return current;
                                  });
                                }, 3000);
                                setTimeout(() => clearInterval(checkActive), 180000);
                              } catch (e) {
                                setError(e instanceof Error ? e.message : "Failed to resume capacity");
                                setResumingCapacity(false);
                              }
                            }}
                          >
                            {resumingCapacity ? "Resuming…" : "Resume"}
                          </Button>
                        </Tooltip>
                      )}
                      </>
                    );
                  })()}
                </div>
              </Field>
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
              validationState={locationUnsupported ? "error" : undefined}
              validationMessage={locationUnsupported
                ? `Not an acceptable region. Please select from the following: ${ahdsRegions?.join(", ")}`
                : undefined}
            >
              <HistoryInput
                field="location"
                value={config.location}
                onChange={(v) => update("location", v)}
                disabled={!!existingDeploy?.priorConfig && !overridePriorSettings}
                suggestions={ahdsRegions ?? undefined}
                suggestionsLabel="Supported AHDS regions"
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
            {/* Advanced Options Toggle */}
            <div className="deploy-advanced-toggle" onClick={() => setShowAdvanced(!showAdvanced)}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <SettingsRegular style={{ fontSize: 16, color: tokens.colorBrandForeground1 }} />
                <Text weight="semibold" size={300}>Advanced Options</Text>
              </div>
              {showAdvanced ? <ChevronUpRegular /> : <ChevronDownRegular />}
            </div>
            <div className={`deploy-collapsible-content${!showAdvanced ? " deploy-collapsible-collapsed" : ""}`}
              style={{ maxHeight: showAdvanced ? "800px" : "0" }}
            >
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
          </div>
        </Card>

        {/* Fabric Configuration */}
        <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 2 ? " deploy-card-active" : ""}`}>
          <CardHeader
            className={styles.sectionHeader}
            header={
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Subtitle1>Fabric Configuration</Subtitle1>
                {(() => {
                  const { complete, total } = getCardCompletion(2);
                  return complete === total ? (
                    <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                  ) : (
                    <Badge color="informative" size="small" icon={<CircleRegular />}>{complete}/{total}</Badge>
                  );
                })()}
              </div>
            }
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
            {selectedCapacity && showAdvanced && (
              <Checkbox
                checked={pauseAfterDeploy}
                onChange={(_, d) => setPauseAfterDeploy(!!d.checked)}
                label={`Pause capacity "${selectedCapacity}" after successful deployment`}
              />
            )}
          </div>
        </Card>

        {/* Data Configuration */}
        <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 3 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
          <CardHeader
            className={styles.sectionHeader}
            header={
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Subtitle1>Data Configuration</Subtitle1>
                {(() => {
                  const { complete, total } = getCardCompletion(3);
                  return complete === total ? (
                    <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                  ) : (
                    <Badge color="error" size="small">Required</Badge>
                  );
                })()}
              </div>
            }
            description="Synthetic patient data generation and alerting"
          />
          <div className={styles.fieldGroup}>

            {/* Existing deployment detection banner */}
            {checkingExisting && (
              <div style={{
                padding: tokens.spacingHorizontalS,
                backgroundColor: tokens.colorNeutralBackground4,
                borderLeft: `4px solid ${tokens.colorBrandStroke1}`,
                borderRadius: tokens.borderRadiusMedium,
                marginBottom: tokens.spacingVerticalS,
                display: "flex",
                alignItems: "center",
                gap: tokens.spacingHorizontalS,
              }}>
                <Badge color="informative" size="small">Checking</Badge>
                <Text size={200}>Querying Azure...</Text>
              </div>
            )}

            {existingDeploy && (
              <div style={{
                padding: tokens.spacingHorizontalM,
                backgroundColor: tokens.colorStatusWarningBackground1,
                borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
                borderRadius: tokens.borderRadiusMedium,
                marginBottom: tokens.spacingVerticalS,
              }}>
                <div
                  onClick={() => setExistingDeployCollapsed(!existingDeployCollapsed)}
                  style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}
                >
                  <Text weight="semibold">
                    Previous deployment detected
                  </Text>
                  {existingDeployCollapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                </div>
                <div className={`deploy-collapsible-content${existingDeployCollapsed ? " deploy-collapsible-collapsed" : ""}`}
                  style={{ maxHeight: existingDeployCollapsed ? "0" : "400px" }}
                >
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
                <InfoLabel info="Email address for clinical alert notifications via Data Activator (Reflex)." infoButton={{ popover: { positioning: "after" } }}>
                  Alert Email
                </InfoLabel>
              }
              required
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

        {/* Phase & Component Control */}
        <Card className={`${styles.section} ${styles.cardOptional}`}>
          <CardHeader
            className={styles.sectionHeader}
            header={
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Subtitle1>Phase &amp; Component Control</Subtitle1>
                <Badge color="informative" size="small">Optional</Badge>
              </div>
            }
            description={
              <span className={styles.fieldLabelWithIcon}>
                <img src="/icon-phases.svg" alt="" width={14} height={14} />
                <span className={styles.labelSeparator} />
                Toggle individual components — dependencies auto-adjust
              </span>
            }
          />
          <div className={styles.checkboxGroup}>
            {/* ── Phase 1: Infrastructure & Data ── */}
            <Text weight="semibold" size={300} style={{ marginTop: tokens.spacingVerticalS, color: tokens.colorBrandForeground1 }}>
              Phase 1: Infrastructure &amp; Data
            </Text>
            <Tooltip content="Skip Event Hub, ACR, emulator ACI, Storage, and Bicep infra" relationship="description" positioning="after">
              <Checkbox
                label={`Azure Emulator Infrastructure${existingDeploy ? " (already deployed)" : ""}`}
                checked={!config.skip_base_infra}
                onChange={(_, d) => update("skip_base_infra", !d.checked)}
              />
            </Tooltip>
            <Tooltip content="Skip FHIR R4 service deployment and data loading" relationship="description" positioning="after">
              <Checkbox
                label={`FHIR Service + Data Loading${existingDeploy ? " (already deployed)" : ""}`}
                checked={!config.skip_fhir}
                onChange={(_, d) => update("skip_fhir", !d.checked)}
                disabled={config.skip_base_infra}
              />
            </Tooltip>
            <div style={{ paddingLeft: 24 }}>
              <Tooltip content="Skip Synthea patient generation — use existing patients" relationship="description" positioning="after">
                <Checkbox
                  label="Synthea Patient Generation"
                  checked={!config.skip_synthea}
                  onChange={(_, d) => update("skip_synthea", !d.checked)}
                  disabled={config.skip_fhir}
                />
              </Tooltip>
              <Tooltip content="Skip Device resource creation + patient associations" relationship="description" positioning="after">
                <Checkbox
                  label="Device Associations"
                  checked={!config.skip_device_assoc}
                  onChange={(_, d) => update("skip_device_assoc", !d.checked)}
                  disabled={config.skip_synthea || config.skip_fhir}
                />
              </Tooltip>
            </div>
            <Tooltip content="Skip DICOM service, TCIA download, and imaging study upload" relationship="description" positioning="after">
              <Checkbox
                label="DICOM Download + Upload"
                checked={!config.skip_dicom}
                onChange={(_, d) => update("skip_dicom", !d.checked)}
                disabled={config.skip_base_infra}
              />
            </Tooltip>
            <Tooltip content="Skip Eventhouse, KQL Database, Eventstream, and alert functions" relationship="description" positioning="after">
              <Checkbox
                label="Fabric RTI (Eventhouse + Eventstream)"
                checked={!config.skip_fabric}
                onChange={(_, d) => update("skip_fabric", !d.checked)}
              />
            </Tooltip>
            <div style={{ paddingLeft: 24 }}>
              <Tooltip content="Skip FHIR $export to ADLS Gen2 — HDS pipelines need this data" relationship="description" positioning="after">
                <Checkbox
                  label="FHIR $export to ADLS"
                  checked={!config.skip_fhir_export}
                  onChange={(_, d) => update("skip_fhir_export", !d.checked)}
                  disabled={config.skip_fabric || config.skip_fhir}
                />
              </Tooltip>
            </div>

            {/* ── Phase 2: Enrichment & Agents ── */}
            <Text weight="semibold" size={300} style={{ marginTop: tokens.spacingVerticalM, color: tokens.colorBrandForeground1 }}>
              Phase 2: Enrichment &amp; Agents
            </Text>
            <Tooltip content="Skip RTI Phase 2 — KQL→Silver shortcuts and enriched alert functions" relationship="description" positioning="after">
              <Checkbox
                label="RTI Phase 2 (Shortcuts + Enrichment)"
                checked={!config.skip_rti_phase2}
                onChange={(_, d) => update("skip_rti_phase2", !d.checked)}
                disabled={config.skip_fabric}
              />
            </Tooltip>
            <Tooltip content="Skip DICOM shortcut creation and HDS pipeline triggers (clinical, imaging, OMOP)" relationship="description" positioning="after">
              <Checkbox
                label="DICOM Shortcut + HDS Pipelines"
                checked={!config.skip_hds_pipelines}
                onChange={(_, d) => update("skip_hds_pipelines", !d.checked)}
                disabled={config.skip_dicom}
              />
            </Tooltip>
            <Tooltip content="Skip Patient 360 + Clinical Triage Data Agents" relationship="description" positioning="after">
              <Checkbox
                label="Data Agents (Patient 360 + Clinical Triage)"
                checked={!config.skip_data_agents}
                onChange={(_, d) => update("skip_data_agents", !d.checked)}
              />
            </Tooltip>

            {/* ── Phase 3: Imaging & Reporting ── */}
            <Text weight="semibold" size={300} style={{ marginTop: tokens.spacingVerticalM, color: tokens.colorBrandForeground1 }}>
              Phase 3: Imaging &amp; Reporting
            </Text>
            <Tooltip content="Skip Cohorting Agent, OHIF DICOM Viewer, PBI Imaging Report" relationship="description" positioning="after">
              <Checkbox
                label="Imaging Toolkit (Cohorting, Viewer, Report)"
                checked={!config.skip_imaging}
                onChange={(_, d) => update("skip_imaging", !d.checked)}
                disabled={config.skip_dicom}
              />
            </Tooltip>

            {/* ── Phase 4: Semantic Layer & Alerts ── */}
            <Text weight="semibold" size={300} style={{ marginTop: tokens.spacingVerticalM, color: tokens.colorBrandForeground1 }}>
              Phase 4: Semantic Layer &amp; Alerts
            </Text>
            <Tooltip content="Skip ClinicalDeviceOntology (9 entities), DeviceAssociation table, agent binding" relationship="description" positioning="after">
              <Checkbox
                label="Ontology + Agent Binding"
                checked={!config.skip_ontology}
                onChange={(_, d) => update("skip_ontology", !d.checked)}
              />
            </Tooltip>
            <Tooltip content={`Skip Data Activator Reflex + email rule${!config.alert_email ? " (no alert email set)" : ""}`} relationship="description" positioning="after">
              <Checkbox
                label="Data Activator (Email Alerts)"
                checked={!config.skip_activator}
                onChange={(_, d) => update("skip_activator", !d.checked)}
                disabled={config.skip_fabric || !config.alert_email}
              />
            </Tooltip>
          </div>
        </Card>

        {error && <div className={styles.error} ref={(el) => el?.scrollIntoView({ behavior: "smooth" })}>{error}</div>}
      </div>
      )}

      {/* Actions */}
      {validationErrors.length > 0 && !initializing && (
        <div style={{
          marginTop: tokens.spacingVerticalL,
          padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
          backgroundColor: tokens.colorStatusWarningBackground1,
          borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
          borderRadius: tokens.borderRadiusMedium,
        }}>
          <Text weight="semibold" size={200} block>Complete these required fields before deployment:</Text>
          {validationErrors.map((issue) => (
            <Text key={issue} size={200} block style={{ color: tokens.colorNeutralForeground2 }}>• {issue}</Text>
          ))}
        </div>
      )}
      <div className={styles.actions}>
        <Tooltip content="Launch the full deployment pipeline" relationship="description">
          <Button
            appearance="primary"
            icon={<RocketRegular />}
            onClick={handleSubmit}
            disabled={!canStartDeployment}
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
      </div>

      {/* Summary Sidebar (wide screens only) */}
      {showSummary && (
        <div
          className={styles.summarySidebar}
          style={{
            width: "280px",
            display: "none",
          }}
        >
          <style>{`
            @media (min-width: 1800px) {
              .${styles.summarySidebar} {
                display: block !important;
              }
            }
          `}</style>
          <Card>
            <CardHeader
              header={<Subtitle1>Deployment Summary</Subtitle1>}
            />
            <div style={{ padding: "0 16px 16px", display: "flex", flexDirection: "column", gap: 12 }}>
              <div>
                <Text size={200} weight="semibold" block>Deployment Name</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {namingPrefix || config.fabric_workspace_name || "<not set>"}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Subscription</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {subscriptions.find(s => s.id === selectedSubscription)?.name?.substring(0, 25) || "<not selected>"}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Capacity</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {selectedCapacity || "<not selected>"}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Patient Count</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {config.reuse_patients ? `${config.patient_count} (reusing)` : config.patient_count}
                </Text>
              </div>
              <div style={{ borderTop: `1px solid ${tokens.colorNeutralStroke2}`, paddingTop: 12, marginTop: 4 }}>
                <Text size={200} weight="semibold" block>Estimated Duration</Text>
                <Text size={300} weight="bold" block style={{ color: tokens.colorBrandForeground1, marginTop: 4 }}>
                  {getEstimatedDuration()}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block style={{ marginBottom: 4 }}>Components</Text>
                <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                  {!config.skip_base_infra && <Text size={100}>✓ Infrastructure</Text>}
                  {!config.skip_fhir && <Text size={100}>✓ FHIR Service</Text>}
                  {!config.skip_dicom && <Text size={100}>✓ DICOM Service</Text>}
                  {!config.skip_fabric && <Text size={100}>✓ Fabric RTI</Text>}
                  {!config.skip_hds_pipelines && <Text size={100}>✓ HDS Pipelines</Text>}
                  {!config.skip_data_agents && <Text size={100}>✓ Data Agents</Text>}
                  {!config.skip_imaging && <Text size={100}>✓ Imaging Toolkit</Text>}
                  {!config.skip_ontology && <Text size={100}>✓ Ontology</Text>}
                  {!config.skip_activator && config.alert_email && <Text size={100}>✓ Alerts</Text>}
                </div>
              </div>
              {Object.keys(config.tags).length > 0 && (
                <div>
                  <Text size={200} weight="semibold" block>Tags</Text>
                  {Object.entries(config.tags).slice(0, 3).map(([k, v]) => (
                    <Text key={k} size={100} block style={{ color: tokens.colorNeutralForeground3 }}>
                      {k}: {v}
                    </Text>
                  ))}
                  {Object.keys(config.tags).length > 3 && (
                    <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>+{Object.keys(config.tags).length - 3} more</Text>
                  )}
                </div>
              )}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}
