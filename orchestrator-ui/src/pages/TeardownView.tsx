import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Checkbox,
  Dropdown,
  Field,
  Option,
  Subtitle1,
  Text,
  Title2,
  Tooltip,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import {
  DeleteRegular,
  ArrowSyncRegular,
  SearchRegular,
  ChevronDownRegular,
  ChevronUpRegular,
  LockClosedRegular,
  LockOpenRegular,
} from "@fluentui/react-icons";
import { startTeardown, getDeploymentCapacity, type DeploymentCapacityMapping } from "../api";
import { useAppState } from "../AppState";
import { typeBadge } from "../components/TypeBadges";
import { MockDataBanner } from "../components/MockDataBanner";
import {
  getMockSubscriptions,
  scanForTeardownCandidates,
  startMockTeardown,
  type TeardownCandidate,
  type MockSubscription,
} from "../mockDeployment";

const useStyles = makeStyles({
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: tokens.spacingVerticalL,
  },
  scanControls: {
    display: "flex",
    alignItems: "flex-end",
    gap: tokens.spacingHorizontalL,
    marginBottom: tokens.spacingVerticalL,
  },
  candidateList: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalS,
  },
  candidateCard: {
    transition: "box-shadow 0.2s ease, transform 0.15s ease",
    cursor: "pointer",
    ":hover": {
      boxShadow: tokens.shadow4,
      transform: "translateY(-1px)",
    },
  },
  candidateCardSelected: {
    border: `2px solid ${tokens.colorPaletteRedForeground1}`,
    boxShadow: tokens.shadow8,
  },
  candidateRow: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalM,
    width: "100%",
  },
  candidateInfo: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXXS,
  },
  candidateName: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
  },
  artifactList: {
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    backgroundColor: tokens.colorNeutralBackground3,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
    fontSize: tokens.fontSizeBase200,
    lineHeight: "1.6",
    fontFamily: "'Cascadia Code', 'Consolas', monospace",
    maxHeight: "200px",
    overflowY: "auto" as const,
  },
  actions: {
    display: "flex",
    gap: tokens.spacingHorizontalM,
    marginTop: tokens.spacingVerticalXL,
  },
  warning: {
    padding: tokens.spacingHorizontalM,
    backgroundColor: tokens.colorStatusDangerBackground1,
    borderLeft: `4px solid ${tokens.colorStatusDangerBorderActive}`,
    borderRadius: tokens.borderRadiusMedium,
    color: tokens.colorStatusDangerForeground1,
    fontSize: tokens.fontSizeBase300,
    fontWeight: tokens.fontWeightSemibold,
    marginBottom: tokens.spacingVerticalL,
  },
  error: {
    color: tokens.colorStatusDangerForeground1,
    fontSize: tokens.fontSizeBase200,
    marginTop: tokens.spacingVerticalS,
  },
  sectionTitle: {
    marginTop: tokens.spacingVerticalXL,
    marginBottom: tokens.spacingVerticalS,
  },
  sectionDesc: {
    color: tokens.colorNeutralForeground3,
    marginBottom: tokens.spacingVerticalS,
    display: "block" as const,
  },
  emptyState: {
    padding: tokens.spacingVerticalXL,
    textAlign: "center" as const,
    color: tokens.colorNeutralForeground3,
  },
});

function statusBadge(status: string) {
  switch (status) {
    case "full":
      return <Badge color="success">Full Deploy</Badge>;
    case "partial":
      return <Badge color="warning">Partial</Badge>;
    case "orphaned":
      return <Badge color="danger">Orphaned</Badge>;
    case "active":
      return <Badge color="informative">Active</Badge>;
    default:
      return <Badge color="subtle">{status}</Badge>;
  }
}

export function TeardownView() {
  const styles = useStyles();
  const navigate = useNavigate();
  const { selectedSubscription, setSelectedSubscription } = useAppState();
  const [loading, setLoading] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [error, setError] = useState("");
  const [subscriptions, setSubscriptions] = useState<MockSubscription[]>(getMockSubscriptions());
  const [candidates, setCandidates] = useState<TeardownCandidate[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [lockedIds, setLockedIds] = useState<Set<string>>(new Set());
  const [scanned, setScanned] = useState(false);
  const [usingMock, setUsingMock] = useState(false);
  const [capacityMappings, setCapacityMappings] = useState<Map<string, DeploymentCapacityMapping>>(new Map());

  // Load locks from backend on mount
  useEffect(() => {
    fetch("/api/locks")
      .then((r) => r.json())
      .then((ids: string[]) => {
        if (ids.length > 0) setLockedIds(new Set(ids));
      })
      .catch(() => {
        // Fall back to localStorage
        try {
          const saved = localStorage.getItem("teardown-locks");
          if (saved) setLockedIds(new Set(JSON.parse(saved)));
        } catch { /* ignore */ }
      });
  }, []);

  // Persist locks to backend (and localStorage fallback) whenever they change
  const persistLocks = useCallback((ids: Set<string>, prevIds: Set<string>) => {
    // Find added and removed locks
    for (const id of ids) {
      if (!prevIds.has(id)) {
        fetch(`/api/locks/${encodeURIComponent(id)}`, { method: "POST" }).catch(() => {});
      }
    }
    for (const id of prevIds) {
      if (!ids.has(id)) {
        fetch(`/api/locks/${encodeURIComponent(id)}`, { method: "DELETE" }).catch(() => {});
      }
    }
    localStorage.setItem("teardown-locks", JSON.stringify([...ids]));
  }, []);

  useEffect(() => {
    // Fetch real subscriptions first, then scan with the correct default
    fetch("/api/scan/subscriptions")
      .then((r) => r.json())
      .then((subs: MockSubscription[]) => {
        if (subs.length > 0) {
          setSubscriptions(subs);
          // Set default subscription from API if none selected yet
          if (!selectedSubscription) {
            setSelectedSubscription(subs[0].id);
          }
        }
      })
      .catch(() => {}) // Fall back to mock subscriptions
      .finally(() => {
        handleScan();
      });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleScan = () => {
    setScanning(true);
    setSelectedIds(new Set()); // Clear selection on rescan

    // Try real backend first, fall back to mock
    fetch(`/api/scan/resources?subscription_id=${encodeURIComponent(selectedSubscription)}`)
      .then((r) => r.json())
      .then((results: TeardownCandidate[]) => {
        setCandidates(results);
        setScanned(true);
        setScanning(false);
        setUsingMock(false);
        // Look up capacity mappings for Azure RGs
        const azureRgs = results.filter((c) => c.type === "azure");
        for (const rg of azureRgs) {
          getDeploymentCapacity(rg.name)
            .then((mapping) => {
              if (mapping) {
                setCapacityMappings((prev) => {
                  const next = new Map(prev);
                  next.set(rg.name, mapping);
                  return next;
                });
              }
            })
            .catch(() => {});
        }
      })
      .catch(() => {
        // Fall back to mock data if backend unavailable
        setCandidates(scanForTeardownCandidates(selectedSubscription));
        setScanned(true);
        setScanning(false);
        setUsingMock(true);
      });
  };

  const toggleSelected = (id: string) => {
    if (lockedIds.has(id)) return;
    const candidate = candidates.find((c) => c.id === id);

    setSelectedIds((prev) => {
      const next = new Set(prev);
      const selecting = !next.has(id);

      if (selecting) {
        next.add(id);
        // Auto-select matching SPNs when a Fabric workspace is selected
        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name && !lockedIds.has(c.id)
          );
          for (const spn of matchingSpns) {
            next.add(spn.id);
          }
        }
      } else {
        next.delete(id);
        // Auto-deselect matching SPNs when a Fabric workspace is deselected
        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name
          );
          for (const spn of matchingSpns) {
            next.delete(spn.id);
          }
        }
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelectedIds(new Set(candidates.filter((c) => !lockedIds.has(c.id)).map((c) => c.id)));
  };

  const deselectAll = () => {
    setSelectedIds(new Set());
  };

  const toggleLocked = (id: string) => {
    setLockedIds((prev) => {
      const next = new Set(prev);
      const candidate = candidates.find((c) => c.id === id);
      const locking = !next.has(id);

      if (locking) {
        next.add(id);
        setSelectedIds((sel) => {
          const nextSel = new Set(sel);
          nextSel.delete(id);
          return nextSel;
        });

        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name
          );
          for (const spn of matchingSpns) {
            next.add(spn.id);
            setSelectedIds((sel) => {
              const nextSel = new Set(sel);
              nextSel.delete(spn.id);
              return nextSel;
            });
          }
        }
      } else {
        next.delete(id);

        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name
          );
          for (const spn of matchingSpns) {
            next.delete(spn.id);
          }
        }
      }
      persistLocks(next, prev);
      return next;
    });
  };

  const unlocked = candidates.filter((c) => !lockedIds.has(c.id));
  const allSelected = unlocked.length > 0 && selectedIds.size === unlocked.length;
  const someSelected = selectedIds.size > 0 && selectedIds.size < unlocked.length;

  const toggleExpanded = (id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleTeardown = async () => {
    if (selectedIds.size === 0) {
      setError("Select at least one resource to tear down.");
      return;
    }
    setLoading(true);
    setError("");

    const selected = candidates.filter((c) => selectedIds.has(c.id));
    const fabricWs = selected.find((c) => c.type === "fabric");
    const azureRg = selected.find((c) => c.type === "azure");

    // Try real backend first
    try {
      await startTeardown({
        fabric_workspace_name: fabricWs?.name ?? "",
        resource_group_name: azureRg?.name ?? "",
        delete_workspace: !!fabricWs,
        delete_azure_rg: !!azureRg,
      });
      // Real backend accepted — navigate to deploy monitor to see real logs
      setLoading(false);
      // Use the history tab to track real teardowns (they appear as deployments)
      navigate("/history");
      return;
    } catch {
      // Backend unavailable — fall back to mock teardown
      for (const candidate of selected) {
        startMockTeardown(candidate);
      }
    }

    setLoading(false);
    navigate("/teardown/monitor");
  };

  const renderCandidate = (c: TeardownCandidate) => {
    const isSelected = selectedIds.has(c.id);
    const isExpanded = expandedIds.has(c.id);
    const isLocked = lockedIds.has(c.id);

    return (
      <Card
        key={c.id}
        className={`${styles.candidateCard} ${isSelected ? styles.candidateCardSelected : ""}`}
        size="small"
        style={isLocked ? { opacity: 0.6 } : undefined}
      >
        <CardHeader
          header={
            <div className={styles.candidateRow}>
              <Checkbox
                checked={isSelected}
                onChange={() => toggleSelected(c.id)}
                disabled={isLocked}
              />
              <div className={styles.candidateInfo}>
                <div className={styles.candidateName}>
                  {typeBadge(c.type)}
                  <Text weight="semibold">{c.name}</Text>
                  {statusBadge(c.status)}
                  {isLocked && <Badge color="subtle">Locked</Badge>}
                </div>
                <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                  {c.detail}
                </Text>
                {c.type === "azure" && capacityMappings.has(c.name) && (() => {
                  const m = capacityMappings.get(c.name)!;
                  return (
                    <div style={{ display: "flex", gap: tokens.spacingHorizontalS, alignItems: "center", marginTop: 2 }}>
                      <Badge color="brand" size="small">Fabric Capacity</Badge>
                      <Text size={200}>{m.capacityName}</Text>
                      {m.workspaceName && (
                        <>
                          <Text size={200} style={{ color: tokens.colorNeutralForeground4 }}>|</Text>
                          <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>Workspace: {m.workspaceName}</Text>
                        </>
                      )}
                    </div>
                  );
                })()}
                {c.resourceCount !== undefined && (
                  <Text size={200}>
                    Resources: {c.resourceCount}/{c.expectedCount}
                  </Text>
                )}
              </div>
              <Tooltip
                content={isLocked ? "Unlock to allow teardown" : "Lock to prevent accidental deletion"}
                relationship="label"
              >
                <Button
                  appearance="subtle"
                  icon={isLocked ? <LockClosedRegular /> : <LockOpenRegular />}
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleLocked(c.id);
                  }}
                  size="small"
                  style={isLocked ? { color: tokens.colorPaletteRedForeground1 } : undefined}
                />
              </Tooltip>
              <Button
                appearance="subtle"
                icon={isExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                onClick={(e) => {
                  e.stopPropagation();
                  toggleExpanded(c.id);
                }}
                size="small"
              />
            </div>
          }
        />
        {isExpanded && c.matchedArtifacts && (
          <div className={styles.artifactList}>
            {c.matchedArtifacts.map((a, i) => (
              <div key={i}>• {a}</div>
            ))}
          </div>
        )}
      </Card>
    );
  };

  const fabricCandidates = candidates.filter((c) => c.type === "fabric");
  const azureCandidates = candidates.filter((c) => c.type === "azure");
  const spnCandidates = candidates.filter((c) => c.type === "spn");

  return (
    <div>
      {usingMock && <MockDataBanner />}
      <div className={styles.header}>
        <Title2>Teardown — Resource Scanner</Title2>
      </div>

      {/* Subscription selector + scan */}
      <div className={styles.scanControls}>
        <Field label="Azure Subscription" style={{ minWidth: 300 }}>
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
        <Button
          appearance="primary"
          icon={scanning ? <ArrowSyncRegular /> : <SearchRegular />}
          onClick={handleScan}
          disabled={scanning}
        >
          {scanning ? "Scanning…" : "Scan Resources"}
        </Button>
      </div>

      {scanned && candidates.length > 0 && (
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: tokens.spacingHorizontalM,
          marginBottom: tokens.spacingVerticalM,
          padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalM}`,
          backgroundColor: tokens.colorNeutralBackground3,
          borderRadius: tokens.borderRadiusMedium,
        }}>
          <Checkbox
            checked={allSelected ? true : someSelected ? "mixed" : false}
            onChange={() => (allSelected ? deselectAll() : selectAll())}
            label={
              allSelected
                ? "Deselect all"
                : lockedIds.size > 0
                  ? `Select all unlocked (${unlocked.length} of ${candidates.length})`
                  : `Select all (${candidates.length} resources)`
            }
          />
        </div>
      )}

      {scanned && candidates.length === 0 && (
        <div className={styles.emptyState}>No matching deployment resources found.</div>
      )}

      {fabricCandidates.length > 0 && (
        <>
          <Subtitle1 className={styles.sectionTitle}>
            Fabric Workspaces ({fabricCandidates.length})
          </Subtitle1>
          <Text size={200} className={styles.sectionDesc}>
            Workspaces matching healthcare1 naming convention with deployment artifacts
          </Text>
          <div className={styles.candidateList}>
            {fabricCandidates.map(renderCandidate)}
          </div>
        </>
      )}

      {azureCandidates.length > 0 && (
        <>
          <Subtitle1 className={styles.sectionTitle}>
            Azure Resource Groups ({azureCandidates.length})
          </Subtitle1>
          <Text size={200} className={styles.sectionDesc}>
            Resource groups with Event Hub, ACR, FHIR Service, and emulator resources (expected: 11)
          </Text>
          <div className={styles.candidateList}>
            {azureCandidates.map(renderCandidate)}
          </div>
        </>
      )}

      {spnCandidates.length > 0 && (
        <>
          <Subtitle1 className={styles.sectionTitle}>
            Workspace Identity SPNs ({spnCandidates.length})
          </Subtitle1>
          <Text size={200} className={styles.sectionDesc}>
            App registrations matching workspace identity naming from prior deployments
          </Text>
          <div className={styles.candidateList}>
            {spnCandidates.map(renderCandidate)}
          </div>
        </>
      )}

      {selectedIds.size > 0 && (() => {
        const selected = candidates.filter((c) => selectedIds.has(c.id));
        const hasFabric = selected.some((c) => c.type === "fabric");
        const hasAzure = selected.some((c) => c.type === "azure");
        const hasBoth = hasFabric && hasAzure;
        const mode = hasBoth
          ? "Teardown-All"
          : hasFabric
            ? "Fabric Teardown"
            : hasAzure
              ? "Azure Teardown"
              : "SPN Cleanup";

        return (
          <>
            <div className={styles.warning} style={{ marginTop: tokens.spacingVerticalXL }}>
              {selectedIds.size} resource(s) selected for deletion.
              {hasBoth && " Both Fabric workspace and Azure RG selected — will run Teardown-All (complete cleanup)."}
              {" "}This action cannot be undone.
            </div>
            <div className={styles.actions}>
              <Button
                appearance="primary"
                icon={<DeleteRegular />}
                onClick={handleTeardown}
                disabled={loading}
                style={{ backgroundColor: tokens.colorPaletteRedBackground3 }}
              >
                {loading ? "Starting teardown…" : `${mode}: Delete ${selectedIds.size} resource(s)`}
              </Button>
              <Button appearance="subtle" onClick={deselectAll}>
                Clear selection
              </Button>
            </div>
          </>
        );
      })()}

      {error && <div className={styles.error}>{error}</div>}
    </div>
  );
}
