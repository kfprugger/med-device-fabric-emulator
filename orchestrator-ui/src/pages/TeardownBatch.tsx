import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Badge, Button, Card, CardHeader, Spinner, Text, Title2, tokens } from "@fluentui/react-components";
import { ArrowSyncRegular, EyeRegular } from "@fluentui/react-icons";
import { getTeardownBatch, type TeardownBatchStatus } from "../api";

function statusColor(status: string): "success" | "warning" | "danger" | "informative" | "subtle" {
  if (status === "Completed") return "success";
  if (status === "Running") return "informative";
  if (status === "Failed" || status === "Terminated") return "danger";
  return "subtle";
}

export function TeardownBatch() {
  const { batchId = "" } = useParams<{ batchId: string }>();
  const navigate = useNavigate();
  const [batch, setBatch] = useState<TeardownBatchStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const refresh = () => {
    if (!batchId) return;
    setLoading(true);
    setError("");
    getTeardownBatch(batchId)
      .then(setBatch)
      .catch((e) => setError(e instanceof Error ? e.message : "Unable to load teardown batch"))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [batchId]);

  return (
    <div style={{ display: "grid", gap: tokens.spacingVerticalM }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <Title2>Teardown Batch</Title2>
          <Text block size={200} style={{ color: tokens.colorNeutralForeground3 }}>{batchId}</Text>
        </div>
        <Button icon={<ArrowSyncRegular />} onClick={refresh} disabled={loading}>Refresh</Button>
      </div>

      {loading && !batch && <Spinner label="Loading teardown batch..." />}
      {error && <Text style={{ color: tokens.colorStatusDangerForeground1 }}>{error}</Text>}

      {batch && (
        <>
          <Card>
            <CardHeader
              header={<Text weight="semibold">Summary</Text>}
              description={`${batch.summary.completed}/${batch.summary.total} complete · ${batch.summary.running} running · ${batch.summary.failed} failed`}
            />
          </Card>
          {batch.children.map((child) => {
            const cs = child.customStatus ?? {};
            const display = String(cs.displayName || cs.workspaceName || cs.resourceGroupName || child.instanceId);
            return (
              <Card key={child.instanceId}>
                <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalM, padding: tokens.spacingHorizontalL }}>
                  <div style={{ flex: 1 }}>
                    <Text weight="semibold" block>{display}</Text>
                    <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>{String(cs.detail || child.instanceId)}</Text>
                  </div>
                  <Badge color={statusColor(child.runtimeStatus)}>{child.runtimeStatus}</Badge>
                  <Button icon={<EyeRegular />} onClick={() => navigate(`/monitor/${child.instanceId}`)}>View</Button>
                </div>
              </Card>
            );
          })}
        </>
      )}
    </div>
  );
}
