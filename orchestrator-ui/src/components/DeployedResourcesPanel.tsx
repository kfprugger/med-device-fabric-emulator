/**
 * Deployed Resources Panel — collapsible tables showing Azure and Fabric resources
 * discovered in the deployed resource group / workspace.
 */

import { useState } from "react";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Subtitle1,
  Text,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import { ChevronDownRegular, ChevronUpRegular, OpenRegular } from "@fluentui/react-icons";
import type { DeployedResourcesResult } from "../api";

const useStyles = makeStyles({
  resources: {
    marginTop: tokens.spacingVerticalXXL,
  },
  resourceSection: {
    marginBottom: tokens.spacingVerticalL,
  },
  resourceSectionHeader: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
    marginBottom: tokens.spacingVerticalS,
  },
  resourceTable: {
    width: "100%",
    borderCollapse: "collapse" as const,
    fontSize: tokens.fontSizeBase200,
  },
  resourceRow: {
    borderBottom: `1px solid ${tokens.colorNeutralStroke2}`,
    ":hover": {
      backgroundColor: tokens.colorNeutralBackground1Hover,
    },
  },
  resourceCell: {
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
    verticalAlign: "middle" as const,
  },
  resourceType: {
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase100,
  },
  resourceLoading: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
    padding: tokens.spacingVerticalM,
    color: tokens.colorNeutralForeground3,
  },
});

interface DeployedResourcesPanelProps {
  deployedResources: DeployedResourcesResult | null;
  resourcesLoading: boolean;
  resourceGroupName: string;
  azurePortalUrl?: string;
}

export function DeployedResourcesPanel({
  deployedResources,
  resourcesLoading,
  resourceGroupName,
  azurePortalUrl,
}: DeployedResourcesPanelProps) {
  const styles = useStyles();
  const [fabricExpanded, setFabricExpanded] = useState(false);
  const [azureExpanded, setAzureExpanded] = useState(false);

  return (
    <Card className={styles.resources}>
      <CardHeader header={<Subtitle1>Deployed Resources</Subtitle1>} />

      {resourcesLoading && !deployedResources && (
        <div className={styles.resourceLoading}>
          <span style={{ display: "inline-block" }}>⟳</span>
          <Text size={200}>Scanning Azure &amp; Fabric APIs…</Text>
        </div>
      )}

      {!resourcesLoading && !deployedResources && (
        <Text size={200} style={{ color: tokens.colorNeutralForeground3, padding: tokens.spacingVerticalS, display: "block" }}>
          Restart the backend server to enable live resource scanning.
        </Text>
      )}

      {deployedResources && (
        <>
          {/* Fabric Workspace */}
          {deployedResources.workspace && (
            <div className={styles.resourceSection}>
              <div
                className={styles.resourceSectionHeader}
                onClick={() => setFabricExpanded((v) => !v)}
                style={{ cursor: "pointer", userSelect: "none" }}
              >
                <Badge color="brand" size="small">Fabric</Badge>
                <Text weight="semibold" size={300}>
                  Workspace: {deployedResources.workspace.name}
                </Text>
                <Badge color="subtle" size="small">{deployedResources.fabric.length} items</Badge>
                <Button
                  as="a"
                  appearance="subtle"
                  icon={<OpenRegular />}
                  size="small"
                  href={deployedResources.workspace.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={(e: React.MouseEvent) => e.stopPropagation()}
                >
                  Open in Fabric
                </Button>
                <Button
                  appearance="subtle"
                  icon={fabricExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                  size="small"
                  onClick={(e) => { e.stopPropagation(); setFabricExpanded((v) => !v); }}
                  style={{ marginLeft: "auto" }}
                />
              </div>
              {fabricExpanded && deployedResources.fabric.length > 0 && (
                <table className={styles.resourceTable}>
                  <thead>
                    <tr>
                      <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Name</th>
                      <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {deployedResources.fabric.map((item) => (
                      <tr key={item.id} className={styles.resourceRow}>
                        <td className={styles.resourceCell}>
                          <Text size={200}>{item.name}</Text>
                        </td>
                        <td className={styles.resourceCell}>
                          <span className={styles.resourceType}>{item.type}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {fabricExpanded && deployedResources.fabric.length === 0 && (
                <Text size={200} style={{ color: tokens.colorNeutralForeground3, padding: tokens.spacingVerticalS }}>
                  No Fabric items found yet
                </Text>
              )}
            </div>
          )}

          {/* Azure Resources */}
          {deployedResources.azure.length > 0 && (
            <div className={styles.resourceSection}>
              <div
                className={styles.resourceSectionHeader}
                onClick={() => setAzureExpanded((v) => !v)}
                style={{ cursor: "pointer", userSelect: "none" }}
              >
                <Badge color="informative" size="small">Azure</Badge>
                <Text weight="semibold" size={300}>
                  Resource Group: {resourceGroupName}
                </Text>
                <Badge color="subtle" size="small">{deployedResources.azure.length} resources</Badge>
                <Button
                  as="a"
                  appearance="subtle"
                  icon={<OpenRegular />}
                  size="small"
                  href={azurePortalUrl || `https://portal.azure.com/#browse/resourcegroups/filterValue/${resourceGroupName}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={(e: React.MouseEvent) => e.stopPropagation()}
                >
                  Open in Azure
                </Button>
                <Button
                  appearance="subtle"
                  icon={azureExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                  size="small"
                  onClick={(e) => { e.stopPropagation(); setAzureExpanded((v) => !v); }}
                  style={{ marginLeft: "auto" }}
                />
              </div>
              {azureExpanded && (
                <table className={styles.resourceTable}>
                  <thead>
                    <tr>
                      <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Name</th>
                      <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Type</th>
                      <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Location</th>
                    </tr>
                  </thead>
                  <tbody>
                    {deployedResources.azure.map((r) => (
                      <tr key={r.id} className={styles.resourceRow}>
                        <td className={styles.resourceCell}>
                          <Text size={200}>{r.name}</Text>
                        </td>
                        <td className={styles.resourceCell}>
                          <span className={styles.resourceType}>{r.type}</span>
                        </td>
                        <td className={styles.resourceCell}>
                          <span className={styles.resourceType}>{r.location}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}

          {/* Nothing found at all */}
          {!deployedResources.workspace && deployedResources.azure.length === 0 && (
            <Text size={200} style={{ color: tokens.colorNeutralForeground3, padding: tokens.spacingVerticalS }}>
              No deployed resources detected yet
            </Text>
          )}
        </>
      )}
    </Card>
  );
}
