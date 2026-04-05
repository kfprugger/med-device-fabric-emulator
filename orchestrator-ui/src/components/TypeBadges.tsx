/**
 * Consistent Azure/Fabric/SPN badge components used across the app.
 * Azure Blue (#0078D4), Fabric Teal (#117865).
 */

import { Badge } from "@fluentui/react-components";

const AZURE_BLUE = "#0078D4";
const FABRIC_TEAL = "#117865";
const ENTRA_BLUE = "#3A96DD";

export function AzureBadge() {
  return (
    <Badge
      style={{
        backgroundColor: AZURE_BLUE,
        color: "white",
      }}
    >
      Azure
    </Badge>
  );
}

export function FabricBadge() {
  return (
    <Badge
      style={{
        backgroundColor: FABRIC_TEAL,
        color: "white",
      }}
    >
      Fabric
    </Badge>
  );
}

export function EntraIdBadge() {
  return (
    <Badge
      style={{
        backgroundColor: ENTRA_BLUE,
        color: "white",
      }}
    >
      Entra ID
    </Badge>
  );
}

export function typeBadge(type: "fabric" | "azure" | "spn") {
  switch (type) {
    case "fabric":
      return <FabricBadge />;
    case "azure":
      return <AzureBadge />;
    case "spn":
      return <EntraIdBadge />;
  }
}
