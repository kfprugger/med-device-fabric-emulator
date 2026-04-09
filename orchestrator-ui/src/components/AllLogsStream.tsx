/**
 * All Logs Stream — single scrollable log viewer showing all deployment logs
 * with timestamp, phase tag, and color-coded messages.
 */

import { Card, CardHeader, Subtitle1, tokens } from "@fluentui/react-components";

export interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
  phase?: number;
}

interface AllLogsStreamProps {
  logs: LogEntry[];
}

export function AllLogsStream({ logs }: AllLogsStreamProps) {
  return (
    <Card style={{ marginBottom: tokens.spacingVerticalL }}>
      <CardHeader header={<Subtitle1>All Logs ({logs.length})</Subtitle1>} />
      <div style={{
        maxHeight: "600px",
        overflowY: "auto",
        padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
        backgroundColor: tokens.colorNeutralBackground3,
        borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
        fontFamily: "'Cascadia Code', 'Consolas', monospace",
        fontSize: tokens.fontSizeBase200,
        lineHeight: "1.6",
      }}>
        {logs.map((log, i) => {
          const time = log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : "";
          const color = log.level === "success" ? tokens.colorPaletteGreenForeground1
            : log.level === "error" ? tokens.colorPaletteRedForeground1
            : log.level === "warn" ? tokens.colorPaletteYellowForeground1
            : tokens.colorNeutralForeground2;
          return (
            <div key={i} style={{ color, display: "flex", gap: tokens.spacingHorizontalM }}>
              <span style={{ color: tokens.colorNeutralForeground3, minWidth: "70px", flexShrink: 0 }}>{time}</span>
              <span style={{ color: tokens.colorNeutralForeground4, flexShrink: 0, minWidth: "20px", fontSize: tokens.fontSizeBase100, fontWeight: tokens.fontWeightSemibold }}>
                {log.phase ? `P${log.phase}` : ""}
              </span>
              <span>{log.message}</span>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
