/**
 * All Logs Stream — interactive console-style logs viewer supporting full-text search,
 * log-level filtering, and real-time ANSI-style colored streams.
 */

import { useState, useMemo } from "react";
import { Card, CardHeader, Subtitle1, Input, tokens } from "@fluentui/react-components";
import { SearchRegular } from "@fluentui/react-icons";

export interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
  phase?: string | number;
}

interface AllLogsStreamProps {
  logs: LogEntry[];
}

export function AllLogsStream({ logs }: AllLogsStreamProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedLevel, setSelectedLevel] = useState("all");

  // Filter logs based on search query and level dropdown selections
  const filteredLogs = useMemo(() => {
    return logs.filter((log) => {
      const matchesLevel =
        selectedLevel === "all" ||
        (log.level || "info").toLowerCase() === selectedLevel ||
        (selectedLevel === "warn" && log.level === "warning");

      const matchesSearch =
        !searchQuery ||
        (log.message || "").toLowerCase().includes(searchQuery.toLowerCase()) ||
        (log.phase !== undefined && String(log.phase).toLowerCase().includes(searchQuery.toLowerCase()));

      return matchesLevel && matchesSearch;
    });
  }, [logs, searchQuery, selectedLevel]);

  return (
    <Card style={{ marginBottom: tokens.spacingVerticalL, boxShadow: tokens.shadow16 }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        flexWrap: "wrap",
        gap: tokens.spacingHorizontalS,
        padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalM}`,
      }}>
        <CardHeader
          header={
            <Subtitle1 style={{ fontWeight: "bold" }}>
              Console Stream ({filteredLogs.length} / {logs.length})
            </Subtitle1>
          }
        />
        
        {/* Terminal Controls Bar */}
        <div style={{ display: "flex", gap: tokens.spacingHorizontalS, alignItems: "center" }}>
          {/* Level Filter Dropdown */}
          <select
            value={selectedLevel}
            onChange={(e) => setSelectedLevel(e.target.value)}
            style={{
              padding: "6px 12px",
              borderRadius: tokens.borderRadiusMedium,
              border: `1px solid ${tokens.colorNeutralStroke1}`,
              backgroundColor: tokens.colorNeutralBackground1,
              color: tokens.colorNeutralForeground1,
              fontFamily: "inherit",
              fontSize: tokens.fontSizeBase200,
              cursor: "pointer",
              outline: "none",
            }}
          >
            <option value="all">All Levels</option>
            <option value="info">Info</option>
            <option value="success">Success</option>
            <option value="warn">Warnings</option>
            <option value="error">Errors</option>
          </select>

          {/* Log Search Input */}
          <Input
            value={searchQuery}
            onChange={(_, data) => setSearchQuery(data.value)}
            placeholder="Search console..."
            contentBefore={<SearchRegular />}
            size="small"
            style={{ minWidth: "220px" }}
          />
        </div>
      </div>

      {/* Console Display Screen */}
      <div style={{
        maxHeight: "550px",
        overflowY: "auto",
        padding: `${tokens.spacingVerticalM} ${tokens.spacingHorizontalL}`,
        backgroundColor: "#0a0a0a",
        borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
        borderBottomLeftRadius: tokens.borderRadiusMedium,
        borderBottomRightRadius: tokens.borderRadiusMedium,
        fontFamily: "'Cascadia Code', 'Consolas', 'Fira Code', monospace",
        fontSize: "12.5px",
        lineHeight: "1.7",
        boxShadow: "inset 0 0 12px rgba(0, 0, 0, 0.8)",
      }}>
        {filteredLogs.length === 0 ? (
          <div style={{ color: tokens.colorNeutralForeground4, padding: "20px 0", textAlign: "center" }}>
            No logs matched the selected filter criteria.
          </div>
        ) : (
          filteredLogs.map((log, i) => {
            const time = log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : "";
            
            // Console colors mapping (highly legible neon palette)
            let color = "#e0e0e0";
            if (log.level === "success") color = "#00f07f"; // neon green
            else if (log.level === "error") color = "#ff4d4d"; // neon red
            else if (log.level === "warn" || log.level === "warning") color = "#ffcc00"; // neon yellow
            else if (log.level === "info") color = "#afbac4"; // classic console gray

            return (
              <div key={i} style={{ color, display: "flex", gap: "12px", borderBottom: "1px solid rgba(255, 255, 255, 0.03)", padding: "2px 0" }}>
                {/* Timestamp */}
                <span style={{ color: "#5c6370", minWidth: "75px", flexShrink: 0, userSelect: "none" }}>
                  [{time}]
                </span>
                
                {/* Optional Phase Number Badge */}
                {log.phase !== undefined && (
                  <span style={{
                    color: tokens.colorPaletteBlueForeground2,
                    fontWeight: "bold",
                    minWidth: "28px",
                    flexShrink: 0,
                    userSelect: "none"
                  }}>
                    P{log.phase}
                  </span>
                )}
                
                {/* Log Line Content */}
                <span style={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{log.message}</span>
              </div>
            );
          })
        )}
      </div>
    </Card>
  );
}
