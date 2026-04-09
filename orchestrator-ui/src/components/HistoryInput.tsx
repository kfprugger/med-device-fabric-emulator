/**
 * Input with autocomplete suggestions from form history.
 * Fetches previously used values from the backend and shows them as you type.
 */

import { useState, useEffect, useRef } from "react";
import { Input, makeStyles, tokens } from "@fluentui/react-components";
import { getHistory, addToHistory } from "../formHistory";

const useStyles = makeStyles({
  wrapper: {
    position: "relative" as const,
  },
  suggestions: {
    position: "absolute" as const,
    top: "100%",
    left: 0,
    right: 0,
    zIndex: 1000,
    backgroundColor: tokens.colorNeutralBackground1,
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: tokens.borderRadiusMedium,
    boxShadow: tokens.shadow16,
    maxHeight: "200px",
    overflowY: "auto" as const,
    marginTop: "2px",
  },
  suggestion: {
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    cursor: "pointer",
    fontSize: tokens.fontSizeBase300,
    fontFamily: "'Segoe UI', sans-serif",
    color: tokens.colorNeutralForeground1,
    borderBottom: `1px solid ${tokens.colorNeutralStroke2}`,
    transition: "background-color 0.1s ease",
    ":hover": {
      backgroundColor: tokens.colorBrandBackground2,
    },
  },
  suggestionActive: {
    backgroundColor: tokens.colorBrandBackground2,
  },
  suggestionIcon: {
    marginRight: tokens.spacingHorizontalS,
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase200,
  },
});

interface HistoryInputProps {
  field: string;
  value: string;
  onChange: (value: string) => void;
  onCommit?: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  type?: "text" | "email" | "url" | "tel" | "search" | "password";
}

export function HistoryInput({
  field,
  value,
  onChange,
  onCommit,
  placeholder,
  disabled,
  type,
}: HistoryInputProps) {
  const styles = useStyles();
  const [history, setHistory] = useState<string[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const wrapperRef = useRef<HTMLDivElement>(null);

  // Fetch history on mount
  useEffect(() => {
    getHistory(field).then(setHistory).catch(() => {});
  }, [field]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Filter suggestions based on current input
  const filtered = value
    ? history.filter(
        (h) => h.toLowerCase().includes(value.toLowerCase()) && h !== value
      )
    : history.filter((h) => h !== value);

  const handleSelect = (suggestion: string) => {
    onChange(suggestion);
    setShowSuggestions(false);
    setActiveIndex(-1);
    onCommit?.(suggestion);
  };

  const handleBlur = () => {
    // Save to history on blur if value is non-empty
    if (value.trim()) {
      addToHistory(field, value.trim());
      // Refresh history
      getHistory(field).then(setHistory).catch(() => {});
    }
    // Delay hiding to allow click on suggestion
    setTimeout(() => setShowSuggestions(false), 200);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!showSuggestions || filtered.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIndex((i) => Math.min(i + 1, filtered.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && activeIndex >= 0) {
      e.preventDefault();
      handleSelect(filtered[activeIndex]);
    } else if (e.key === "Escape") {
      setShowSuggestions(false);
    }
  };

  return (
    <div ref={wrapperRef} className={styles.wrapper}>
      <Input
        value={value}
        onChange={(_, d) => {
          onChange(d.value);
          setShowSuggestions(true);
          setActiveIndex(-1);
        }}
        onFocus={() => setShowSuggestions(true)}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={disabled}
        type={type}
      />
      {showSuggestions && filtered.length > 0 && !disabled && (
        <div className={styles.suggestions}>
          {filtered.map((s, i) => (
            <div
              key={s}
              className={`${styles.suggestion} ${i === activeIndex ? styles.suggestionActive : ""}`}
              onMouseDown={() => handleSelect(s)}
            >
              <span className={styles.suggestionIcon}>&#x1F552;</span>
              {s}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
