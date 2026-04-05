/**
 * Form field history — backed by the backend SQLite database.
 * Falls back to localStorage if the backend is unavailable.
 */

const API_BASE = "/api";

export async function getHistory(field: string): Promise<string[]> {
  try {
    const resp = await fetch(`${API_BASE}/form-history/${encodeURIComponent(field)}`);
    if (resp.ok) return resp.json();
  } catch { /* fall through */ }
  // Fallback to localStorage
  try {
    const raw = localStorage.getItem(`form-history-${field}`);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export async function addToHistory(field: string, value: string): Promise<void> {
  if (!value.trim()) return;
  try {
    await fetch(`${API_BASE}/form-history/${encodeURIComponent(field)}?value=${encodeURIComponent(value)}`, {
      method: "POST",
    });
  } catch { /* ignore */ }
  // Also save to localStorage as fallback
  try {
    const raw = localStorage.getItem(`form-history-${field}`);
    const history: string[] = raw ? JSON.parse(raw) : [];
    const filtered = history.filter((v) => v !== value);
    filtered.unshift(value);
    localStorage.setItem(`form-history-${field}`, JSON.stringify(filtered.slice(0, 10)));
  } catch { /* ignore */ }
}

export async function getTagHistory(): Promise<Array<Record<string, string>>> {
  try {
    const resp = await fetch(`${API_BASE}/form-history/tags`);
    if (resp.ok) {
      const values: string[] = await resp.json();
      return values.map((v) => { try { return JSON.parse(v); } catch { return {}; } });
    }
  } catch { /* fall through */ }
  try {
    const raw = localStorage.getItem("form-history-tags");
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export async function addTagToHistory(tags: Record<string, string>): Promise<void> {
  if (Object.keys(tags).length === 0) return;
  const tagJson = JSON.stringify(tags);
  await addToHistory("tags", tagJson);
}
