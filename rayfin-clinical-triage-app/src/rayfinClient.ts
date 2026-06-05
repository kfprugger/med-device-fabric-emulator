import { RayfinClient } from '@microsoft/rayfin-client';
import type { AppSchema } from '../rayfin/data/schema';

// Single shared client instance for the whole app.
export const rayfinClient = new RayfinClient<AppSchema>({
  baseUrl: import.meta.env.VITE_RAYFIN_API_URL ?? 'http://localhost:5168',
  publishableKey: import.meta.env.VITE_RAYFIN_PUBLISHABLE_KEY ?? '',
});
