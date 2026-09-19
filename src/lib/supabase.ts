import { createClient, SupabaseClient } from '@supabase/supabase-js';

// Retrieve Supabase URL and Key from env or defaults or localStorage
const DEFAULT_SUPABASE_URL = 'https://lwgprtxopdonxtmjmgqm.supabase.co';
const DEFAULT_SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx3Z3BydHhvcGRvbnh0bWptZ3FtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODc3Nzc2MzAsImV4cCI6MjEwMzM1MzYzMH0.3Zpj--H_E3A8lpI2UyjB3Nkh3-xbmLXOEwHu8lrXE-o';

function getSupabaseConfig() {
  if (typeof window === 'undefined') {
    return {
      url: DEFAULT_SUPABASE_URL,
      key: DEFAULT_SUPABASE_ANON_KEY
    };
  }
  const customUrl = localStorage.getItem('eeu_custom_supabase_url');
  const customKey = localStorage.getItem('eeu_custom_supabase_key');
  
  const envUrl = (import.meta as any).env?.VITE_SUPABASE_URL;
  const envKey = (import.meta as any).env?.VITE_SUPABASE_ANON_KEY;

  const url = customUrl || (envUrl && !envUrl.includes('your-project') ? envUrl : DEFAULT_SUPABASE_URL);
  const key = customKey || (envKey && !envKey.includes('your-anon-key') ? envKey : DEFAULT_SUPABASE_ANON_KEY);

  return { url, key };
}

const config = getSupabaseConfig();

export const isSupabaseConfigured = Boolean(
  config.url && config.key && !config.url.includes('your-project')
);

// Create direct Supabase client for static hosting (e.g. GitHub Pages) and direct fallback
export const supabase: SupabaseClient = createClient(config.url, config.key, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
  },
  realtime: {
    params: {
      eventsPerSecond: 10,
    },
  },
});

/**
 * Health check helper to verify connectivity
 */
export async function checkSupabaseHealth(): Promise<{ connected: boolean; count: number; latencyMs: number; error?: string }> {
  const start = Date.now();
  try {
    // If running on static host (like GitHub Pages), check Supabase directly
    if (typeof window !== 'undefined' && (window.location.hostname.includes('github.io') || window.location.hostname.includes('localhost') === false && !window.location.port)) {
      const { data, error } = await supabase.from('interruptions').select('id').limit(1);
      const latencyMs = Date.now() - start;
      if (!error) {
        return { connected: true, count: data?.length ?? 0, latencyMs };
      }
      return { connected: false, count: 0, latencyMs, error: error.message };
    }

    // Try server /api/health first
    const res = await fetch('/api/health').catch(() => null);
    const latencyMs = Date.now() - start;
    if (res && res.ok) {
      return { connected: true, count: 1, latencyMs };
    }

    // Direct Supabase fallback check
    const { data, error } = await supabase.from('interruptions').select('id').limit(1);
    if (!error) {
      return { connected: true, count: data?.length ?? 0, latencyMs: Date.now() - start };
    }
    return { connected: false, count: 0, latencyMs: Date.now() - start, error: error?.message || 'Network error' };
  } catch (err: any) {
    return { connected: false, count: 0, latencyMs: Date.now() - start, error: err?.message || 'Network error' };
  }
}

