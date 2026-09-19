// Client-side stub: In Option 2 (Server-Side Proxy Architecture),
// all database operations and Supabase calls are proxied through our full-stack Express server (/api/*)
// so that client browsers never directly contact *.supabase.co.

export const isSupabaseConfigured = true;

/**
 * Health check helper to verify backend connectivity via local API proxy
 */
export async function checkSupabaseHealth(): Promise<{ connected: boolean; count: number; latencyMs: number; error?: string }> {
  const start = Date.now();
  try {
    const res = await fetch('/api/health');
    const latencyMs = Date.now() - start;
    if (res.ok) {
      return { connected: true, count: 1, latencyMs };
    }
    return { connected: false, count: 0, latencyMs, error: `HTTP ${res.status}` };
  } catch (err: any) {
    return { connected: false, count: 0, latencyMs: Date.now() - start, error: err?.message || 'Network error' };
  }
}
