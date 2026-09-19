import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.VITE_SUPABASE_URL || 'https://lwgprtxopdonxtmjmgqm.supabase.co';
const SUPABASE_ANON_KEY = process.env.VITE_SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx3Z3BydHhvcGRvbnh0bWptZ3FtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODc3Nzc2MzAsImV4cCI6MjEwMzM1MzYzMH0.3Zpj--H_E3A8lpI2UyjB3Nkh3-xbmLXOEwHu8lrXE-o';

export const isServerSupabaseConfigured = Boolean(
  SUPABASE_URL &&
  SUPABASE_ANON_KEY &&
  !SUPABASE_URL.includes('your-project') &&
  !SUPABASE_ANON_KEY.includes('your-anon-key')
);

export const supabaseServer = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
  },
  realtime: {
    params: {
      eventsPerSecond: 10,
    },
  },
});
