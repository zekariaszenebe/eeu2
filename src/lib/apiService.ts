import { FeederInterruption, SystemNotification, InterruptionStatus, InterruptionType, TeamLeaderNote, ContactItem, TeamLeaderUser } from '../types';
import { INITIAL_FEEDERS_LIST, INITIAL_CUSTOMER_CONTACTS } from '../data/mockData';
import { FEEDERS_VERSION } from '../data/feedersList';
import { HubRecord, HUB_RECORDS } from '../data/hubData';

export const DEFAULT_TEAM_LEADERS: TeamLeaderUser[] = [
  { id: 'admin-1', username: 'admin', password: '@Eeu1234', name: 'System Administrator', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() },
  { id: 'agent-1', username: 'contactcenter', password: '@Eeu1234', name: 'Contact Center Agent', district: 'Team A', role: 'agent', createdAt: new Date().toISOString() },
  { id: 'tl-1', username: 'teamleader', password: '@Eeu1234', name: 'Team Leader', district: 'Team D', role: 'team_leader', createdAt: new Date().toISOString() },
  { id: 'tl-d', username: 'zz01641821', password: 'eeu1234', name: 'Zekarias Zenebe', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() }
];

// Helper to notify UI if an error occurred
function notifyIfRlsError(table: string, error: any) {
  if (!error) return;
  console.warn(`[Proxy API ${table} Error]:`, error);
  const msg = (error.message || '').toLowerCase();
  if (
    error.code === '42501' ||
    msg.includes('row-level security') ||
    msg.includes('violates row-level security') ||
    msg.includes('permission denied') ||
    msg.includes('rls')
  ) {
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('supabase-rls-notice', {
        detail: { table, message: error.message }
      }));
    }
  }
}

// Local storage persistent fallback helpers
export function getLocal<T>(key: string, fallback: T): T {
  if (typeof window === 'undefined') return fallback;
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

export function setLocal<T>(key: string, data: T) {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(key, JSON.stringify(data));
  } catch {
    // ignore
  }
}

// Generic API caller with error handling
async function apiFetch<T>(endpoint: string, options?: RequestInit): Promise<T | null> {
  try {
    const res = await fetch(endpoint, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(options?.headers || {})
      }
    });
    if (!res.ok) {
      const errBody = await res.json().catch(() => ({}));
      throw new Error(errBody.error || `Request failed with status ${res.status}`);
    }
    return (await res.json()) as T;
  } catch (err) {
    console.warn(`API call to ${endpoint} failed:`, err);
    return null;
  }
}

// BroadcastChannel for instant local cross-tab communication
let localBroadcastChannel: BroadcastChannel | null = null;
if (typeof window !== 'undefined' && 'BroadcastChannel' in window) {
  try {
    localBroadcastChannel = new BroadcastChannel('eeu_local_sync_channel');
  } catch {
    localBroadcastChannel = null;
  }
}

// ==========================================
// SHARED REALTIME SYNC ENGINE (SERVER SSE STREAM PROXY)
// ==========================================
let sseSource: EventSource | null = null;
const syncListeners = {
  interruptions: new Set<() => void>(),
  notifications: new Set<() => void>(),
  teamLeaders: new Set<() => void>(),
  teamLeaderNotes: new Set<() => void>(),
  presetFeeders: new Set<() => void>(),
  hubRecords: new Set<() => void>(),
  customerContacts: new Set<() => void>(),
};

export function getSharedRealtimeChannel() {
  if (typeof window === 'undefined') return null;

  if (!sseSource) {
    try {
      // Connect to our own full-stack server SSE stream
      sseSource = new EventSource('/api/sync/stream');

      sseSource.addEventListener('sync', (e: MessageEvent) => {
        try {
          const payload = JSON.parse(e.data);
          const topic = payload?.topic as keyof typeof syncListeners;
          if (topic && syncListeners[topic]) {
            syncListeners[topic].forEach(cb => {
              try { cb(); } catch (err) { console.error('Sync listener error:', err); }
            });
          }
        } catch (err) {
          console.warn('SSE payload parse error:', err);
        }
      });

      sseSource.onerror = (err) => {
        // SSE automatically attempts reconnection per browser standard
        console.warn('SSE stream status update / reconnecting:', err);
      };
    } catch (err) {
      console.warn('Could not establish Server-Sent Events stream:', err);
    }

    if (localBroadcastChannel) {
      localBroadcastChannel.onmessage = (event) => {
        const topic = event.data?.topic as keyof typeof syncListeners;
        if (topic && syncListeners[topic]) {
          syncListeners[topic].forEach(cb => {
            try { cb(); } catch {}
          });
        }
      };
    }
  }

  return sseSource;
}

export function broadcastGlobalSync(topic: keyof typeof syncListeners, meta?: any) {
  try {
    if (syncListeners[topic]) {
      syncListeners[topic].forEach(cb => {
        try { cb(); } catch {}
      });
    }
    if (localBroadcastChannel) {
      localBroadcastChannel.postMessage({ topic, timestamp: Date.now(), ...meta });
    }
  } catch (e) {
    console.warn('broadcastGlobalSync error:', e);
  }
}

export async function seedInitialDataIfEmpty() {
  if (typeof window !== 'undefined') {
    const localTL = getLocal<TeamLeaderUser[]>('eeu-team-leaders', []);
    if (!localTL || localTL.length === 0) {
      setLocal('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
    }
    const localHub = getLocal<HubRecord[]>('eeu-hub-records', []);
    if (!localHub || localHub.length === 0) {
      setLocal('eeu-hub-records', HUB_RECORDS);
    }
    const localContacts = getLocal<ContactItem[]>('eeu-customer-contacts', []);
    if (!localContacts || localContacts.length === 0) {
      setLocal('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
    }
    const localFeeders = getLocal<string[]>('eeu-feeders-list-v4', []);
    if (!localFeeders || localFeeders.length === 0) {
      setLocal('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    }
  }

  // Pre-seed backend team leaders if needed
  try {
    const existing = await apiFetch<TeamLeaderUser[]>('/api/teamLeaders');
    if (!existing || existing.length === 0) {
      for (const tl of DEFAULT_TEAM_LEADERS) {
        await apiFetch('/api/teamLeaders', {
          method: 'POST',
          body: JSON.stringify(tl)
        });
      }
    }
  } catch {
    // Ignore initial seeding failures
  }

  const SEED_STORAGE_KEY = 'eeu-local-seeded-v7';
  if (typeof window !== 'undefined' && localStorage.getItem(SEED_STORAGE_KEY)) {
    return;
  }

  if (typeof window !== 'undefined') {
    if (!localStorage.getItem('eeu-feeders-list-v4')) {
      setLocal('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    }
    if (!localStorage.getItem('eeu-hub-records')) {
      setLocal('eeu-hub-records', HUB_RECORDS);
    }
    if (!localStorage.getItem('eeu-customer-contacts')) {
      setLocal('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
    }
    localStorage.setItem(SEED_STORAGE_KEY, 'true');
  }
}

// ==========================================
// 1. FEEDER INTERRUPTIONS (PROXIED VIA /api/interruptions)
// ==========================================

export function subscribeToInterruptions(onUpdate: (items: FeederInterruption[]) => void) {
  // 1. Immediately emit cached data for instantaneous 0ms render
  const cached = getLocal<FeederInterruption[]>('eeu-interruptions', []);
  if (cached && cached.length > 0) {
    onUpdate(cached);
  }

  const fetchBackend = async () => {
    try {
      const data = await apiFetch<FeederInterruption[]>('/api/interruptions');
      if (data && Array.isArray(data)) {
        onUpdate(data);
        setLocal('eeu-interruptions', data);
        return true;
      }
    } catch (err) {
      console.warn('Backend fetch error for interruptions:', err);
    }
    return false;
  };

  const fetchFallback = () => {
    onUpdate(getLocal('eeu-interruptions', []));
  };

  // Initial load
  fetchBackend().then(success => {
    if (!success) fetchFallback();
  });

  // Register on Shared Realtime Sync
  getSharedRealtimeChannel();
  const onSync = () => {
    fetchBackend();
  };
  syncListeners.interruptions.add(onSync);

  // Automatic refresh when switching back to tab
  const handleVisibility = () => {
    if (typeof document !== 'undefined' && document.visibilityState === 'visible') {
      fetchBackend();
    }
  };
  const handleFocus = () => {
    fetchBackend();
  };
  if (typeof window !== 'undefined') {
    window.addEventListener('visibilitychange', handleVisibility);
    window.addEventListener('focus', handleFocus);
  }

  // Periodic polling fallback every 4 seconds to guarantee freshness
  const interval = setInterval(() => {
    fetchBackend().then(success => {
      if (!success) fetchFallback();
    });
  }, 4000);

  return () => {
    syncListeners.interruptions.delete(onSync);
    clearInterval(interval);
    if (typeof window !== 'undefined') {
      window.removeEventListener('visibilitychange', handleVisibility);
      window.removeEventListener('focus', handleFocus);
    }
  };
}

export async function addInterruptionDoc(entry: Omit<FeederInterruption, 'id' | 'lastUpdated'>, customId?: string) {
  const suffix = Math.random().toString(36).substring(2, 9);
  const newId = customId || `f-${Date.now()}-${suffix}`;
  const timestampStr = new Date().toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
  });

  const record: FeederInterruption = {
    id: newId,
    feederName: entry.feederName || '',
    district: entry.district || 'Team A',
    direction: entry.direction,
    type: entry.type || InterruptionType.EARTH_FAULT,
    status: entry.status || InterruptionStatus.ACTIVE,
    startTime: entry.startTime || timestampStr,
    estimatedRestorationTime: entry.estimatedRestorationTime || 'N/A',
    affectedArea: entry.affectedArea || '',
    remark: entry.remark || '',
    lastUpdated: timestampStr
  };

  // 1. Update localStorage immediately for instant UI update
  try {
    const existing = getLocal<FeederInterruption[]>('eeu-interruptions', []);
    setLocal('eeu-interruptions', [record, ...existing.filter(i => i.id !== newId)]);
  } catch {}

  const notiId = `n-${Date.now()}-${suffix}`;
  const newNoti: SystemNotification = {
    id: notiId,
    feederId: newId,
    type: 'new',
    title: `New Feeder Added`,
    message: `${entry.feederName} (${entry.district}) logged under ${entry.status}. Affected areas: ${entry.affectedArea}`,
    timestamp: timestampStr,
    read: false
  };

  try {
    const existingNotis = getLocal<SystemNotification[]>('eeu-notifications', []);
    setLocal('eeu-notifications', [newNoti, ...existingNotis]);
  } catch {}

  // 2. Post to backend proxy
  const saved = await apiFetch<FeederInterruption>('/api/interruptions', {
    method: 'POST',
    body: JSON.stringify(record)
  });

  // Post notification
  apiFetch<SystemNotification>('/api/notifications', {
    method: 'POST',
    body: JSON.stringify(newNoti)
  }).catch(() => {});

  // Broadcast sync
  broadcastGlobalSync('interruptions');
  broadcastGlobalSync('notifications');

  return saved || record;
}

export async function updateInterruptionDoc(id: string, entry: Partial<FeederInterruption>, existingRecord?: FeederInterruption) {
  const timestampStr = new Date().toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
  });

  const merged = { ...existingRecord, ...entry, lastUpdated: timestampStr };

  // 1. Update localStorage immediately
  try {
    const existing = getLocal<FeederInterruption[]>('eeu-interruptions', []);
    setLocal('eeu-interruptions', existing.map(i => i.id === id ? { ...i, ...merged } : i));
  } catch {}

  let changeNoti: SystemNotification | null = null;
  if (entry.status && existingRecord?.status && entry.status !== existingRecord.status) {
    const typeVal = entry.status === InterruptionStatus.RESTORED ? 'resolve' : 'update';
    const titleText = entry.status === InterruptionStatus.RESTORED ? 'Feeder Line Restored' : 'Operational Status Changed';
    const messageText = entry.status === InterruptionStatus.RESTORED 
      ? `${merged.feederName} restored to active grid status and re-energized successfully.`
      : `${merged.feederName} reassessed as ${entry.status}. Details: ${entry.remark || merged.remark}`;

    const notiId = `n-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`;
    changeNoti = {
      id: notiId,
      feederId: id,
      type: typeVal,
      title: titleText,
      message: messageText,
      timestamp: timestampStr,
      read: false
    };

    try {
      const existingNotis = getLocal<SystemNotification[]>('eeu-notifications', []);
      setLocal('eeu-notifications', [changeNoti, ...existingNotis]);
    } catch {}
  }

  // 2. Update via server-side proxy
  const updatePayload: Record<string, any> = {
    lastUpdated: timestampStr
  };
  if (entry.status !== undefined) updatePayload.status = entry.status;
  if (entry.remark !== undefined) updatePayload.remark = entry.remark;
  if (entry.estimatedRestorationTime !== undefined) updatePayload.estimatedRestorationTime = entry.estimatedRestorationTime;
  if (entry.feederName !== undefined) updatePayload.feederName = entry.feederName;
  if (entry.district !== undefined) updatePayload.district = entry.district;
  if (entry.type !== undefined) updatePayload.type = entry.type;
  if (entry.startTime !== undefined) updatePayload.startTime = entry.startTime;
  if (entry.affectedArea !== undefined) updatePayload.affectedArea = entry.affectedArea;
  if (entry.direction !== undefined) updatePayload.direction = entry.direction;

  await apiFetch(`/api/interruptions/${id}`, {
    method: 'PUT',
    body: JSON.stringify(updatePayload)
  });

  if (changeNoti) {
    apiFetch('/api/notifications', {
      method: 'POST',
      body: JSON.stringify(changeNoti)
    }).catch(() => {});
    broadcastGlobalSync('notifications');
  }

  broadcastGlobalSync('interruptions');
}

export async function deleteInterruptionDoc(id: string) {
  try {
    const existing = getLocal<FeederInterruption[]>('eeu-interruptions', []);
    setLocal('eeu-interruptions', existing.filter(i => i.id !== id));
  } catch {}

  await apiFetch(`/api/interruptions/${id}`, {
    method: 'DELETE'
  });

  broadcastGlobalSync('interruptions');
}

// ==========================================
// 2. NOTIFICATIONS (PROXIED VIA /api/notifications)
// ==========================================

export function subscribeToNotifications(onUpdate: (items: SystemNotification[]) => void) {
  const cached = getLocal<SystemNotification[]>('eeu-notifications', []);
  if (cached && cached.length > 0) {
    onUpdate(cached);
  }

  const fetchBackend = async () => {
    try {
      const data = await apiFetch<SystemNotification[]>('/api/notifications');
      if (data && Array.isArray(data)) {
        onUpdate(data);
        setLocal('eeu-notifications', data);
        return true;
      }
    } catch {}
    return false;
  };

  const fetchFallback = () => {
    onUpdate(getLocal('eeu-notifications', []));
  };

  fetchBackend().then(success => {
    if (!success) fetchFallback();
  });

  getSharedRealtimeChannel();
  const onSync = () => {
    fetchBackend();
  };
  syncListeners.notifications.add(onSync);

  const handleVisibility = () => {
    if (typeof document !== 'undefined' && document.visibilityState === 'visible') {
      fetchBackend();
    }
  };
  if (typeof window !== 'undefined') {
    window.addEventListener('visibilitychange', handleVisibility);
  }

  const interval = setInterval(() => {
    fetchBackend().then(success => {
      if (!success) fetchFallback();
    });
  }, 4000);

  return () => {
    syncListeners.notifications.delete(onSync);
    clearInterval(interval);
    if (typeof window !== 'undefined') {
      window.removeEventListener('visibilitychange', handleVisibility);
    }
  };
}

export async function markAllNotificationsAsReadDoc() {
  try {
    const existing = getLocal<SystemNotification[]>('eeu-notifications', []);
    setLocal('eeu-notifications', existing.map(n => ({ ...n, read: true })));
  } catch {}

  await apiFetch('/api/notifications/read-all', {
    method: 'PUT'
  });

  broadcastGlobalSync('notifications');
}

export async function markOneNotificationAsReadDoc(id: string) {
  try {
    const existing = getLocal<SystemNotification[]>('eeu-notifications', []);
    setLocal('eeu-notifications', existing.map(n => n.id === id ? { ...n, read: true } : n));
  } catch {}

  await apiFetch(`/api/notifications/${id}/read`, {
    method: 'PUT'
  });

  broadcastGlobalSync('notifications');
}

export async function clearAllNotificationsDoc() {
  try {
    setLocal('eeu-notifications', []);
  } catch {}

  await apiFetch('/api/notifications', {
    method: 'DELETE'
  });

  broadcastGlobalSync('notifications');
}

// ==========================================
// 3. PRESET FEEDERS LIST
// ==========================================

export function subscribeToFeedersList(onUpdate: (items: string[]) => void) {
  const load = () => {
    const data = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    onUpdate(data);
  };
  load();

  // Also query server for preset feeders
  apiFetch<string[]>('/api/presetFeeders').then(feeders => {
    if (feeders && Array.isArray(feeders) && feeders.length > 0) {
      setLocal('eeu-feeders-list-v4', feeders);
      onUpdate(feeders);
    }
  }).catch(() => {});

  const handleStorage = (e: StorageEvent) => {
    if (e.key === 'eeu-feeders-list-v4') load();
  };
  const handleCustom = () => load();

  if (typeof window !== 'undefined') {
    window.addEventListener('storage', handleStorage);
    window.addEventListener('eeu-feeders-updated', handleCustom);
  }

  getSharedRealtimeChannel();
  const onSync = () => {
    apiFetch<string[]>('/api/presetFeeders').then(feeders => {
      if (feeders && Array.isArray(feeders) && feeders.length > 0) {
        setLocal('eeu-feeders-list-v4', feeders);
        onUpdate(feeders);
      }
    }).catch(() => {});
  };
  syncListeners.presetFeeders.add(onSync);

  return () => {
    syncListeners.presetFeeders.delete(onSync);
    if (typeof window !== 'undefined') {
      window.removeEventListener('storage', handleStorage);
      window.removeEventListener('eeu-feeders-updated', handleCustom);
    }
  };
}

export async function addPresetFeederDoc(feederStr: string) {
  try {
    const existing = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    const updated = Array.from(new Set([...existing, feederStr])).sort();
    setLocal('eeu-feeders-list-v4', updated);
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
    }
  } catch {}

  apiFetch('/api/presetFeeders', {
    method: 'POST',
    body: JSON.stringify({ feederStr })
  }).catch(() => {});
}

export async function deletePresetFeederDoc(feederStr: string) {
  try {
    const existing = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    const updated = existing.filter(f => f !== feederStr);
    setLocal('eeu-feeders-list-v4', updated);
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
    }
  } catch {}

  apiFetch(`/api/presetFeeders/${encodeURIComponent(feederStr)}`, {
    method: 'DELETE'
  }).catch(() => {});
}

export async function updatePresetFeederDoc(oldFeederStr: string, newFeederStr: string) {
  try {
    const existing = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    const updated = existing.map(f => f === oldFeederStr ? newFeederStr : f);
    setLocal('eeu-feeders-list-v4', updated);
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
    }
  } catch {}

  apiFetch('/api/presetFeeders', {
    method: 'PUT',
    body: JSON.stringify({ oldFeederStr, newFeederStr })
  }).catch(() => {});
}

export async function resetAllPresetFeedersToMaster() {
  try {
    setLocal('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
    }
  } catch {}

  apiFetch('/api/presetFeeders/bulk', {
    method: 'POST',
    body: JSON.stringify({ feeders: INITIAL_FEEDERS_LIST })
  }).catch(() => {});
}

// ==========================================
// 4. HUB RECORDS
// ==========================================

export function subscribeToHubRecords(onUpdate: (items: HubRecord[]) => void) {
  const load = () => {
    const stored = getLocal<HubRecord[]>('eeu-hub-records', HUB_RECORDS);
    const baseMap = new Map<number, HubRecord>();
    HUB_RECORDS.forEach(r => baseMap.set(r.no, { ...r }));
    if (Array.isArray(stored)) {
      stored.forEach(r => {
        if (r && typeof r.no === 'number') {
          const existing = baseMap.get(r.no);
          baseMap.set(r.no, existing ? { ...existing, ...r } : r);
        }
      });
    }
    const merged = Array.from(baseMap.values()).sort((a, b) => (a.no || 0) - (b.no || 0));
    onUpdate(merged);
  };

  load();

  // Also sync with server
  apiFetch<HubRecord[]>('/api/hubRecords').then(records => {
    if (records && Array.isArray(records) && records.length > 0) {
      setLocal('eeu-hub-records', records);
      onUpdate(records);
    }
  }).catch(() => {});

  const handleStorage = (e: StorageEvent) => {
    if (e.key === 'eeu-hub-records') load();
  };
  const handleCustom = () => load();

  if (typeof window !== 'undefined') {
    window.addEventListener('storage', handleStorage);
    window.addEventListener('eeu-hub-records-updated', handleCustom);
  }

  getSharedRealtimeChannel();
  const onSync = () => {
    apiFetch<HubRecord[]>('/api/hubRecords').then(records => {
      if (records && Array.isArray(records) && records.length > 0) {
        setLocal('eeu-hub-records', records);
        onUpdate(records);
      }
    }).catch(() => {});
  };
  syncListeners.hubRecords.add(onSync);

  return () => {
    syncListeners.hubRecords.delete(onSync);
    if (typeof window !== 'undefined') {
      window.removeEventListener('storage', handleStorage);
      window.removeEventListener('eeu-hub-records-updated', handleCustom);
    }
  };
}

export async function updateHubRecordDoc(record: HubRecord) {
  try {
    const stored = getLocal<HubRecord[]>('eeu-hub-records', HUB_RECORDS);
    const updated = stored.map(item => item.no === record.no ? { ...item, ...record } : item);
    setLocal('eeu-hub-records', updated);
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-hub-records-updated'));
    }
  } catch {}

  apiFetch(`/api/hubRecords/${record.no}`, {
    method: 'PUT',
    body: JSON.stringify(record)
  }).catch(() => {});
}

export async function resetHubRecordsToDefaultDoc() {
  setLocal('eeu-hub-records', HUB_RECORDS);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-hub-records-updated'));
  }

  apiFetch('/api/hubRecords/reset', {
    method: 'POST'
  }).catch(() => {});
}

// ==========================================
// 5. TEAM LEADER NOTES (PROXIED VIA /api/teamLeaderNotes)
// ==========================================

export function subscribeToTeamLeaderNotes(onUpdate: (items: TeamLeaderNote[]) => void) {
  const cached = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
  if (cached && cached.length > 0) {
    onUpdate(cached);
  }

  const fetchBackend = async () => {
    try {
      const data = await apiFetch<TeamLeaderNote[]>('/api/teamLeaderNotes');
      if (data && Array.isArray(data)) {
        onUpdate(data);
        setLocal('eeu-team-leader-notes', data);
        return true;
      }
    } catch {}
    return false;
  };

  const fetchFallback = () => {
    onUpdate(getLocal('eeu-team-leader-notes', []));
  };

  fetchBackend().then(success => {
    if (!success) fetchFallback();
  });

  getSharedRealtimeChannel();
  const onSync = () => {
    fetchBackend();
  };
  syncListeners.teamLeaderNotes.add(onSync);

  const interval = setInterval(() => {
    fetchBackend().then(success => {
      if (!success) fetchFallback();
    });
  }, 4000);

  return () => {
    syncListeners.teamLeaderNotes.delete(onSync);
    clearInterval(interval);
  };
}

export async function addTeamLeaderNoteDoc(content: string, author: string, isUrgent: boolean) {
  const cleanId = 'note-' + Date.now() + '-' + Math.random().toString(36).substring(2, 6);
  const timestampStr = new Date().toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
  });

  const record: TeamLeaderNote = { id: cleanId, content, author: author || "Team Leader", timestamp: timestampStr, isUrgent };
  try {
    const existing = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
    setLocal('eeu-team-leader-notes', [record, ...existing]);
  } catch {}

  const saved = await apiFetch<TeamLeaderNote>('/api/teamLeaderNotes', {
    method: 'POST',
    body: JSON.stringify(record)
  });

  broadcastGlobalSync('teamLeaderNotes');
  return saved || record;
}

export async function updateTeamLeaderNoteDoc(id: string, content: string, isUrgent: boolean) {
  const timestampStr = new Date().toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
  });
  try {
    const existing = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
    setLocal('eeu-team-leader-notes', existing.map(n => n.id === id ? { ...n, content, isUrgent, timestamp: timestampStr } : n));
  } catch {}

  await apiFetch(`/api/teamLeaderNotes/${id}`, {
    method: 'PUT',
    body: JSON.stringify({ content, isUrgent, timestamp: timestampStr })
  });

  broadcastGlobalSync('teamLeaderNotes');
}

export async function deleteTeamLeaderNoteDoc(id: string) {
  try {
    const existing = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
    setLocal('eeu-team-leader-notes', existing.filter(n => n.id !== id));
  } catch {}

  await apiFetch(`/api/teamLeaderNotes/${id}`, {
    method: 'DELETE'
  });

  broadcastGlobalSync('teamLeaderNotes');
}

export async function clearTeamLeaderNotes() {
  try {
    setLocal('eeu-team-leader-notes', []);
  } catch {}

  await apiFetch('/api/teamLeaderNotes', {
    method: 'DELETE'
  });

  broadcastGlobalSync('teamLeaderNotes');
}

// ==========================================
// 6. CUSTOMER CONTACTS
// ==========================================

export function subscribeToCustomerContacts(onUpdate: (items: ContactItem[]) => void) {
  const load = () => {
    const data = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
    data.sort((a: any, b: any) => {
      const catOrder: any = { 'head_regional': 0, 'sheger_city': 1, 'regional_hotline': 2 };
      const aOrder = catOrder[a.category] ?? 3;
      const bOrder = catOrder[b.category] ?? 3;
      if (aOrder !== bOrder) return aOrder - bOrder;
      return a.name.localeCompare(b.name);
    });
    onUpdate(data);
  };

  load();

  apiFetch<ContactItem[]>('/api/customerContacts').then(contacts => {
    if (contacts && Array.isArray(contacts) && contacts.length > 0) {
      setLocal('eeu-customer-contacts', contacts);
      load();
    }
  }).catch(() => {});

  const handleStorage = (e: StorageEvent) => {
    if (e.key === 'eeu-customer-contacts') load();
  };
  const handleCustom = () => load();

  if (typeof window !== 'undefined') {
    window.addEventListener('storage', handleStorage);
    window.addEventListener('eeu-customer-contacts-updated', handleCustom);
  }

  getSharedRealtimeChannel();
  const onSync = () => {
    apiFetch<ContactItem[]>('/api/customerContacts').then(contacts => {
      if (contacts && Array.isArray(contacts) && contacts.length > 0) {
        setLocal('eeu-customer-contacts', contacts);
        load();
      }
    }).catch(() => {});
  };
  syncListeners.customerContacts.add(onSync);

  return () => {
    syncListeners.customerContacts.delete(onSync);
    if (typeof window !== 'undefined') {
      window.removeEventListener('storage', handleStorage);
      window.removeEventListener('eeu-customer-contacts-updated', handleCustom);
    }
  };
}

export async function addCustomerContactDoc(item: Omit<ContactItem, 'id'>) {
  const newId = `cc-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`;
  const record: ContactItem = { ...item, id: newId };
  try {
    const existing = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
    setLocal('eeu-customer-contacts', [...existing, record]);
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-customer-contacts-updated'));
    }
  } catch {}

  apiFetch('/api/customerContacts', {
    method: 'POST',
    body: JSON.stringify(record)
  }).catch(() => {});

  return record;
}

export async function updateCustomerContactDoc(item: ContactItem) {
  try {
    const existing = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
    setLocal('eeu-customer-contacts', existing.map(c => c.id === item.id ? item : c));
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-customer-contacts-updated'));
    }
  } catch {}

  apiFetch(`/api/customerContacts/${item.id}`, {
    method: 'PUT',
    body: JSON.stringify(item)
  }).catch(() => {});
}

export async function deleteCustomerContactDoc(id: string) {
  try {
    const existing = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
    setLocal('eeu-customer-contacts', existing.filter(c => c.id !== id));
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-customer-contacts-updated'));
    }
  } catch {}

  apiFetch(`/api/customerContacts/${id}`, {
    method: 'DELETE'
  }).catch(() => {});
}

// ==========================================
// 7. TEAM LEADERS & USERS (PROXIED VIA /api/teamLeaders)
// ==========================================

export function subscribeToTeamLeaders(onUpdate: (items: TeamLeaderUser[]) => void) {
  const getInitial = (): TeamLeaderUser[] => {
    const stored = getLocal<TeamLeaderUser[]>('eeu-team-leaders', []);
    if (!stored || stored.length === 0) {
      setLocal('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
      return DEFAULT_TEAM_LEADERS;
    }
    return stored;
  };

  // Immediate cached render
  const initial = getInitial();
  onUpdate(initial);

  const fetchBackend = async () => {
    try {
      const data = await apiFetch<TeamLeaderUser[]>('/api/teamLeaders');
      if (data && Array.isArray(data) && data.length > 0) {
        data.sort((a: any, b: any) => (a.name || '').localeCompare(b.name || ''));
        onUpdate(data);
        setLocal('eeu-team-leaders', data);
        return true;
      }
    } catch {}
    return false;
  };

  const fetchFallback = () => {
    const fallback = getInitial();
    fallback.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    onUpdate(fallback);
  };

  fetchBackend().then(success => {
    if (!success) fetchFallback();
  });

  getSharedRealtimeChannel();
  const onSync = () => {
    fetchBackend();
  };
  syncListeners.teamLeaders.add(onSync);

  const interval = setInterval(() => {
    fetchBackend().then(success => {
      if (!success) fetchFallback();
    });
  }, 4000);

  return () => {
    syncListeners.teamLeaders.delete(onSync);
    clearInterval(interval);
  };
}

export async function addTeamLeaderDoc(item: Omit<TeamLeaderUser, 'id' | 'createdAt'>) {
  const newId = `tl-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`;
  const record: TeamLeaderUser = {
    id: newId,
    username: item.username.trim(),
    password: item.password.trim(),
    name: item.name.trim(),
    district: item.district || 'Team A',
    role: item.role || 'team_leader',
    createdAt: new Date().toISOString()
  };
  if (typeof item.mustChangePassword === 'boolean') record.mustChangePassword = item.mustChangePassword;

  // 1. Local storage immediate save
  try {
    const existing = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
    const updated = [...existing.filter(tl => tl.id !== record.id), record];
    setLocal('eeu-team-leaders', updated);
  } catch (err) {
    console.error('Failed to save team leader to localStorage:', err);
  }

  // 2. Server proxy save
  const saved = await apiFetch<TeamLeaderUser>('/api/teamLeaders', {
    method: 'POST',
    body: JSON.stringify(record)
  });

  broadcastGlobalSync('teamLeaders');
  return saved || record;
}

export async function updateTeamLeaderDoc(item: TeamLeaderUser) {
  const record: TeamLeaderUser = {
    id: item.id,
    username: item.username.trim(),
    password: item.password.trim(),
    name: item.name.trim(),
    district: item.district || 'Team A',
    role: item.role || 'team_leader',
    createdAt: item.createdAt || new Date().toISOString()
  };
  if (typeof item.mustChangePassword === 'boolean') record.mustChangePassword = item.mustChangePassword;

  try {
    const existing = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
    const updated = existing.map(tl => tl.id === item.id ? { ...tl, ...record } : tl);
    setLocal('eeu-team-leaders', updated);
  } catch (err) {
    console.error('Failed to update team leader in localStorage:', err);
  }

  await apiFetch(`/api/teamLeaders/${item.id}`, {
    method: 'PUT',
    body: JSON.stringify(record)
  });

  broadcastGlobalSync('teamLeaders');
}

export async function deleteTeamLeaderDoc(id: string) {
  try {
    const existing = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
    const updated = existing.filter(tl => tl.id !== id);
    setLocal('eeu-team-leaders', updated);
  } catch (err) {
    console.error('Failed to delete team leader from localStorage:', err);
  }

  await apiFetch(`/api/teamLeaders/${id}`, {
    method: 'DELETE'
  });

  broadcastGlobalSync('teamLeaders');
}

export interface FeedbackRecord {
  id: string;
  rating: number;
  category: string;
  feedbackText: string;
  submittedBy: string;
  targetEmail: string;
  timestamp: string;
}

export async function addFeedbackDoc(feedback: {
  rating: number;
  category: string;
  feedbackText: string;
  submittedBy: string;
  targetEmail: string;
}): Promise<FeedbackRecord> {
  const newId = `fb_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
  const record: FeedbackRecord = {
    id: newId,
    rating: feedback.rating,
    category: feedback.category,
    feedbackText: feedback.feedbackText.trim(),
    submittedBy: feedback.submittedBy.trim(),
    targetEmail: feedback.targetEmail.trim(),
    timestamp: new Date().toISOString()
  };
  try {
    const existing = getLocal<FeedbackRecord[]>('eeu-feedback-records', []);
    setLocal('eeu-feedback-records', [record, ...existing]);
  } catch {}

  apiFetch('/api/feedbacks', {
    method: 'POST',
    body: JSON.stringify(record)
  }).catch(() => {});

  return record;
}
