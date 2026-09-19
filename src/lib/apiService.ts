import { FeederInterruption, SystemNotification, InterruptionStatus, InterruptionType, TeamLeaderNote, ContactItem, TeamLeaderUser } from '../types';
import { INITIAL_FEEDERS_LIST, INITIAL_CUSTOMER_CONTACTS } from '../data/mockData';
import { HubRecord, HUB_RECORDS } from '../data/hubData';

export const DEFAULT_TEAM_LEADERS: TeamLeaderUser[] = [
  { id: 'admin-1', username: 'admin', password: '@Eeu1234', name: 'System Administrator', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() },
  { id: 'agent-1', username: 'contactcenter', password: '@Eeu1234', name: 'Contact Center Agent', district: 'Team A', role: 'agent', createdAt: new Date().toISOString() },
  { id: 'tl-1', username: 'teamleader', password: '@Eeu1234', name: 'Team Leader', district: 'Team D', role: 'team_leader', createdAt: new Date().toISOString() },
  { id: 'tl-d', username: 'zz01641821', password: 'eeu1234', name: 'Zekarias Zenebe', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() }
];

export const isStaticEnvironment = typeof window !== 'undefined' && (
  window.location.hostname.includes('github.io') ||
  window.location.hostname.includes('vercel.app') ||
  window.location.hostname.includes('netlify.app') ||
  window.location.protocol === 'file:'
);

let serverProxyAvailable: boolean = !isStaticEnvironment;

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

// Generic API caller
async function apiFetch<T>(endpoint: string, options?: RequestInit): Promise<T | null> {
  if (!serverProxyAvailable) return null;
  try {
    const res = await fetch(endpoint, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(options?.headers || {})
      }
    });
    if (res.status === 404) {
      serverProxyAvailable = false;
      return null;
    }
    if (!res.ok) {
      const errBody = await res.json().catch(() => ({}));
      throw new Error(errBody.error || `Request failed with status ${res.status}`);
    }
    return (await res.json()) as T;
  } catch {
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

const syncListeners = {
  interruptions: new Set<() => void>(),
  notifications: new Set<() => void>(),
  teamLeaders: new Set<() => void>(),
  teamLeaderNotes: new Set<() => void>(),
  presetFeeders: new Set<() => void>(),
  hubRecords: new Set<() => void>(),
  customerContacts: new Set<() => void>(),
};

let sseSource: EventSource | null = null;

export function getSharedRealtimeChannel() {
  if (typeof window === 'undefined') return null;

  // Connect to SSE stream
  if (serverProxyAvailable && !sseSource) {
    try {
      sseSource = new EventSource('/api/sync/stream');

      sseSource.addEventListener('sync', (e: MessageEvent) => {
        try {
          const payload = JSON.parse(e.data);
          const topic = payload?.topic as keyof typeof syncListeners;
          if (topic && syncListeners[topic]) {
            syncListeners[topic].forEach(cb => {
              try { cb(); } catch {}
            });
          }
        } catch {}
      });

      sseSource.onerror = () => {
        // SSE auto-reconnects
      };
    } catch {
      // ignore
    }
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

  if (serverProxyAvailable) {
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
    } catch {}
  }
}

let detectedTeamLeadersTable: 'teamLeaders' | 'team_leaders' | null = null;
let detectedNotesTable: 'teamLeaderNotes' | 'team_leader_notes' | null = null;


// ==========================================
// 1. FEEDER INTERRUPTIONS
// ==========================================

export async function fetchInterruptions(): Promise<FeederInterruption[]> {
  // 1. Try server proxy if available
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<FeederInterruption[]>('/api/interruptions');
      if (data && Array.isArray(data)) {
        setLocal('eeu-interruptions', data);
        return data;
      }
    } catch {}
  }

  // 2. LocalStorage fallback
  return getLocal<FeederInterruption[]>('eeu-interruptions', []);
}

export async function syncInterruptionsWithServer(localItems: FeederInterruption[]): Promise<FeederInterruption[]> {
  if (!Array.isArray(localItems) || localItems.length === 0) {
    return fetchInterruptions();
  }
  if (serverProxyAvailable) {
    try {
      const res = await apiFetch<FeederInterruption[]>('/api/interruptions/sync', {
        method: 'POST',
        body: JSON.stringify({ items: localItems })
      });
      if (res && Array.isArray(res)) {
        setLocal('eeu-interruptions', res);
        return res;
      }
    } catch {}
  }
  return localItems;
}

export async function manualRefreshAllData(): Promise<{ success: boolean; count: number }> {
  try {
    const interruptions = await fetchInterruptions();
    const notifications = await fetchNotifications();
    const teamLeaders = await fetchTeamLeaders();
    const notes = await fetchTeamLeaderNotes();

    broadcastGlobalSync('interruptions');
    broadcastGlobalSync('notifications');
    broadcastGlobalSync('teamLeaders');
    broadcastGlobalSync('teamLeaderNotes');

    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('eeu-manual-refresh-done', { 
        detail: { count: (interruptions || []).length, timestamp: Date.now() } 
      }));
    }

    return { success: true, count: (interruptions || []).length };
  } catch (err) {
    console.warn('manualRefreshAllData error:', err);
    return { success: false, count: 0 };
  }
}

export function subscribeToInterruptions(onUpdate: (items: FeederInterruption[]) => void) {
  const cached = getLocal<FeederInterruption[]>('eeu-interruptions', []);
  if (cached && cached.length > 0) {
    onUpdate(cached);
  }

  const doFetch = async () => {
    const data = await fetchInterruptions();
    if (data && Array.isArray(data)) {
      onUpdate(data);
    }
  };

  doFetch();

  getSharedRealtimeChannel();
  const onSync = () => {
    doFetch();
  };
  syncListeners.interruptions.add(onSync);

  const handleVisibility = () => {
    if (typeof document !== 'undefined' && document.visibilityState === 'visible') {
      doFetch();
    }
  };
  if (typeof window !== 'undefined') {
    window.addEventListener('visibilitychange', handleVisibility);
    window.addEventListener('focus', doFetch);
  }

  const interval = setInterval(doFetch, 3000);

  return () => {
    syncListeners.interruptions.delete(onSync);
    clearInterval(interval);
    if (typeof window !== 'undefined') {
      window.removeEventListener('visibilitychange', handleVisibility);
      window.removeEventListener('focus', doFetch);
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

  // 1. LocalStorage Immediate
  const existing = getLocal<FeederInterruption[]>('eeu-interruptions', []);
  setLocal('eeu-interruptions', [record, ...existing.filter(i => i.id !== newId)]);

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

  const existingNotis = getLocal<SystemNotification[]>('eeu-notifications', []);
  setLocal('eeu-notifications', [newNoti, ...existingNotis]);

  // 2. Dual Write: Server Proxy
  if (serverProxyAvailable) {
    apiFetch<FeederInterruption>('/api/interruptions', {
      method: 'POST',
      body: JSON.stringify(record)
    }).catch(() => {});
    apiFetch('/api/notifications', {
      method: 'POST',
      body: JSON.stringify(newNoti)
    }).catch(() => {});
  }

  broadcastGlobalSync('interruptions');
  broadcastGlobalSync('notifications');
  return record;
}

export async function updateInterruptionDoc(id: string, entry: Partial<FeederInterruption>, existingRecord?: FeederInterruption) {
  const timestampStr = new Date().toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
  });

  const merged = { ...existingRecord, ...entry, lastUpdated: timestampStr };

  // 1. Update localStorage
  const existing = getLocal<FeederInterruption[]>('eeu-interruptions', []);
  setLocal('eeu-interruptions', existing.map(i => i.id === id ? { ...i, ...merged } : i));

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

    const existingNotis = getLocal<SystemNotification[]>('eeu-notifications', []);
    setLocal('eeu-notifications', [changeNoti, ...existingNotis]);
  }

  // 2. Dual Write: Server proxy
  if (serverProxyAvailable) {
    const updatePayload: Record<string, any> = { lastUpdated: timestampStr };
    if (entry.status !== undefined) updatePayload.status = entry.status;
    if (entry.remark !== undefined) updatePayload.remark = entry.remark;
    if (entry.estimatedRestorationTime !== undefined) updatePayload.estimatedRestorationTime = entry.estimatedRestorationTime;
    if (entry.feederName !== undefined) updatePayload.feederName = entry.feederName;
    if (entry.district !== undefined) updatePayload.district = entry.district;
    if (entry.type !== undefined) updatePayload.type = entry.type;
    if (entry.startTime !== undefined) updatePayload.startTime = entry.startTime;
    if (entry.affectedArea !== undefined) updatePayload.affectedArea = entry.affectedArea;
    if (entry.direction !== undefined) updatePayload.direction = entry.direction;

    apiFetch(`/api/interruptions/${id}`, {
      method: 'PUT',
      body: JSON.stringify(updatePayload)
    }).catch(() => {});

    if (changeNoti) {
      apiFetch('/api/notifications', {
        method: 'POST',
        body: JSON.stringify(changeNoti)
      }).catch(() => {});
      broadcastGlobalSync('notifications');
    }
  }

  broadcastGlobalSync('interruptions');
}

export async function deleteInterruptionDoc(id: string) {
  const existing = getLocal<FeederInterruption[]>('eeu-interruptions', []);
  setLocal('eeu-interruptions', existing.filter(i => i.id !== id));

  if (serverProxyAvailable) {
    apiFetch(`/api/interruptions/${id}`, { method: 'DELETE' }).catch(() => {});
  }

  broadcastGlobalSync('interruptions');
}

// ==========================================
// 2. NOTIFICATIONS
// ==========================================

export async function fetchNotifications(): Promise<SystemNotification[]> {
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<SystemNotification[]>('/api/notifications');
      if (data && Array.isArray(data)) {
        setLocal('eeu-notifications', data);
        return data;
      }
    } catch {}
  }

  return getLocal<SystemNotification[]>('eeu-notifications', []);
}

export function subscribeToNotifications(onUpdate: (items: SystemNotification[]) => void) {
  const cached = getLocal<SystemNotification[]>('eeu-notifications', []);
  if (cached && cached.length > 0) {
    onUpdate(cached);
  }

  const doFetch = async () => {
    const data = await fetchNotifications();
    if (data && Array.isArray(data)) {
      onUpdate(data);
    }
  };

  doFetch();

  getSharedRealtimeChannel();
  const onSync = () => {
    doFetch();
  };
  syncListeners.notifications.add(onSync);

  const handleVisibility = () => {
    if (typeof document !== 'undefined' && document.visibilityState === 'visible') {
      doFetch();
    }
  };
  if (typeof window !== 'undefined') {
    window.addEventListener('visibilitychange', handleVisibility);
  }

  const interval = setInterval(doFetch, 5000);

  return () => {
    syncListeners.notifications.delete(onSync);
    clearInterval(interval);
    if (typeof window !== 'undefined') {
      window.removeEventListener('visibilitychange', handleVisibility);
    }
  };
}

export async function markAllNotificationsAsReadDoc() {
  const existing = getLocal<SystemNotification[]>('eeu-notifications', []);
  setLocal('eeu-notifications', existing.map(n => ({ ...n, read: true })));

  if (serverProxyAvailable) {
    apiFetch('/api/notifications/read-all', { method: 'PUT' }).catch(() => {});
  }

  broadcastGlobalSync('notifications');
}

export async function markOneNotificationAsReadDoc(id: string) {
  const existing = getLocal<SystemNotification[]>('eeu-notifications', []);
  setLocal('eeu-notifications', existing.map(n => n.id === id ? { ...n, read: true } : n));

  if (serverProxyAvailable) {
    apiFetch(`/api/notifications/${id}/read`, { method: 'PUT' }).catch(() => {});
  }

  broadcastGlobalSync('notifications');
}

export async function clearAllNotificationsDoc() {
  setLocal('eeu-notifications', []);

  if (serverProxyAvailable) {
    apiFetch('/api/notifications', { method: 'DELETE' }).catch(() => {});
  }

  broadcastGlobalSync('notifications');
}

// ==========================================
// 3. PRESET FEEDERS LIST
// ==========================================

export async function fetchPresetFeeders(): Promise<string[]> {
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<string[]>('/api/presetFeeders');
      if (data && Array.isArray(data) && data.length > 0) {
        setLocal('eeu-feeders-list-v4', data);
        return data;
      }
    } catch {}
  }

  return getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
}

export function subscribeToFeedersList(onUpdate: (items: string[]) => void) {
  const load = () => {
    const data = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
    onUpdate(data);
  };
  load();

  fetchPresetFeeders().then(feeders => {
    if (feeders && Array.isArray(feeders) && feeders.length > 0) {
      onUpdate(feeders);
    }
  });

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
    fetchPresetFeeders().then(feeders => {
      if (feeders && Array.isArray(feeders) && feeders.length > 0) {
        onUpdate(feeders);
      }
    });
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
  const existing = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
  const updated = Array.from(new Set([...existing, feederStr])).sort();
  setLocal('eeu-feeders-list-v4', updated);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch('/api/presetFeeders', {
      method: 'POST',
      body: JSON.stringify({ feederStr })
    }).catch(() => {});
  }
}

export async function deletePresetFeederDoc(feederStr: string) {
  const existing = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
  const updated = existing.filter(f => f !== feederStr);
  setLocal('eeu-feeders-list-v4', updated);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch(`/api/presetFeeders/${encodeURIComponent(feederStr)}`, {
      method: 'DELETE'
    }).catch(() => {});
  }
}

export async function updatePresetFeederDoc(oldFeederStr: string, newFeederStr: string) {
  const existing = getLocal<string[]>('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
  const updated = existing.map(f => f === oldFeederStr ? newFeederStr : f);
  setLocal('eeu-feeders-list-v4', updated);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch('/api/presetFeeders', {
      method: 'PUT',
      body: JSON.stringify({ oldFeederStr, newFeederStr })
    }).catch(() => {});
  }
}

export async function resetAllPresetFeedersToMaster() {
  setLocal('eeu-feeders-list-v4', INITIAL_FEEDERS_LIST);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-feeders-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch('/api/presetFeeders/bulk', {
      method: 'POST',
      body: JSON.stringify({ feeders: INITIAL_FEEDERS_LIST })
    }).catch(() => {});
  }
}

// ==========================================
// 4. HUB RECORDS
// ==========================================

export async function fetchHubRecords(): Promise<HubRecord[]> {
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<HubRecord[]>('/api/hubRecords');
      if (data && Array.isArray(data) && data.length > 0) {
        setLocal('eeu-hub-records', data);
        return data;
      }
    } catch {}
  }

  return getLocal<HubRecord[]>('eeu-hub-records', HUB_RECORDS);
}

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

  fetchHubRecords().then(records => {
    if (records && Array.isArray(records) && records.length > 0) {
      load();
    }
  });

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
    fetchHubRecords().then(() => load());
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
  const stored = getLocal<HubRecord[]>('eeu-hub-records', HUB_RECORDS);
  const updated = stored.map(item => item.no === record.no ? { ...item, ...record } : item);
  setLocal('eeu-hub-records', updated);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-hub-records-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch(`/api/hubRecords/${record.no}`, {
      method: 'PUT',
      body: JSON.stringify(record)
    }).catch(() => {});
  }
}

export async function resetHubRecordsToDefaultDoc() {
  setLocal('eeu-hub-records', HUB_RECORDS);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-hub-records-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch('/api/hubRecords/reset', { method: 'POST' }).catch(() => {});
  }
}

// ==========================================
// 5. TEAM LEADER NOTES
// ==========================================

export async function fetchTeamLeaderNotes(): Promise<TeamLeaderNote[]> {
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<TeamLeaderNote[]>('/api/teamLeaderNotes');
      if (data && Array.isArray(data)) {
        setLocal('eeu-team-leader-notes', data);
        return data;
      }
    } catch {}
  }

  return getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
}

export function subscribeToTeamLeaderNotes(onUpdate: (items: TeamLeaderNote[]) => void) {
  const cached = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
  if (cached && cached.length > 0) {
    onUpdate(cached);
  }

  const doFetch = async () => {
    const data = await fetchTeamLeaderNotes();
    if (data && Array.isArray(data)) {
      onUpdate(data);
    }
  };

  doFetch();

  getSharedRealtimeChannel();
  const onSync = () => {
    doFetch();
  };
  syncListeners.teamLeaderNotes.add(onSync);

  const interval = setInterval(doFetch, 4000);

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
  const existing = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
  setLocal('eeu-team-leader-notes', [record, ...existing]);

  if (serverProxyAvailable) {
    apiFetch<TeamLeaderNote>('/api/teamLeaderNotes', {
      method: 'POST',
      body: JSON.stringify(record)
    }).catch(() => {});
  }

  broadcastGlobalSync('teamLeaderNotes');
  return record;
}

export async function updateTeamLeaderNoteDoc(id: string, content: string, isUrgent: boolean) {
  const timestampStr = new Date().toLocaleString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
  });
  const existing = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
  setLocal('eeu-team-leader-notes', existing.map(n => n.id === id ? { ...n, content, isUrgent, timestamp: timestampStr } : n));

  if (serverProxyAvailable) {
    apiFetch(`/api/teamLeaderNotes/${id}`, {
      method: 'PUT',
      body: JSON.stringify({ content, isUrgent, timestamp: timestampStr })
    }).catch(() => {});
  }

  broadcastGlobalSync('teamLeaderNotes');
}

export async function deleteTeamLeaderNoteDoc(id: string) {
  const existing = getLocal<TeamLeaderNote[]>('eeu-team-leader-notes', []);
  setLocal('eeu-team-leader-notes', existing.filter(n => n.id !== id));

  if (serverProxyAvailable) {
    apiFetch(`/api/teamLeaderNotes/${id}`, { method: 'DELETE' }).catch(() => {});
  }

  broadcastGlobalSync('teamLeaderNotes');
}

export async function clearTeamLeaderNotes() {
  setLocal('eeu-team-leader-notes', []);

  if (serverProxyAvailable) {
    apiFetch('/api/teamLeaderNotes', { method: 'DELETE' }).catch(() => {});
  }

  broadcastGlobalSync('teamLeaderNotes');
}

// ==========================================
// 6. CUSTOMER CONTACTS
// ==========================================

export async function fetchCustomerContacts(): Promise<ContactItem[]> {
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<ContactItem[]>('/api/customerContacts');
      if (data && Array.isArray(data) && data.length > 0) {
        setLocal('eeu-customer-contacts', data);
        return data;
      }
    } catch {}
  }

  if (supabase) {
    try {
      const { data, error } = await supabase.from('customer_contacts').select('*');
      if (!error && Array.isArray(data) && data.length > 0) {
        const normalized = data.map((c: any) => ({
          id: c.id,
          name: c.name,
          phone: c.phone,
          category: c.category,
          locationInfo: c.locationInfo || c.location_info,
          hotlineShortCode: c.hotlineShortCode || c.hotline_short_code
        }));
        setLocal('eeu-customer-contacts', normalized);
        return normalized;
      }
    } catch {}
  }

  return getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
}

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

  fetchCustomerContacts().then(() => load());

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
    fetchCustomerContacts().then(() => load());
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
  const existing = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
  setLocal('eeu-customer-contacts', [...existing, record]);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-customer-contacts-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch('/api/customerContacts', {
      method: 'POST',
      body: JSON.stringify(record)
    }).catch(() => {});
  }
  if (supabase) {
    safeSupa(() => supabase.from('customer_contacts').upsert({
      id: record.id,
      name: record.name,
      phone: record.phone,
      category: record.category,
      location_info: record.locationInfo,
      hotline_short_code: record.hotlineShortCode
    }));
  }

  return record;
}

export async function updateCustomerContactDoc(item: ContactItem) {
  const existing = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
  setLocal('eeu-customer-contacts', existing.map(c => c.id === item.id ? item : c));
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-customer-contacts-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch(`/api/customerContacts/${item.id}`, {
      method: 'PUT',
      body: JSON.stringify(item)
    }).catch(() => {});
  }
  if (supabase) {
    safeSupa(() => supabase.from('customer_contacts').upsert({
      id: item.id,
      name: item.name,
      phone: item.phone,
      category: item.category,
      location_info: item.locationInfo,
      hotline_short_code: item.hotlineShortCode
    }));
  }
}

export async function deleteCustomerContactDoc(id: string) {
  const existing = getLocal<ContactItem[]>('eeu-customer-contacts', INITIAL_CUSTOMER_CONTACTS);
  setLocal('eeu-customer-contacts', existing.filter(c => c.id !== id));
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent('eeu-customer-contacts-updated'));
  }

  if (serverProxyAvailable) {
    apiFetch(`/api/customerContacts/${id}`, { method: 'DELETE' }).catch(() => {});
  }
  if (supabase) {
    safeSupa(() => supabase.from('customer_contacts').delete().eq('id', id));
  }
}

// ==========================================
// 7. TEAM LEADERS & USERS
// ==========================================

export async function fetchTeamLeaders(): Promise<TeamLeaderUser[]> {
  if (serverProxyAvailable) {
    try {
      const data = await apiFetch<TeamLeaderUser[]>('/api/teamLeaders');
      if (data && Array.isArray(data) && data.length > 0) {
        data.sort((a: any, b: any) => (a.name || '').localeCompare(b.name || ''));
        setLocal('eeu-team-leaders', data);
        return data;
      }
    } catch {}
  }

  if (supabase) {
    try {
      let res = await supabase.from('teamLeaders').select('*');
      if (res.error) {
        res = await supabase.from('team_leaders').select('*');
      }
      if (!res.error && Array.isArray(res.data) && res.data.length > 0) {
        const normalized = res.data.map(normalizeTeamLeader);
        normalized.sort((a: any, b: any) => (a.name || '').localeCompare(b.name || ''));
        setLocal('eeu-team-leaders', normalized);
        return normalized;
      }
    } catch {}
  }

  const stored = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
  return stored.length > 0 ? stored : DEFAULT_TEAM_LEADERS;
}

export function subscribeToTeamLeaders(onUpdate: (items: TeamLeaderUser[]) => void) {
  const getInitial = (): TeamLeaderUser[] => {
    const stored = getLocal<TeamLeaderUser[]>('eeu-team-leaders', []);
    if (!stored || stored.length === 0) {
      setLocal('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
      return DEFAULT_TEAM_LEADERS;
    }
    return stored;
  };

  const initial = getInitial();
  onUpdate(initial);

  const doFetch = async () => {
    const leaders = await fetchTeamLeaders();
    if (leaders && Array.isArray(leaders) && leaders.length > 0) {
      onUpdate(leaders);
    }
  };

  doFetch();

  getSharedRealtimeChannel();
  const onSync = () => {
    doFetch();
  };
  syncListeners.teamLeaders.add(onSync);

  const interval = setInterval(doFetch, 4000);

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

  const existing = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
  const updated = [...existing.filter(tl => tl.id !== record.id), record];
  setLocal('eeu-team-leaders', updated);

  if (serverProxyAvailable) {
    apiFetch<TeamLeaderUser>('/api/teamLeaders', {
      method: 'POST',
      body: JSON.stringify(record)
    }).catch(() => {});
  }
  if (supabase) {
    safeSupa(async () => {
      const { error } = await supabase.from('teamLeaders').upsert({
        id: record.id,
        username: record.username,
        password: record.password,
        name: record.name,
        district: record.district,
        role: record.role,
        mustChangePassword: record.mustChangePassword,
        createdAt: record.createdAt
      });
      if (error) {
        try {
          await supabase.from('team_leaders').upsert({
            id: record.id,
            username: record.username,
            password: record.password,
            name: record.name,
            district: record.district,
            role: record.role,
            must_change_password: record.mustChangePassword,
            created_at: record.createdAt
          });
        } catch {}
      }
    });
  }

  broadcastGlobalSync('teamLeaders');
  return record;
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

  const existing = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
  const updated = existing.map(tl => tl.id === item.id ? { ...tl, ...record } : tl);
  setLocal('eeu-team-leaders', updated);

  if (serverProxyAvailable) {
    apiFetch(`/api/teamLeaders/${item.id}`, {
      method: 'PUT',
      body: JSON.stringify(record)
    }).catch(() => {});
  }
  if (supabase) {
    safeSupa(async () => {
      const { error } = await supabase.from('teamLeaders').upsert({
        id: record.id,
        username: record.username,
        password: record.password,
        name: record.name,
        district: record.district,
        role: record.role,
        mustChangePassword: record.mustChangePassword,
        createdAt: record.createdAt
      });
      if (error) {
        try {
          await supabase.from('team_leaders').upsert({
            id: record.id,
            username: record.username,
            password: record.password,
            name: record.name,
            district: record.district,
            role: record.role,
            must_change_password: record.mustChangePassword,
            created_at: record.createdAt
          });
        } catch {}
      }
    });
  }

  broadcastGlobalSync('teamLeaders');
}

export async function deleteTeamLeaderDoc(id: string) {
  const existing = getLocal<TeamLeaderUser[]>('eeu-team-leaders', DEFAULT_TEAM_LEADERS);
  const updated = existing.filter(tl => tl.id !== id);
  setLocal('eeu-team-leaders', updated);

  if (serverProxyAvailable) {
    apiFetch(`/api/teamLeaders/${id}`, { method: 'DELETE' }).catch(() => {});
  }
  if (supabase) {
    safeSupa(async () => {
      const { error } = await supabase.from('teamLeaders').delete().eq('id', id);
      if (error) {
        try {
          await supabase.from('team_leaders').delete().eq('id', id);
        } catch {}
      }
    });
  }

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

  const existing = getLocal<FeedbackRecord[]>('eeu-feedback-records', []);
  setLocal('eeu-feedback-records', [record, ...existing]);

  if (serverProxyAvailable) {
    apiFetch('/api/feedbacks', {
      method: 'POST',
      body: JSON.stringify(record)
    }).catch(() => {});
  }
  if (supabase) {
    safeSupa(() => supabase.from('feedbacks').upsert({
      id: record.id,
      rating: record.rating,
      category: record.category,
      feedback_text: record.feedbackText,
      submitted_by: record.submittedBy,
      target_email: record.targetEmail,
      timestamp: record.timestamp
    }));
  }

  return record;
}
