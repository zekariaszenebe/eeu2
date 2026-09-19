import express from "express";
import path from "path";
import { createServer as createViteServer } from "vite";
import { requireAuth, AuthRequest } from "./src/middleware/auth.ts";
import { getOrCreateUser, getUsers } from "./src/db/users.ts";
import { supabaseServer, isServerSupabaseConfigured } from "./src/server/supabaseServer.ts";
import {
  getInterruptionsRepo,
  addInterruptionRepo,
  updateInterruptionRepo,
  deleteInterruptionRepo,
  getNotificationsRepo,
  addNotificationRepo,
  markNotificationReadRepo,
  markAllNotificationsReadRepo,
  clearAllNotificationsRepo,
  getPresetFeedersRepo,
  addPresetFeederRepo,
  bulkSetPresetFeedersRepo,
  deletePresetFeederRepo,
  updatePresetFeederRepo,
  getHubRecordsRepo,
  updateHubRecordRepo,
  bulkUpdateHubRecordsRepo,
  resetHubRecordsRepo,
  getTeamLeaderNotesRepo,
  addTeamLeaderNoteRepo,
  updateTeamLeaderNoteRepo,
  deleteTeamLeaderNoteRepo,
  clearTeamLeaderNotesRepo,
  getCustomerContactsRepo,
  addCustomerContactRepo,
  updateCustomerContactRepo,
  deleteCustomerContactRepo,
  getTeamLeadersRepo,
  addTeamLeaderRepo,
  updateTeamLeaderRepo,
  deleteTeamLeaderRepo,
  addFeedbackRepo
} from "./src/db/repository.ts";

// Set of connected SSE clients
const sseClients = new Set<express.Response>();

export function broadcastSse(topic: string, data?: any) {
  const payload = JSON.stringify({ topic, data, timestamp: Date.now() });
  for (const client of sseClients) {
    try {
      client.write(`event: sync\ndata: ${payload}\n\n`);
    } catch {
      sseClients.delete(client);
    }
  }
}

async function safeSupabaseAction(action: () => PromiseLike<any>) {
  if (!isServerSupabaseConfigured) return;
  try {
    const res = await action();
    return res;
  } catch (err) {
    // Ignore background sync errors
  }
}

// Track tables missing from remote Supabase schema cache
const missingSupabaseTables = new Set<string>();

function handleSupabaseTableError(tableName: string, error: any) {
  if (!error) return;
  const msg = (error.message || '').toLowerCase();
  if (error.code === 'PGRST205' || msg.includes('could not find the table') || msg.includes('schema cache')) {
    if (!missingSupabaseTables.has(tableName)) {
      missingSupabaseTables.add(tableName);
    }
  }
}

// ========================================================
// SERVER MASTER IN-MEMORY PERSISTENCE STORE
// Ensures instant multi-agent sync across all connected browsers
// ========================================================
const serverInterruptions = new Map<string, any>();
const serverNotifications = new Map<string, any>();
const serverTeamLeaders = new Map<string, any>();
const serverTeamLeaderNotes = new Map<string, any>();
const serverPresetFeeders = new Set<string>();
const serverHubRecords = new Map<number, any>();
const serverCustomerContacts = new Map<string, any>();

// Seed default team leaders
const DEFAULT_LEADERS = [
  { id: 'admin-1', username: 'admin', password: '@Eeu1234', name: 'System Administrator', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() },
  { id: 'agent-1', username: 'contactcenter', password: '@Eeu1234', name: 'Contact Center Agent', district: 'Team A', role: 'agent', createdAt: new Date().toISOString() },
  { id: 'tl-1', username: 'teamleader', password: '@Eeu1234', name: 'Team Leader', district: 'Team D', role: 'team_leader', createdAt: new Date().toISOString() },
  { id: 'tl-d', username: 'zz01641821', password: 'eeu1234', name: 'Zekarias Zenebe', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() }
];
for (const tl of DEFAULT_LEADERS) {
  serverTeamLeaders.set(tl.id, tl);
}

// Setup Supabase Realtime listener on the server to bridge updates to SSE clients
if (isServerSupabaseConfigured) {
  try {
    supabaseServer
      .channel("server-supabase-bridge")
      .on("postgres_changes", { event: "*", schema: "public", table: "interruptions" }, (payload) => {
        if (payload.new && (payload.new as any).id) {
          serverInterruptions.set((payload.new as any).id, payload.new);
        } else if (payload.eventType === 'DELETE' && payload.old && (payload.old as any).id) {
          serverInterruptions.delete((payload.old as any).id);
        }
        broadcastSse("interruptions", payload);
      })
      .on("postgres_changes", { event: "*", schema: "public", table: "notifications" }, (payload) => {
        if (payload.new && (payload.new as any).id) {
          serverNotifications.set((payload.new as any).id, payload.new);
        }
        broadcastSse("notifications", payload);
      })
      .on("postgres_changes", { event: "*", schema: "public", table: "teamLeaders" }, (payload) => {
        if (payload.new && (payload.new as any).id) {
          serverTeamLeaders.set((payload.new as any).id, payload.new);
        }
        broadcastSse("teamLeaders", payload);
      })
      .on("postgres_changes", { event: "*", schema: "public", table: "teamLeaderNotes" }, (payload) => {
        if (payload.new && (payload.new as any).id) {
          serverTeamLeaderNotes.set((payload.new as any).id, payload.new);
        }
        broadcastSse("teamLeaderNotes", payload);
      })
      .subscribe();
  } catch (err) {
    // Ignore realtime attach issues
  }
}

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(express.json());

  // === Health Check ===
  app.get("/api/health", (req, res) => {
    res.json({ status: "ok", supabaseConfigured: isServerSupabaseConfigured });
  });

  // === Real-time SSE Stream (Option 2 Proxy) ===
  app.get("/api/sync/stream", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    if (res.flushHeaders) {
      res.flushHeaders();
    }

    sseClients.add(res);

    // Initial greeting / handshake event
    res.write(`event: connected\ndata: ${JSON.stringify({ status: "connected", timestamp: Date.now() })}\n\n`);

    req.on("close", () => {
      sseClients.delete(res);
    });
  });

  // Regular heartbeat to keep SSE connection alive across reverse proxies
  setInterval(() => {
    for (const client of sseClients) {
      try {
        client.write(": keepalive\n\n");
      } catch {
        sseClients.delete(client);
      }
    }
  }, 15000);

  // === Users / Firebase Auth Sync ===
  app.post("/api/auth/sync", requireAuth, async (req: AuthRequest, res) => {
    try {
      if (!req.user || !req.user.uid) {
        return res.status(400).json({ error: "Missing authenticated user" });
      }
      const user = await getOrCreateUser(req.user.uid, req.user.email || '', req.body.name);
      res.json(user);
    } catch (error: any) {
      console.error("Failed to sync user:", error);
      res.status(500).json({ error: error.message || "Failed to sync user" });
    }
  });

  app.get("/api/users", requireAuth, async (req: AuthRequest, res) => {
    try {
      const users = await getUsers();
      res.json(users);
    } catch (error: any) {
      console.error("Failed to fetch users:", error);
      res.status(500).json({ error: error.message || "Failed to fetch users" });
    }
  });

  // === 1. Interruptions (Server-side Supabase Proxy with Master Store fallback) ===
  app.get("/api/interruptions", async (req, res) => {
    try {
      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        const { data, error } = await supabaseServer
          .from("interruptions")
          .select("*")
          .order("id", { ascending: false });

        if (!error && Array.isArray(data)) {
          for (const item of data) {
            if (item && item.id) {
              serverInterruptions.set(item.id, item);
            }
          }
        } else if (error) {
          handleSupabaseTableError("interruptions", error);
        }
      }

      // Try Cloud SQL if available
      try {
        const sqlList = await getInterruptionsRepo();
        if (Array.isArray(sqlList)) {
          for (const item of sqlList) {
            if (item && item.id && !serverInterruptions.has(item.id)) {
              serverInterruptions.set(item.id, item);
            }
          }
        }
      } catch {}

      const list = Array.from(serverInterruptions.values()).sort((a: any, b: any) => {
        const tA = a.createdAt ? new Date(a.createdAt).getTime() : (a.lastUpdated ? new Date(a.lastUpdated).getTime() : 0);
        const tB = b.createdAt ? new Date(b.createdAt).getTime() : (b.lastUpdated ? new Date(b.lastUpdated).getTime() : 0);
        if (!isNaN(tA) && !isNaN(tB) && tA !== tB) return tB - tA;
        return (b.id || "").localeCompare(a.id || "");
      });
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch interruptions:", error);
      const list = Array.from(serverInterruptions.values());
      res.json(list);
    }
  });

  // Bulk sync endpoint to reconcile client and server state
  app.post("/api/interruptions/sync", async (req, res) => {
    try {
      const { items } = req.body || {};
      if (Array.isArray(items)) {
        for (const item of items) {
          if (item && item.id) {
            if (!serverInterruptions.has(item.id)) {
              serverInterruptions.set(item.id, item);
              try { addInterruptionRepo(item).catch(() => {}); } catch {}
            }
          }
        }
      }
      const list = Array.from(serverInterruptions.values()).sort((a: any, b: any) => {
        const tA = a.createdAt ? new Date(a.createdAt).getTime() : (a.lastUpdated ? new Date(a.lastUpdated).getTime() : 0);
        const tB = b.createdAt ? new Date(b.createdAt).getTime() : (b.lastUpdated ? new Date(b.lastUpdated).getTime() : 0);
        if (!isNaN(tA) && !isNaN(tB) && tA !== tB) return tB - tA;
        return (b.id || "").localeCompare(a.id || "");
      });
      broadcastSse("interruptions", { synced: true, count: list.length });
      res.json(list);
    } catch (error: any) {
      console.error("Failed to sync interruptions:", error);
      res.json(Array.from(serverInterruptions.values()));
    }
  });

  app.post("/api/interruptions", async (req, res) => {
    const id = req.body.id || `f-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const createdAt = req.body.createdAt || new Date().toISOString();
    const lastUpdated = req.body.lastUpdated || new Date().toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    });
    const item = { ...req.body, id, createdAt, lastUpdated };
    try {
      // 1. Instantly store in Master Server Memory Map for guaranteed 100% sync
      serverInterruptions.set(id, item);

      // 2. Also create notification on server
      const notiId = `n-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`;
      const noti = {
        id: notiId,
        feederId: id,
        type: 'new',
        title: 'New Feeder Added',
        message: `${item.feederName} (${item.district || 'Team A'}) logged under ${item.status || 'Active'}. Affected areas: ${item.affectedArea || 'N/A'}`,
        timestamp: lastUpdated,
        read: false
      };
      serverNotifications.set(notiId, noti);

      // 3. Write to Supabase asynchronously
      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        safeSupabaseAction(async () => {
          const { error } = await supabaseServer.from("interruptions").insert(item).select().single();
          if (error) {
            if (error.message && error.message.toLowerCase().includes("direction")) {
              const { direction, ...compatRecord } = item;
              await supabaseServer.from("interruptions").insert(compatRecord);
            } else {
              handleSupabaseTableError("interruptions", error);
            }
          }
        });

        safeSupabaseAction(() => supabaseServer.from("notifications").insert(noti));
      }

      // 4. Write to Cloud SQL repo in background
      try {
        addInterruptionRepo(item).catch(() => {});
        addNotificationRepo(noti).catch(() => {});
      } catch {}

      // 5. Broadcast real-time update to all connected browser SSE clients
      broadcastSse("interruptions", item);
      broadcastSse("notifications", noti);

      res.json(item);
    } catch (error: any) {
      console.error("Failed to add interruption:", error);
      res.json(item);
    }
  });

  app.put("/api/interruptions/:id", async (req, res) => {
    const id = req.params.id;
    const updatePayload = { ...req.body };
    try {
      const existing = serverInterruptions.get(id) || {};
      const updatedRecord = { ...existing, ...updatePayload, id };
      serverInterruptions.set(id, updatedRecord);

      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        safeSupabaseAction(async () => {
          const { error } = await supabaseServer.from("interruptions").update(updatePayload).eq("id", id);
          if (error) {
            if (error.message && error.message.toLowerCase().includes("direction")) {
              const { direction, ...compatPayload } = updatePayload;
              await supabaseServer.from("interruptions").update(compatPayload).eq("id", id);
            } else {
              handleSupabaseTableError("interruptions", error);
            }
          }
        });
      }

      try {
        updateInterruptionRepo(id, updatePayload).catch(() => {});
      } catch {}

      broadcastSse("interruptions", updatedRecord);
      res.json(updatedRecord);
    } catch (error: any) {
      console.error("Failed to update interruption:", error);
      res.status(500).json({ error: error.message || "Failed to update interruption" });
    }
  });

  app.delete("/api/interruptions/:id", async (req, res) => {
    const id = req.params.id;
    try {
      serverInterruptions.delete(id);

      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        safeSupabaseAction(async () => {
          const { error } = await supabaseServer.from("interruptions").delete().eq("id", id);
          if (error) handleSupabaseTableError("interruptions", error);
        });
      }

      try {
        deleteInterruptionRepo(id).catch(() => {});
      } catch {}

      broadcastSse("interruptions", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete interruption:", error);
      res.json({ success: true, id });
    }
  });

  // === 2. Notifications (Server-side Supabase Proxy with Master Store) ===
  app.get("/api/notifications", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer
          .from("notifications")
          .select("*")
          .order("timestamp", { ascending: false });

        if (!error && Array.isArray(data)) {
          for (const noti of data) {
            if (noti && noti.id) serverNotifications.set(noti.id, noti);
          }
        }
      }

      try {
        const list = await getNotificationsRepo();
        if (Array.isArray(list)) {
          for (const noti of list) {
            if (noti && noti.id && !serverNotifications.has(noti.id)) {
              serverNotifications.set(noti.id, noti);
            }
          }
        }
      } catch {}

      const list = Array.from(serverNotifications.values()).sort(
        (a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime()
      );
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch notifications:", error);
      res.json(Array.from(serverNotifications.values()));
    }
  });

  app.post("/api/notifications", async (req, res) => {
    const id = req.body.id || `n-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const timestamp = req.body.timestamp || new Date().toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    });
    const item = { read: false, ...req.body, id, timestamp };
    try {
      serverNotifications.set(id, item);

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("notifications").insert(item));
      }

      try {
        addNotificationRepo(item).catch(() => {});
      } catch {}

      broadcastSse("notifications", item);
      res.json(item);
    } catch (error: any) {
      console.error("Failed to add notification:", error);
      res.json(item);
    }
  });

  app.put("/api/notifications/:id/read", async (req, res) => {
    const id = req.params.id;
    try {
      const existing = serverNotifications.get(id);
      if (existing) {
        serverNotifications.set(id, { ...existing, read: true });
      }

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("notifications").update({ read: true }).eq("id", id));
      }
      try {
        markNotificationReadRepo(id).catch(() => {});
      } catch {}

      broadcastSse("notifications", { id, read: true });
      res.json({ success: true, id, read: true });
    } catch (error: any) {
      res.json({ success: true, id, read: true });
    }
  });

  app.put("/api/notifications/:id", async (req, res) => {
    const id = req.params.id;
    try {
      const existing = serverNotifications.get(id) || {};
      const updated = { ...existing, ...req.body, id };
      serverNotifications.set(id, updated);

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("notifications").update(req.body).eq("id", id));
      }
      try {
        if (req.body.read) markNotificationReadRepo(id).catch(() => {});
      } catch {}

      broadcastSse("notifications", updated);
      res.json({ success: true, id, ...req.body });
    } catch (error: any) {
      res.json({ success: true, id, ...req.body });
    }
  });

  app.put("/api/notifications/read-all", async (req, res) => {
    try {
      for (const [id, noti] of serverNotifications.entries()) {
        serverNotifications.set(id, { ...noti, read: true });
      }

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("notifications").update({ read: true }).neq("id", ""));
      }
      try {
        markAllNotificationsReadRepo().catch(() => {});
      } catch {}

      broadcastSse("notifications", { allRead: true });
      res.json({ success: true });
    } catch (error: any) {
      res.json({ success: true });
    }
  });

  app.delete("/api/notifications/:id", async (req, res) => {
    const id = req.params.id;
    try {
      serverNotifications.delete(id);
      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("notifications").delete().eq("id", id));
      }
      broadcastSse("notifications", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      res.json({ success: true, id });
    }
  });

  app.delete("/api/notifications", async (req, res) => {
    try {
      serverNotifications.clear();
      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("notifications").delete().neq("id", ""));
      }
      try {
        clearAllNotificationsRepo().catch(() => {});
      } catch {}

      broadcastSse("notifications", { cleared: true });
      res.json({ success: true });
    } catch (error: any) {
      res.json({ success: true });
    }
  });

  // === 3. Team Leaders (Server-side Supabase Proxy with Master Store) ===
  app.get("/api/teamLeaders", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaders").select("*");
        if (!error && Array.isArray(data)) {
          for (const tl of data) {
            if (tl && tl.id) serverTeamLeaders.set(tl.id, tl);
          }
        }
      }

      try {
        const list = await getTeamLeadersRepo();
        if (Array.isArray(list)) {
          for (const tl of list) {
            if (tl && tl.id && !serverTeamLeaders.has(tl.id)) {
              serverTeamLeaders.set(tl.id, tl);
            }
          }
        }
      } catch {}

      const list = Array.from(serverTeamLeaders.values()).sort((a: any, b: any) =>
        (a.name || "").localeCompare(b.name || "")
      );
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch team leaders:", error);
      res.json(Array.from(serverTeamLeaders.values()));
    }
  });

  app.post("/api/teamLeaders", async (req, res) => {
    const id = req.body.id || `tl-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const item = { createdAt: new Date().toISOString(), ...req.body, id };
    try {
      serverTeamLeaders.set(id, item);

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaders").insert(item));
      }
      try {
        addTeamLeaderRepo(item).catch(() => {});
      } catch {}

      broadcastSse("teamLeaders", item);
      res.json(item);
    } catch (error: any) {
      console.error("Failed to add team leader:", error);
      res.json(item);
    }
  });

  app.put("/api/teamLeaders/:id", async (req, res) => {
    const id = req.params.id;
    const item = { ...req.body };
    try {
      const existing = serverTeamLeaders.get(id) || {};
      const updated = { ...existing, ...item, id };
      serverTeamLeaders.set(id, updated);

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaders").update(item).eq("id", id));
      }
      try {
        updateTeamLeaderRepo(id, item).catch(() => {});
      } catch {}

      broadcastSse("teamLeaders", updated);
      res.json(updated);
    } catch (error: any) {
      console.error("Failed to update team leader:", error);
      res.status(500).json({ error: error.message || "Failed to update team leader" });
    }
  });

  app.delete("/api/teamLeaders/:id", async (req, res) => {
    const id = req.params.id;
    try {
      serverTeamLeaders.delete(id);
      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaders").delete().eq("id", id));
      }
      try {
        deleteTeamLeaderRepo(id).catch(() => {});
      } catch {}

      broadcastSse("teamLeaders", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete team leader:", error);
      res.json({ success: true, id });
    }
  });

  // === 4. Team Leader Notes (Server-side Supabase Proxy with Master Store) ===
  app.get("/api/teamLeaderNotes", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaderNotes").select("*");
        if (!error && Array.isArray(data)) {
          for (const note of data) {
            if (note && note.id) serverTeamLeaderNotes.set(note.id, note);
          }
        }
      }

      try {
        const list = await getTeamLeaderNotesRepo();
        if (Array.isArray(list)) {
          for (const note of list) {
            if (note && note.id && !serverTeamLeaderNotes.has(note.id)) {
              serverTeamLeaderNotes.set(note.id, note);
            }
          }
        }
      } catch {}

      const list = Array.from(serverTeamLeaderNotes.values()).sort(
        (a: any, b: any) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime()
      );
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch team leader notes:", error);
      res.json(Array.from(serverTeamLeaderNotes.values()));
    }
  });

  app.post("/api/teamLeaderNotes", async (req, res) => {
    const id = req.body.id || `note-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const timestamp = req.body.timestamp || new Date().toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    });
    const item = { isUrgent: false, ...req.body, id, timestamp };
    try {
      serverTeamLeaderNotes.set(id, item);

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaderNotes").insert(item));
      }
      try {
        addTeamLeaderNoteRepo(item).catch(() => {});
      } catch {}

      broadcastSse("teamLeaderNotes", item);
      res.json(item);
    } catch (error: any) {
      console.error("Failed to add team leader note:", error);
      res.json(item);
    }
  });

  app.put("/api/teamLeaderNotes/:id", async (req, res) => {
    const id = req.params.id;
    const item = { ...req.body };
    try {
      const existing = serverTeamLeaderNotes.get(id) || {};
      const updated = { ...existing, ...item, id };
      serverTeamLeaderNotes.set(id, updated);

      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaderNotes").update(item).eq("id", id));
      }
      try {
        updateTeamLeaderNoteRepo(id, item).catch(() => {});
      } catch {}

      broadcastSse("teamLeaderNotes", updated);
      res.json(updated);
    } catch (error: any) {
      console.error("Failed to update team leader note:", error);
      res.status(500).json({ error: error.message || "Failed to update team leader note" });
    }
  });

  app.delete("/api/teamLeaderNotes/:id", async (req, res) => {
    const id = req.params.id;
    try {
      serverTeamLeaderNotes.delete(id);
      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaderNotes").delete().eq("id", id));
      }
      try {
        deleteTeamLeaderNoteRepo(id).catch(() => {});
      } catch {}

      broadcastSse("teamLeaderNotes", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete team leader note:", error);
      res.status(500).json({ error: error.message || "Failed to delete team leader note" });
    }
  });

  app.delete("/api/teamLeaderNotes", async (req, res) => {
    try {
      serverTeamLeaderNotes.clear();
      if (isServerSupabaseConfigured) {
        safeSupabaseAction(() => supabaseServer.from("teamLeaderNotes").delete().neq("id", ""));
      }
      try {
        clearTeamLeaderNotesRepo().catch(() => {});
      } catch {}

      broadcastSse("teamLeaderNotes", { cleared: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to clear team leader notes:", error);
      res.status(500).json({ error: error.message || "Failed to clear team leader notes" });
    }
  });

  // === Preset Feeders ===
  app.get("/api/presetFeeders", async (req, res) => {
    try {
      const list = await getPresetFeedersRepo();
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch preset feeders:", error);
      res.status(500).json({ error: error.message || "Failed to fetch preset feeders" });
    }
  });

  app.post("/api/presetFeeders", async (req, res) => {
    try {
      const item = await addPresetFeederRepo(req.body.feederStr);
      broadcastSse("presetFeeders", item);
      res.json(item);
    } catch (error: any) {
      console.error("Failed to add preset feeder:", error);
      res.status(500).json({ error: error.message || "Failed to add preset feeder" });
    }
  });

  app.post("/api/presetFeeders/bulk", async (req, res) => {
    try {
      await bulkSetPresetFeedersRepo(req.body.feeders || []);
      broadcastSse("presetFeeders", { bulk: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to bulk update preset feeders:", error);
      res.status(500).json({ error: error.message || "Failed to bulk update preset feeders" });
    }
  });

  app.delete("/api/presetFeeders/:feederStr", async (req, res) => {
    try {
      await deletePresetFeederRepo(req.params.feederStr);
      broadcastSse("presetFeeders", { deleted: req.params.feederStr });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to delete preset feeder:", error);
      res.status(500).json({ error: error.message || "Failed to delete preset feeder" });
    }
  });

  app.put("/api/presetFeeders", async (req, res) => {
    try {
      await updatePresetFeederRepo(req.body.oldFeederStr, req.body.newFeederStr);
      broadcastSse("presetFeeders", { updated: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to update preset feeder:", error);
      res.status(500).json({ error: error.message || "Failed to update preset feeder" });
    }
  });

  // === Hub Records ===
  app.get("/api/hubRecords", async (req, res) => {
    try {
      const list = await getHubRecordsRepo();
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch hub records:", error);
      res.status(500).json({ error: error.message || "Failed to fetch hub records" });
    }
  });

  app.put("/api/hubRecords/:no", async (req, res) => {
    try {
      const updated = await updateHubRecordRepo(Number(req.params.no), req.body);
      broadcastSse("hubRecords", updated);
      res.json(updated);
    } catch (error: any) {
      console.error("Failed to update hub record:", error);
      res.status(500).json({ error: error.message || "Failed to update hub record" });
    }
  });

  app.post("/api/hubRecords/bulk", async (req, res) => {
    try {
      await bulkUpdateHubRecordsRepo(req.body.records || []);
      broadcastSse("hubRecords", { bulk: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to bulk update hub records:", error);
      res.status(500).json({ error: error.message || "Failed to bulk update hub records" });
    }
  });

  app.post("/api/hubRecords/reset", async (req, res) => {
    try {
      const records = await resetHubRecordsRepo();
      broadcastSse("hubRecords", { reset: true });
      res.json({ success: true, records });
    } catch (error: any) {
      console.error("Failed to reset hub records:", error);
      res.status(500).json({ error: error.message || "Failed to reset hub records" });
    }
  });

  // === Customer Contacts ===
  app.get("/api/customerContacts", async (req, res) => {
    try {
      const list = await getCustomerContactsRepo();
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch customer contacts:", error);
      res.status(500).json({ error: error.message || "Failed to fetch customer contacts" });
    }
  });

  app.post("/api/customerContacts", async (req, res) => {
    try {
      const item = await addCustomerContactRepo(req.body);
      broadcastSse("customerContacts", item);
      res.json(item);
    } catch (error: any) {
      console.error("Failed to add customer contact:", error);
      res.status(500).json({ error: error.message || "Failed to add customer contact" });
    }
  });

  app.put("/api/customerContacts/:id", async (req, res) => {
    try {
      const updated = await updateCustomerContactRepo(req.params.id, req.body);
      broadcastSse("customerContacts", updated);
      if (updated) {
        res.json(updated);
      } else {
        res.status(404).json({ error: "Not found" });
      }
    } catch (error: any) {
      console.error("Failed to update customer contact:", error);
      res.status(500).json({ error: error.message || "Failed to update customer contact" });
    }
  });

  app.delete("/api/customerContacts/:id", async (req, res) => {
    try {
      await deleteCustomerContactRepo(req.params.id);
      broadcastSse("customerContacts", { id: req.params.id, deleted: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to delete customer contact:", error);
      res.status(500).json({ error: error.message || "Failed to delete customer contact" });
    }
  });

  // === Feedbacks ===
  app.post("/api/feedbacks", async (req, res) => {
    try {
      const item = await addFeedbackRepo(req.body);
      res.json(item);
    } catch (error: any) {
      console.error("Failed to add feedback:", error);
      res.status(500).json({ error: error.message || "Failed to add feedback" });
    }
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
