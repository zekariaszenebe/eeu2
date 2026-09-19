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

// Setup Supabase Realtime listener on the server to bridge updates to SSE clients
if (isServerSupabaseConfigured) {
  try {
    supabaseServer
      .channel("server-supabase-bridge")
      .on("postgres_changes", { event: "*", schema: "public", table: "interruptions" }, (payload) => {
        broadcastSse("interruptions", payload);
      })
      .on("postgres_changes", { event: "*", schema: "public", table: "notifications" }, (payload) => {
        broadcastSse("notifications", payload);
      })
      .on("postgres_changes", { event: "*", schema: "public", table: "teamLeaders" }, (payload) => {
        broadcastSse("teamLeaders", payload);
      })
      .on("postgres_changes", { event: "*", schema: "public", table: "teamLeaderNotes" }, (payload) => {
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

  // === 1. Interruptions (Server-side Supabase Proxy with Cloud SQL fallback) ===
  app.get("/api/interruptions", async (req, res) => {
    try {
      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        const { data, error } = await supabaseServer
          .from("interruptions")
          .select("*")
          .order("id", { ascending: false });

        if (!error && Array.isArray(data) && data.length > 0) {
          return res.json(data);
        }
        if (error) {
          handleSupabaseTableError("interruptions", error);
        }
      }

      // Fallback to Cloud SQL
      const list = await getInterruptionsRepo();
      res.json(list.sort((a, b) => new Date(b.lastUpdated).getTime() - new Date(a.lastUpdated).getTime()));
    } catch (error: any) {
      console.error("Failed to fetch interruptions:", error);
      try {
        const list = await getInterruptionsRepo();
        res.json(list);
      } catch {
        res.status(500).json({ error: error.message || "Failed to fetch interruptions" });
      }
    }
  });

  app.post("/api/interruptions", async (req, res) => {
    const id = req.body.id || `f-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const lastUpdated = req.body.lastUpdated || new Date().toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    });
    const item = { ...req.body, id, lastUpdated };
    try {
      let savedRecord = item;

      // 1. Write to Supabase if configured
      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        let { data, error } = await supabaseServer.from("interruptions").insert(item).select().single();
        if (error && error.message && error.message.toLowerCase().includes("direction")) {
          const { direction, ...compatRecord } = item;
          const retry = await supabaseServer.from("interruptions").insert(compatRecord).select().single();
          error = retry.error;
          if (retry.data) data = retry.data;
        }
        if (error) {
          handleSupabaseTableError("interruptions", error);
        } else if (data) {
          savedRecord = data;
        }
      }

      // 2. Also persist to Cloud SQL repo for durability
      try {
        await addInterruptionRepo(savedRecord);
      } catch (sqlErr) {
        // Cloud SQL sync
      }

      // 3. Broadcast real-time update to all connected browser SSE clients
      broadcastSse("interruptions", savedRecord);

      res.json(savedRecord);
    } catch (error: any) {
      console.error("Failed to add interruption:", error);
      res.status(500).json({ error: error.message || "Failed to add interruption" });
    }
  });

  app.put("/api/interruptions/:id", async (req, res) => {
    const id = req.params.id;
    const updatePayload = { ...req.body };
    try {
      let updatedRecord = { id, ...updatePayload };

      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        let { data, error } = await supabaseServer
          .from("interruptions")
          .update(updatePayload)
          .eq("id", id)
          .select()
          .single();

        if (error && error.message && error.message.toLowerCase().includes("direction")) {
          const { direction, ...compatPayload } = updatePayload;
          const retry = await supabaseServer
            .from("interruptions")
            .update(compatPayload)
            .eq("id", id)
            .select()
            .single();
          error = retry.error;
          if (retry.data) data = retry.data;
        }

        if (error) {
          handleSupabaseTableError("interruptions", error);
        } else if (data) {
          updatedRecord = data;
        }
      }

      try {
        const repoUpd = await updateInterruptionRepo(id, updatePayload);
        if (repoUpd && !isServerSupabaseConfigured) updatedRecord = repoUpd;
      } catch (sqlErr) {
        // Cloud SQL sync
      }

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
      if (isServerSupabaseConfigured && !missingSupabaseTables.has("interruptions")) {
        const { error } = await supabaseServer.from("interruptions").delete().eq("id", id);
        if (error) {
          handleSupabaseTableError("interruptions", error);
        }
      }

      try {
        await deleteInterruptionRepo(id);
      } catch (sqlErr) {
        // Cloud SQL sync
      }

      broadcastSse("interruptions", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete interruption:", error);
      res.status(500).json({ error: error.message || "Failed to delete interruption" });
    }
  });

  // === 2. Notifications (Server-side Supabase Proxy) ===
  app.get("/api/notifications", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer
          .from("notifications")
          .select("*")
          .order("timestamp", { ascending: false });

        if (!error && Array.isArray(data)) {
          return res.json(data);
        }
      }

      const list = await getNotificationsRepo();
      res.json(list.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()));
    } catch (error: any) {
      console.error("Failed to fetch notifications:", error);
      try {
        const list = await getNotificationsRepo();
        res.json(list);
      } catch {
        res.status(500).json({ error: error.message || "Failed to fetch notifications" });
      }
    }
  });

  app.post("/api/notifications", async (req, res) => {
    const id = req.body.id || `n-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const timestamp = req.body.timestamp || new Date().toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    });
    const item = { read: false, ...req.body, id, timestamp };
    try {
      let saved = item;
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("notifications").insert(item).select().single();
        if (!error && data) saved = data;
      }

      try {
        await addNotificationRepo(saved);
      } catch (sqlErr) {
        console.warn("Cloud SQL notification sync note:", sqlErr);
      }

      broadcastSse("notifications", saved);
      res.json(saved);
    } catch (error: any) {
      console.error("Failed to add notification:", error);
      res.status(500).json({ error: error.message || "Failed to add notification" });
    }
  });

  app.put("/api/notifications/:id/read", async (req, res) => {
    const id = req.params.id;
    try {
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("notifications").update({ read: true }).eq("id", id);
      }
      try {
        await markNotificationReadRepo(id);
      } catch {}

      broadcastSse("notifications", { id, read: true });
      res.json({ success: true, id, read: true });
    } catch (error: any) {
      console.error("Failed to mark notification read:", error);
      res.status(500).json({ error: error.message || "Failed to mark notification read" });
    }
  });

  app.put("/api/notifications/:id", async (req, res) => {
    const id = req.params.id;
    try {
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("notifications").update(req.body).eq("id", id);
      }
      try {
        if (req.body.read) await markNotificationReadRepo(id);
      } catch {}

      broadcastSse("notifications", { id, ...req.body });
      res.json({ success: true, id, ...req.body });
    } catch (error: any) {
      console.error("Failed to update notification:", error);
      res.status(500).json({ error: error.message || "Failed to update notification" });
    }
  });

  app.put("/api/notifications/read-all", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("notifications").update({ read: true }).neq("id", "");
      }
      try {
        await markAllNotificationsReadRepo();
      } catch {}

      broadcastSse("notifications", { allRead: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to mark all notifications read:", error);
      res.status(500).json({ error: error.message || "Failed to mark all notifications read" });
    }
  });

  app.delete("/api/notifications/:id", async (req, res) => {
    const id = req.params.id;
    try {
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("notifications").delete().eq("id", id);
      }
      broadcastSse("notifications", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete notification:", error);
      res.status(500).json({ error: error.message || "Failed to delete notification" });
    }
  });

  app.delete("/api/notifications", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("notifications").delete().neq("id", "");
      }
      try {
        await clearAllNotificationsRepo();
      } catch {}

      broadcastSse("notifications", { cleared: true });
      res.json({ success: true });
    } catch (error: any) {
      console.error("Failed to clear notifications:", error);
      res.status(500).json({ error: error.message || "Failed to clear notifications" });
    }
  });

  // === 3. Team Leaders (Server-side Supabase Proxy) ===
  app.get("/api/teamLeaders", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaders").select("*");
        if (!error && Array.isArray(data) && data.length > 0) {
          data.sort((a: any, b: any) => (a.name || "").localeCompare(b.name || ""));
          return res.json(data);
        }
      }

      const list = await getTeamLeadersRepo();
      res.json(list);
    } catch (error: any) {
      console.error("Failed to fetch team leaders:", error);
      try {
        const list = await getTeamLeadersRepo();
        res.json(list);
      } catch {
        res.status(500).json({ error: error.message || "Failed to fetch team leaders" });
      }
    }
  });

  app.post("/api/teamLeaders", async (req, res) => {
    const id = req.body.id || `tl-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const item = { createdAt: new Date().toISOString(), ...req.body, id };
    try {
      let saved = item;
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaders").insert(item).select().single();
        if (!error && data) saved = data;
      }
      try {
        await addTeamLeaderRepo(saved);
      } catch (sqlErr) {
        console.warn("Cloud SQL team leader sync note:", sqlErr);
      }

      broadcastSse("teamLeaders", saved);
      res.json(saved);
    } catch (error: any) {
      console.error("Failed to add team leader:", error);
      res.status(500).json({ error: error.message || "Failed to add team leader" });
    }
  });

  app.put("/api/teamLeaders/:id", async (req, res) => {
    const id = req.params.id;
    const item = { ...req.body };
    try {
      let updated = { id, ...item };
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaders").update(item).eq("id", id).select().single();
        if (!error && data) updated = data;
      }
      try {
        await updateTeamLeaderRepo(id, item);
      } catch (sqlErr) {
        console.warn("Cloud SQL team leader update note:", sqlErr);
      }

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
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("teamLeaders").delete().eq("id", id);
      }
      try {
        await deleteTeamLeaderRepo(id);
      } catch (sqlErr) {
        console.warn("Cloud SQL team leader delete note:", sqlErr);
      }

      broadcastSse("teamLeaders", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete team leader:", error);
      res.status(500).json({ error: error.message || "Failed to delete team leader" });
    }
  });

  // === 4. Team Leader Notes (Server-side Supabase Proxy) ===
  app.get("/api/teamLeaderNotes", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaderNotes").select("*");
        if (!error && Array.isArray(data)) {
          return res.json(data.sort((a: any, b: any) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()));
        }
      }

      const list = await getTeamLeaderNotesRepo();
      res.json(list.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()));
    } catch (error: any) {
      console.error("Failed to fetch team leader notes:", error);
      try {
        const list = await getTeamLeaderNotesRepo();
        res.json(list);
      } catch {
        res.status(500).json({ error: error.message || "Failed to fetch team leader notes" });
      }
    }
  });

  app.post("/api/teamLeaderNotes", async (req, res) => {
    const id = req.body.id || `note-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const timestamp = req.body.timestamp || new Date().toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    });
    const item = { isUrgent: false, ...req.body, id, timestamp };
    try {
      let saved = item;
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaderNotes").insert(item).select().single();
        if (!error && data) saved = data;
      }
      try {
        await addTeamLeaderNoteRepo(saved);
      } catch (sqlErr) {
        console.warn("Cloud SQL note insert sync note:", sqlErr);
      }

      broadcastSse("teamLeaderNotes", saved);
      res.json(saved);
    } catch (error: any) {
      console.error("Failed to add team leader note:", error);
      res.status(500).json({ error: error.message || "Failed to add team leader note" });
    }
  });

  app.put("/api/teamLeaderNotes/:id", async (req, res) => {
    const id = req.params.id;
    const item = { ...req.body };
    try {
      let updated = { id, ...item };
      if (isServerSupabaseConfigured) {
        const { data, error } = await supabaseServer.from("teamLeaderNotes").update(item).eq("id", id).select().single();
        if (!error && data) updated = data;
      }
      try {
        await updateTeamLeaderNoteRepo(id, item);
      } catch (sqlErr) {
        console.warn("Cloud SQL note update sync note:", sqlErr);
      }

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
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("teamLeaderNotes").delete().eq("id", id);
      }
      try {
        await deleteTeamLeaderNoteRepo(id);
      } catch (sqlErr) {
        console.warn("Cloud SQL note delete sync note:", sqlErr);
      }

      broadcastSse("teamLeaderNotes", { id, deleted: true });
      res.json({ success: true, id });
    } catch (error: any) {
      console.error("Failed to delete team leader note:", error);
      res.status(500).json({ error: error.message || "Failed to delete team leader note" });
    }
  });

  app.delete("/api/teamLeaderNotes", async (req, res) => {
    try {
      if (isServerSupabaseConfigured) {
        await supabaseServer.from("teamLeaderNotes").delete().neq("id", "");
      }
      try {
        await clearTeamLeaderNotesRepo();
      } catch (sqlErr) {
        console.warn("Cloud SQL notes clear sync note:", sqlErr);
      }

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
