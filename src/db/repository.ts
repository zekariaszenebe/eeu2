import { db } from './index.ts';
import {
  interruptions,
  notifications,
  presetFeeders,
  hubRecords,
  teamLeaderNotes,
  customerContacts,
  teamLeaders,
  feedbacks,
  users
} from './schema.ts';
import { eq, desc, asc } from 'drizzle-orm';
import { HUB_RECORDS } from '../data/hubData.ts';
import { INITIAL_FEEDERS_LIST, INITIAL_CUSTOMER_CONTACTS } from '../data/mockData.ts';

// Initial defaults
const DEFAULT_LEADERS = [
  { id: 'admin-1', username: 'admin', password: '@Eeu1234', name: 'System Administrator', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() },
  { id: 'agent-1', username: 'contactcenter', password: '@Eeu1234', name: 'Contact Center Agent', district: 'Team A', role: 'agent', createdAt: new Date().toISOString() },
  { id: 'tl-1', username: 'teamleader', password: '@Eeu1234', name: 'Team Leader', district: 'Team D', role: 'team_leader', createdAt: new Date().toISOString() },
  { id: 'tl-d', username: 'zz01641821', password: 'eeu1234', name: 'Zekarias Zenebe', district: 'Admin', role: 'admin', createdAt: new Date().toISOString() }
];

// --- Interruptions ---
export async function getInterruptionsRepo() {
  try {
    return await db.select().from(interruptions);
  } catch (error) {
    console.error("Failed to query interruptions:", error);
    throw new Error("Failed to query interruptions", { cause: error });
  }
}

export async function addInterruptionRepo(data: any) {
  try {
    const id = data.id || `f-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const res = await db.insert(interruptions).values({
      id,
      feederName: data.feederName || 'Unknown Feeder',
      district: data.district || 'Team A',
      direction: data.direction || null,
      type: data.type || 'Earth Fault',
      status: data.status || 'Active',
      startTime: data.startTime || new Date().toISOString(),
      estimatedRestorationTime: data.estimatedRestorationTime || 'N/A',
      affectedArea: data.affectedArea || '',
      remark: data.remark || '',
      lastUpdated: data.lastUpdated || new Date().toISOString()
    }).returning();
    return res[0];
  } catch (error) {
    console.error("Failed to insert interruption:", error);
    throw new Error("Failed to insert interruption", { cause: error });
  }
}

export async function updateInterruptionRepo(id: string, data: any) {
  try {
    const updatePayload: any = {};
    if (data.feederName !== undefined) updatePayload.feederName = data.feederName;
    if (data.district !== undefined) updatePayload.district = data.district;
    if (data.direction !== undefined) updatePayload.direction = data.direction;
    if (data.type !== undefined) updatePayload.type = data.type;
    if (data.status !== undefined) updatePayload.status = data.status;
    if (data.startTime !== undefined) updatePayload.startTime = data.startTime;
    if (data.estimatedRestorationTime !== undefined) updatePayload.estimatedRestorationTime = data.estimatedRestorationTime;
    if (data.affectedArea !== undefined) updatePayload.affectedArea = data.affectedArea;
    if (data.remark !== undefined) updatePayload.remark = data.remark;
    if (data.lastUpdated !== undefined) updatePayload.lastUpdated = data.lastUpdated;

    const res = await db.update(interruptions).set(updatePayload).where(eq(interruptions.id, id)).returning();
    return res[0] || null;
  } catch (error) {
    console.error("Failed to update interruption:", error);
    throw new Error("Failed to update interruption", { cause: error });
  }
}

export async function deleteInterruptionRepo(id: string) {
  try {
    await db.delete(interruptions).where(eq(interruptions.id, id));
    return true;
  } catch (error) {
    console.error("Failed to delete interruption:", error);
    throw new Error("Failed to delete interruption", { cause: error });
  }
}

// --- Notifications ---
export async function getNotificationsRepo() {
  try {
    return await db.select().from(notifications);
  } catch (error) {
    console.error("Failed to query notifications:", error);
    throw new Error("Failed to query notifications", { cause: error });
  }
}

export async function addNotificationRepo(data: any) {
  try {
    const res = await db.insert(notifications).values({
      id: data.id,
      feederId: data.feederId || null,
      type: data.type,
      title: data.title,
      message: data.message,
      timestamp: data.timestamp,
      read: Boolean(data.read)
    }).returning();
    return res[0];
  } catch (error) {
    console.error("Failed to insert notification:", error);
    throw new Error("Failed to insert notification", { cause: error });
  }
}

export async function markNotificationReadRepo(id: string) {
  try {
    const res = await db.update(notifications).set({ read: true }).where(eq(notifications.id, id)).returning();
    return res[0] || null;
  } catch (error) {
    console.error("Failed to mark notification as read:", error);
    throw new Error("Failed to mark notification as read", { cause: error });
  }
}

export async function markAllNotificationsReadRepo() {
  try {
    await db.update(notifications).set({ read: true });
    return true;
  } catch (error) {
    console.error("Failed to mark all notifications read:", error);
    throw new Error("Failed to mark all notifications read", { cause: error });
  }
}

export async function clearAllNotificationsRepo() {
  try {
    await db.delete(notifications);
    return true;
  } catch (error) {
    console.error("Failed to clear notifications:", error);
    throw new Error("Failed to clear notifications", { cause: error });
  }
}

// --- Preset Feeders ---
export async function getPresetFeedersRepo() {
  try {
    let rows = await db.select().from(presetFeeders);
    if (rows.length === 0) {
      const initial = INITIAL_FEEDERS_LIST.map((f, idx) => ({ id: `feeder-${idx}`, feederStr: f }));
      await db.insert(presetFeeders).values(initial);
      rows = await db.select().from(presetFeeders);
    }
    return rows.map(r => r.feederStr);
  } catch (error) {
    console.error("Failed to query preset feeders:", error);
    throw new Error("Failed to query preset feeders", { cause: error });
  }
}

export async function addPresetFeederRepo(feederStr: string) {
  try {
    const id = `feeder-${Date.now()}`;
    await db.insert(presetFeeders).values({ id, feederStr });
    return { id, feederStr };
  } catch (error) {
    console.error("Failed to add preset feeder:", error);
    throw new Error("Failed to add preset feeder", { cause: error });
  }
}

export async function bulkSetPresetFeedersRepo(feederStrings: string[]) {
  try {
    await db.delete(presetFeeders);
    if (feederStrings && feederStrings.length > 0) {
      const items = feederStrings.map((f, idx) => ({
        id: `feeder-${idx}-${Date.now()}`,
        feederStr: f
      }));
      await db.insert(presetFeeders).values(items);
    }
    return true;
  } catch (error) {
    console.error("Failed to bulk set preset feeders:", error);
    throw new Error("Failed to bulk set preset feeders", { cause: error });
  }
}

export async function deletePresetFeederRepo(feederStr: string) {
  try {
    await db.delete(presetFeeders).where(eq(presetFeeders.feederStr, feederStr));
    return true;
  } catch (error) {
    console.error("Failed to delete preset feeder:", error);
    throw new Error("Failed to delete preset feeder", { cause: error });
  }
}

export async function updatePresetFeederRepo(oldFeederStr: string, newFeederStr: string) {
  try {
    await db.update(presetFeeders).set({ feederStr: newFeederStr }).where(eq(presetFeeders.feederStr, oldFeederStr));
    return true;
  } catch (error) {
    console.error("Failed to update preset feeder:", error);
    throw new Error("Failed to update preset feeder", { cause: error });
  }
}

// --- Hub Records ---
export async function getHubRecordsRepo() {
  try {
    let rows = await db.select().from(hubRecords).orderBy(asc(hubRecords.no));
    if (rows.length === 0) {
      await db.insert(hubRecords).values(HUB_RECORDS);
      rows = await db.select().from(hubRecords).orderBy(asc(hubRecords.no));
    }
    return rows;
  } catch (error) {
    console.error("Failed to query hub records:", error);
    throw new Error("Failed to query hub records", { cause: error });
  }
}

export async function updateHubRecordRepo(no: number, data: any) {
  try {
    const existing = await db.select().from(hubRecords).where(eq(hubRecords.no, no));
    if (existing.length > 0) {
      const res = await db.update(hubRecords).set(data).where(eq(hubRecords.no, no)).returning();
      return res[0];
    } else {
      const res = await db.insert(hubRecords).values({ no, ...data }).returning();
      return res[0];
    }
  } catch (error) {
    console.error("Failed to update hub record:", error);
    throw new Error("Failed to update hub record", { cause: error });
  }
}

export async function bulkUpdateHubRecordsRepo(records: any[]) {
  try {
    for (const record of records) {
      const no = Number(record.no);
      const existing = await db.select().from(hubRecords).where(eq(hubRecords.no, no));
      if (existing.length > 0) {
        await db.update(hubRecords).set(record).where(eq(hubRecords.no, no));
      } else {
        await db.insert(hubRecords).values(record);
      }
    }
    return true;
  } catch (error) {
    console.error("Failed to bulk update hub records:", error);
    throw new Error("Failed to bulk update hub records", { cause: error });
  }
}

export async function resetHubRecordsRepo() {
  try {
    await db.delete(hubRecords);
    await db.insert(hubRecords).values(HUB_RECORDS);
    return HUB_RECORDS;
  } catch (error) {
    console.error("Failed to reset hub records:", error);
    throw new Error("Failed to reset hub records", { cause: error });
  }
}

// --- Team Leader Notes ---
export async function getTeamLeaderNotesRepo() {
  try {
    return await db.select().from(teamLeaderNotes);
  } catch (error) {
    console.error("Failed to query team leader notes:", error);
    throw new Error("Failed to query team leader notes", { cause: error });
  }
}

export async function addTeamLeaderNoteRepo(data: any) {
  try {
    const res = await db.insert(teamLeaderNotes).values({
      id: data.id,
      content: data.content,
      author: data.author,
      timestamp: data.timestamp,
      isUrgent: Boolean(data.isUrgent)
    }).returning();
    return res[0];
  } catch (error) {
    console.error("Failed to insert team leader note:", error);
    throw new Error("Failed to insert team leader note", { cause: error });
  }
}

export async function updateTeamLeaderNoteRepo(id: string, data: any) {
  try {
    const res = await db.update(teamLeaderNotes).set(data).where(eq(teamLeaderNotes.id, id)).returning();
    return res[0] || null;
  } catch (error) {
    console.error("Failed to update team leader note:", error);
    throw new Error("Failed to update team leader note", { cause: error });
  }
}

export async function deleteTeamLeaderNoteRepo(id: string) {
  try {
    await db.delete(teamLeaderNotes).where(eq(teamLeaderNotes.id, id));
    return true;
  } catch (error) {
    console.error("Failed to delete team leader note:", error);
    throw new Error("Failed to delete team leader note", { cause: error });
  }
}

export async function clearTeamLeaderNotesRepo() {
  try {
    await db.delete(teamLeaderNotes);
    return true;
  } catch (error) {
    console.error("Failed to clear team leader notes:", error);
    throw new Error("Failed to clear team leader notes", { cause: error });
  }
}

// --- Customer Contacts ---
export async function getCustomerContactsRepo() {
  try {
    let rows = await db.select().from(customerContacts);
    if (rows.length === 0) {
      await db.insert(customerContacts).values(INITIAL_CUSTOMER_CONTACTS);
      rows = await db.select().from(customerContacts);
    }
    return rows;
  } catch (error) {
    console.error("Failed to query customer contacts:", error);
    throw new Error("Failed to query customer contacts", { cause: error });
  }
}

export async function addCustomerContactRepo(data: any) {
  try {
    const res = await db.insert(customerContacts).values({
      id: data.id,
      name: data.name,
      phone: data.phone,
      category: data.category,
      locationInfo: data.locationInfo || null,
      hotlineShortCode: data.hotlineShortCode || null
    }).returning();
    return res[0];
  } catch (error) {
    console.error("Failed to add customer contact:", error);
    throw new Error("Failed to add customer contact", { cause: error });
  }
}

export async function updateCustomerContactRepo(id: string, data: any) {
  try {
    const res = await db.update(customerContacts).set(data).where(eq(customerContacts.id, id)).returning();
    return res[0] || null;
  } catch (error) {
    console.error("Failed to update customer contact:", error);
    throw new Error("Failed to update customer contact", { cause: error });
  }
}

export async function deleteCustomerContactRepo(id: string) {
  try {
    await db.delete(customerContacts).where(eq(customerContacts.id, id));
    return true;
  } catch (error) {
    console.error("Failed to delete customer contact:", error);
    throw new Error("Failed to delete customer contact", { cause: error });
  }
}

// --- Team Leaders ---
export async function getTeamLeadersRepo() {
  try {
    let rows = await db.select().from(teamLeaders);
    if (rows.length === 0) {
      await db.insert(teamLeaders).values(DEFAULT_LEADERS);
      rows = await db.select().from(teamLeaders);
    }
    return rows;
  } catch (error) {
    console.error("Failed to query team leaders:", error);
    throw new Error("Failed to query team leaders", { cause: error });
  }
}

export async function addTeamLeaderRepo(data: any) {
  try {
    const res = await db.insert(teamLeaders).values({
      id: data.id,
      username: data.username,
      password: data.password,
      name: data.name,
      district: data.district,
      role: data.role || 'team_leader',
      mustChangePassword: Boolean(data.mustChangePassword),
      createdAt: data.createdAt || new Date().toISOString()
    }).returning();
    return res[0];
  } catch (error) {
    console.error("Failed to add team leader:", error);
    throw new Error("Failed to add team leader", { cause: error });
  }
}

export async function updateTeamLeaderRepo(id: string, data: any) {
  try {
    const res = await db.update(teamLeaders).set(data).where(eq(teamLeaders.id, id)).returning();
    return res[0] || null;
  } catch (error) {
    console.error("Failed to update team leader:", error);
    throw new Error("Failed to update team leader", { cause: error });
  }
}

export async function deleteTeamLeaderRepo(id: string) {
  try {
    await db.delete(teamLeaders).where(eq(teamLeaders.id, id));
    return true;
  } catch (error) {
    console.error("Failed to delete team leader:", error);
    throw new Error("Failed to delete team leader", { cause: error });
  }
}

// --- Feedbacks ---
export async function addFeedbackRepo(data: any) {
  try {
    const res = await db.insert(feedbacks).values({
      id: data.id || `fb-${Date.now()}`,
      rating: Number(data.rating),
      category: data.category,
      feedbackText: data.feedbackText,
      submittedBy: data.submittedBy,
      targetEmail: data.targetEmail,
      timestamp: data.timestamp || new Date().toISOString()
    }).returning();
    return res[0];
  } catch (error) {
    console.error("Failed to insert feedback:", error);
    throw new Error("Failed to insert feedback", { cause: error });
  }
}
