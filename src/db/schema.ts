import { boolean, integer, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  id: serial('id').primaryKey(),
  uid: text('uid').notNull().unique(),
  email: text('email').notNull(),
  name: text('name'),
  role: text('role').default('agent'),
  createdAt: timestamp('created_at').defaultNow(),
});

export const interruptions = pgTable('interruptions', {
  id: text('id').primaryKey(),
  feederName: text('feeder_name').notNull(),
  district: text('district').notNull(),
  direction: text('direction'),
  type: text('type').notNull(),
  status: text('status').notNull(),
  startTime: text('start_time').notNull(),
  estimatedRestorationTime: text('estimated_restoration_time').notNull(),
  affectedArea: text('affected_area').notNull(),
  remark: text('remark').notNull(),
  lastUpdated: text('last_updated').notNull(),
});

export const notifications = pgTable('notifications', {
  id: text('id').primaryKey(),
  feederId: text('feeder_id'),
  type: text('type').notNull(),
  title: text('title').notNull(),
  message: text('message').notNull(),
  timestamp: text('timestamp').notNull(),
  read: boolean('read').default(false).notNull(),
});

export const presetFeeders = pgTable('preset_feeders', {
  id: text('id').primaryKey(),
  feederStr: text('feeder_str').notNull(),
});

export const hubRecords = pgTable('hub_records', {
  no: integer('no').primaryKey(),
  region: text('region').notNull(),
  csc: text('csc').notNull(),
  address: text('address').notNull(),
  dummyBp: text('dummy_bp').notNull(),
  rsg: text('rsg').notNull(),
  dispatcherName: text('dispatcher_name').notNull(),
  dispatcherId: text('dispatcher_id').notNull(),
  customerServiceTlId: text('customer_service_tl_id').notNull(),
  officeLocation: text('office_location').notNull(),
});

export const teamLeaderNotes = pgTable('team_leader_notes', {
  id: text('id').primaryKey(),
  content: text('content').notNull(),
  author: text('author').notNull(),
  timestamp: text('timestamp').notNull(),
  isUrgent: boolean('is_urgent').default(false).notNull(),
});

export const customerContacts = pgTable('customer_contacts', {
  id: text('id').primaryKey(),
  name: text('name').notNull(),
  phone: text('phone').notNull(),
  category: text('category').notNull(),
  locationInfo: text('location_info'),
  hotlineShortCode: text('hotline_short_code'),
});

export const teamLeaders = pgTable('team_leaders', {
  id: text('id').primaryKey(),
  username: text('username').notNull(),
  password: text('password').notNull(),
  name: text('name').notNull(),
  district: text('district').notNull(),
  role: text('role').default('team_leader'),
  mustChangePassword: boolean('must_change_password').default(false),
  createdAt: text('created_at').notNull(),
});

export const feedbacks = pgTable('feedbacks', {
  id: text('id').primaryKey(),
  rating: integer('rating').notNull(),
  category: text('category').notNull(),
  feedbackText: text('feedback_text').notNull(),
  submittedBy: text('submitted_by').notNull(),
  targetEmail: text('target_email').notNull(),
  timestamp: text('timestamp').notNull(),
});
