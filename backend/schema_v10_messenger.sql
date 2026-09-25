-- Part 12 migration — run this AFTER schema_v9_status.sql, against the same database.
-- Adds: real contacts (so users are not browsable/chattable unless added),
-- profile picture + phone number, file attachments in chat, and photo/video
-- status updates.

-- Profile picture and phone number (phone lets someone be found/chatted with
-- even if the other person doesn't remember their username).
ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_number TEXT UNIQUE;

-- A user only shows up for you (in "New Chat", "New Group", and Status) once
-- you've explicitly added them by their exact username or phone number —
-- this is what stops everyone's username being visible to everyone.
CREATE TABLE IF NOT EXISTS contacts (
  owner_username    TEXT NOT NULL REFERENCES users(username),
  contact_username  TEXT NOT NULL REFERENCES users(username),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_username, contact_username),
  CHECK (owner_username <> contact_username)
);
CREATE INDEX IF NOT EXISTS idx_contacts_owner ON contacts (owner_username);

-- Generic file attachments (PDF, docs, zip, etc.) in 1:1 and group chat —
-- separate from `image`, which stays dedicated to photo bubbles.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_url TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_type TEXT;

ALTER TABLE group_messages ADD COLUMN IF NOT EXISTS file_url TEXT;
ALTER TABLE group_messages ADD COLUMN IF NOT EXISTS file_name TEXT;
ALTER TABLE group_messages ADD COLUMN IF NOT EXISTS file_type TEXT;

-- Photo/video status updates (previously text-only).
ALTER TABLE statuses ADD COLUMN IF NOT EXISTS media_url TEXT;
ALTER TABLE statuses ADD COLUMN IF NOT EXISTS media_type TEXT; -- 'image' | 'video'
ALTER TABLE statuses ALTER COLUMN text DROP NOT NULL;
