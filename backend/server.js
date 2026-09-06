const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");
const pool = require("./db");
const auth = require("./auth");
const media = require("./media");
const push = require("./push");
const ai = require("./ai");

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e7,
});

// socket.id lookup stays in memory — this is fine, it's a live-connection
// index, not application data. Everything durable now lives in Postgres.
const socketsByUser = new Map();

// --- Simple rate limiting: stops one account from flooding the server or
// spamming other users. This is intentionally basic (in-memory, per-server)
// — a production system would use Redis so limits hold across multiple
// server instances, but for a single server this is a real, working guard.
const messageTimestamps = new Map(); // username -> array of recent send times
const RATE_LIMIT_WINDOW_MS = 10000;
const RATE_LIMIT_MAX = 20; // max 20 messages per 10 seconds per user

function isRateLimited(username) {
  const now = Date.now();
  const timestamps = (messageTimestamps.get(username) || []).filter(
    (t) => now - t < RATE_LIMIT_WINDOW_MS
  );
  if (timestamps.length >= RATE_LIMIT_MAX) {
    messageTimestamps.set(username, timestamps);
    return true;
  }
  timestamps.push(now);
  messageTimestamps.set(username, timestamps);
  return false;
}

const MAX_TEXT_LENGTH = 4000;

// AI calls cost real money per request, so they get their own (tighter)
// rate limit separate from ordinary messaging.
const aiCallTimestamps = new Map();
const AI_RATE_LIMIT_WINDOW_MS = 60000;
const AI_RATE_LIMIT_MAX = 10; // max 10 AI requests per minute per user

function isAiRateLimited(username) {
  const now = Date.now();
  const timestamps = (aiCallTimestamps.get(username) || []).filter(
    (t) => now - t < AI_RATE_LIMIT_WINDOW_MS
  );
  if (timestamps.length >= AI_RATE_LIMIT_MAX) {
    aiCallTimestamps.set(username, timestamps);
    return true;
  }
  timestamps.push(now);
  aiCallTimestamps.set(username, timestamps);
  return false;
}

function sanitizeText(text) {
  if (typeof text !== "string") return "";
  return text.slice(0, MAX_TEXT_LENGTH);
}

function chatKeyParts(a, b) {
  return a < b ? [a, b] : [b, a];
}

async function broadcastDirectory() {
  const { rows } = await pool.query(
    `SELECT u.username, u.is_online, u.last_seen,
            COALESCE(s.show_last_seen, true) AS show_last_seen
     FROM users u
     LEFT JOIN user_settings s ON s.username = u.username
     ORDER BY u.username`
  );
  io.emit(
    "directory",
    rows.map((r) => ({
      name: r.username,
      online: r.is_online,
      // Respect the "show last seen" privacy setting — if it's off, we
      // simply don't send the timestamp at all (not just hide it in the UI).
      lastSeen: r.show_last_seen ? r.last_seen.getTime() : null,
    }))
  );
}

async function userGroups(username) {
  const { rows } = await pool.query(
    `SELECT g.id, g.name, g.invite_code AS "inviteCode", gm.role AS "myRole",
            array_agg(gm2.username) AS members
     FROM groups g
     JOIN group_members gm ON gm.group_id = g.id AND gm.username = $1
     JOIN group_members gm2 ON gm2.group_id = g.id
     GROUP BY g.id, gm.role`,
    [username]
  );
  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    members: r.members,
    myRole: r.myRole,
    // Only owners/admins get the invite code back automatically — regular
    // members have to ask, or we could expose it too; keeping it admin-only
    // matches how invite links are treated as a privileged action below.
    inviteCode: r.myRole === "owner" || r.myRole === "admin" ? r.inviteCode : undefined,
  }));
}

async function generateUniqueInviteCode() {
  for (let attempt = 0; attempt < 5; attempt++) {
    const code = crypto.randomBytes(5).toString("hex");
    const { rows } = await pool.query("SELECT 1 FROM groups WHERE invite_code = $1", [code]);
    if (rows.length === 0) return code;
  }
  throw new Error("Could not generate a unique invite code — try again");
}

async function getBlockedUsers(username) {
  const { rows } = await pool.query(
    `SELECT blocked, created_at FROM blocked_users WHERE blocker = $1 ORDER BY created_at DESC`,
    [username]
  );
  return rows.map((r) => ({ username: r.blocked, blockedAt: r.created_at.getTime() }));
}

// Builds the "Chats" list: one row per conversation that actually has
// messages (private) plus every group you belong to (even empty ones,
// like WhatsApp shows a brand-new group right away). Each row carries the
// last message preview, an unread count, and this user's pin/archive state.
//
// Note: this loops per-conversation with a few small queries each. That's
// fine at demo scale; a high-traffic production version would collapse
// this into one or two indexed queries instead.
async function getChatList(username) {
  const results = [];

  const { rows: partners } = await pool.query(
    `SELECT DISTINCT CASE WHEN sender = $1 THEN receiver ELSE sender END AS other
     FROM messages WHERE sender = $1 OR receiver = $1`,
    [username]
  );

  for (const { other } of partners) {
    const { rows: lastMsg } = await pool.query(
      `SELECT text, image, EXTRACT(EPOCH FROM created_at)*1000 AS time
       FROM messages WHERE (sender = $1 AND receiver = $2) OR (sender = $2 AND receiver = $1)
       ORDER BY created_at DESC LIMIT 1`,
      [username, other]
    );
    const { rows: readRows } = await pool.query(
      `SELECT last_read_at FROM read_state WHERE username = $1 AND scope = 'private' AND conversation_key = $2`,
      [username, other]
    );
    const lastRead = readRows[0]?.last_read_at || new Date(0);
    const { rows: unread } = await pool.query(
      `SELECT count(*)::int AS cnt FROM messages WHERE receiver = $1 AND sender = $2 AND created_at > $3`,
      [username, other, lastRead]
    );
    const { rows: pref } = await pool.query(
      `SELECT pinned, archived FROM chat_preferences WHERE username = $1 AND scope = 'private' AND conversation_key = $2`,
      [username, other]
    );
    const { rows: userRow } = await pool.query(
      `SELECT u.is_online, u.last_seen, COALESCE(s.show_last_seen, true) AS show_last_seen
       FROM users u LEFT JOIN user_settings s ON s.username = u.username
       WHERE u.username = $1`,
      [other]
    );

    results.push({
      type: "user",
      id: other,
      name: other,
      lastMessage: lastMsg[0]?.image ? "📷 Photo" : lastMsg[0]?.text || "",
      lastMessageTime: lastMsg[0]?.time || 0,
      unreadCount: unread[0]?.cnt || 0,
      pinned: pref[0]?.pinned || false,
      archived: pref[0]?.archived || false,
      online: userRow[0]?.is_online || false,
      lastSeen: userRow[0]?.show_last_seen && userRow[0]?.last_seen ? userRow[0].last_seen.getTime() : null,
    });
  }

  const groups = await userGroups(username);
  for (const g of groups) {
    const { rows: lastMsg } = await pool.query(
      `SELECT text, image, EXTRACT(EPOCH FROM created_at)*1000 AS time
       FROM group_messages WHERE group_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [g.id]
    );
    const { rows: readRows } = await pool.query(
      `SELECT last_read_at FROM read_state WHERE username = $1 AND scope = 'group' AND conversation_key = $2`,
      [username, g.id]
    );
    const lastRead = readRows[0]?.last_read_at || new Date(0);
    const { rows: unread } = await pool.query(
      `SELECT count(*)::int AS cnt FROM group_messages WHERE group_id = $1 AND sender != $2 AND created_at > $3`,
      [g.id, username, lastRead]
    );
    const { rows: pref } = await pool.query(
      `SELECT pinned, archived FROM chat_preferences WHERE username = $1 AND scope = 'group' AND conversation_key = $2`,
      [username, g.id]
    );

    results.push({
      type: "group",
      id: g.id,
      name: g.name,
      lastMessage: lastMsg[0]?.image ? "📷 Photo" : lastMsg[0]?.text || "",
      lastMessageTime: lastMsg[0]?.time || 0,
      unreadCount: unread[0]?.cnt || 0,
      pinned: pref[0]?.pinned || false,
      archived: pref[0]?.archived || false,
    });
  }

  results.sort((a, b) => {
    if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
    return (b.lastMessageTime || 0) - (a.lastMessageTime || 0);
  });
  return results;
}

async function broadcastGroupsTo(username) {
  const sId = socketsByUser.get(username);
  if (!sId) return;
  const groups = await userGroups(username);
  io.to(sId).emit("groups", groups);
}

app.get("/", (req, res) => {
  res.send("ChatWave backend is running (PostgreSQL-backed).");
});

app.get("/health/db", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ database: "connected" });
  } catch (e) {
    res.status(500).json({ database: "error", message: e.message });
  }
});

app.post("/api/v1/auth/register", async (req, res) => {
  try {
    const { username, password } = req.body || {};
    const result = await auth.registerUser(username, password);
    res.json(result);
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post("/api/v1/auth/login", async (req, res) => {
  try {
    const { username, password } = req.body || {};
    const result = await auth.loginUser(username, password);
    res.json(result);
  } catch (e) {
    res.status(401).json({ error: e.message });
  }
});

// Protects the upload endpoint the same way sockets are protected: the
// client proves who it is with a signed token, we never trust a client-
// supplied username for this.
function requireAuth(req, res, next) {
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: "Missing token" });
  try {
    const payload = auth.verifyToken(token);
    req.username = payload.username;
    next();
  } catch (e) {
    res.status(401).json({ error: "Invalid or expired token" });
  }
}

app.post("/api/v1/media/upload", requireAuth, async (req, res) => {
  try {
    const { image } = req.body || {};
    const url = await media.uploadImageFromDataUrl(image);
    res.json({ url });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Loads the last N messages of a conversation FOR THE REQUESTING USER ONLY
// — same authorization rules as the socket handlers (must be a participant
// or group member). This is what feeds the AI endpoints below.
async function loadRecentMessages(username, scope, conversationKey, limit) {
  if (scope === "private") {
    const { rows } = await pool.query(
      `SELECT sender AS "from", text, image FROM messages
       WHERE (sender = $1 AND receiver = $2) OR (sender = $2 AND receiver = $1)
       ORDER BY created_at DESC LIMIT $3`,
      [username, conversationKey, limit]
    );
    return rows.reverse();
  }
  if (scope === "group") {
    const { rows: memberCheck } = await pool.query(
      `SELECT 1 FROM group_members WHERE group_id = $1 AND username = $2`,
      [conversationKey, username]
    );
    if (memberCheck.length === 0) throw new Error("You're not a member of this group.");
    const { rows } = await pool.query(
      `SELECT sender AS "from", text, image FROM group_messages
       WHERE group_id = $1 ORDER BY created_at DESC LIMIT $2`,
      [conversationKey, limit]
    );
    return rows.reverse();
  }
  throw new Error("Invalid scope");
}

app.post("/api/v1/ai/translate", requireAuth, async (req, res) => {
  try {
    if (isAiRateLimited(req.username)) {
      return res.status(429).json({ error: "Too many AI requests — wait a moment and try again." });
    }
    const { text, targetLang } = req.body || {};
    if (!text || !targetLang) return res.status(400).json({ error: "text and targetLang are required" });
    const translation = await ai.translateText(String(text).slice(0, MAX_TEXT_LENGTH), targetLang);
    res.json({ translation });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post("/api/v1/ai/summarize", requireAuth, async (req, res) => {
  try {
    if (isAiRateLimited(req.username)) {
      return res.status(429).json({ error: "Too many AI requests — wait a moment and try again." });
    }
    const { scope, conversationKey } = req.body || {};
    const messages = await loadRecentMessages(req.username, scope, conversationKey, 30);
    if (messages.length === 0) return res.json({ summary: "No messages to summarize yet." });
    const summary = await ai.summarizeConversation(messages);
    res.json({ summary });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.post("/api/v1/ai/smart-replies", requireAuth, async (req, res) => {
  try {
    if (isAiRateLimited(req.username)) {
      return res.status(429).json({ error: "Too many AI requests — wait a moment and try again." });
    }
    const { scope, conversationKey } = req.body || {};
    const messages = await loadRecentMessages(req.username, scope, conversationKey, 10);
    if (messages.length === 0) return res.json({ suggestions: [] });
    const suggestions = await ai.suggestReplies(messages);
    res.json({ suggestions });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

io.on("connection", (socket) => {
  let currentUser = null;

  // The client no longer tells us who it is by name — it proves it with a
  // signed token from login/register. We decode the username FROM the token,
  // never from anything the client sends directly. This is what stops one
  // user from impersonating another by just typing their name.
  socket.on("authenticate", async (token) => {
    try {
      const payload = auth.verifyToken(token);
      currentUser = payload.username;
      socketsByUser.set(currentUser, socket.id);
      await pool.query(
        `UPDATE users SET is_online = true, last_seen = now() WHERE username = $1`,
        [currentUser]
      );
      socket.emit("authenticated", { username: currentUser });
      await broadcastDirectory();
      await broadcastGroupsTo(currentUser);
    } catch (e) {
      socket.emit("auth_error", "Session expired or invalid — please log in again.");
    }
  });

  socket.on("get_history", async ({ with: otherUser }) => {
    try {
      if (!currentUser || !otherUser) return;
      const { rows } = await pool.query(
        `SELECT m.id, m.sender AS "from", m.text, m.image,
                EXTRACT(EPOCH FROM m.created_at)*1000 AS time,
                m.reply_to_id AS "replyToId", m.reply_to_sender AS "replyToSender", m.reply_to_text AS "replyToText",
                EXTRACT(EPOCH FROM m.edited_at)*1000 AS "editedAt",
                (s.id IS NOT NULL) AS starred
         FROM messages m
         LEFT JOIN message_stars s ON s.scope = 'private' AND s.message_id = m.id AND s.username = $1
         WHERE (m.sender = $1 AND m.receiver = $2) OR (m.sender = $2 AND m.receiver = $1)
         ORDER BY m.created_at ASC`,
        [currentUser, otherUser]
      );
      socket.emit("history", { with: otherUser, messages: rows });
      // Opening a chat marks it as read.
      await pool.query(
        `INSERT INTO read_state (username, scope, conversation_key, last_read_at)
         VALUES ($1, 'private', $2, now())
         ON CONFLICT (username, scope, conversation_key) DO UPDATE SET last_read_at = now()`,
        [currentUser, otherUser]
      );
      // Reciprocal read receipts: only tell the other person "seen" if
      // BOTH of you have read receipts turned on — same trade-off WhatsApp
      // uses (turn yours off, and you stop seeing others' too).
      const { rows: settingsRows } = await pool.query(
        `SELECT u.username, COALESCE(s.show_read_receipts, true) AS show_read_receipts
         FROM users u LEFT JOIN user_settings s ON s.username = u.username
         WHERE u.username IN ($1, $2)`,
        [currentUser, otherUser]
      );
      const mySettings = settingsRows.find((r) => r.username === currentUser);
      const theirSettings = settingsRows.find((r) => r.username === otherUser);
      if (mySettings?.show_read_receipts && theirSettings?.show_read_receipts) {
        const otherSocketId = socketsByUser.get(otherUser);
        if (otherSocketId) {
          io.to(otherSocketId).emit("read_receipt", { conversationKey: currentUser, readAt: Date.now() });
        }
      }
    } catch (e) {
      console.error("get_history error", e);
    }
  });

  // A currently-open chat should stay "read" even as new messages arrive
  // in it — the client calls this whenever it receives a live message for
  // the conversation it's actively displaying.
  socket.on("mark_read", async ({ scope, conversationKey }) => {
    try {
      if (!currentUser || !scope || !conversationKey) return;
      await pool.query(
        `INSERT INTO read_state (username, scope, conversation_key, last_read_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (username, scope, conversation_key) DO UPDATE SET last_read_at = now()`,
        [currentUser, scope, conversationKey]
      );
    } catch (e) {
      console.error("mark_read error", e);
    }
  });

  // --- Privacy settings ---
  socket.on("get_settings", async () => {
    try {
      if (!currentUser) return;
      const { rows } = await pool.query(
        `SELECT COALESCE(show_last_seen, true) AS "showLastSeen",
                COALESCE(show_read_receipts, true) AS "showReadReceipts",
                COALESCE(ai_enabled, false) AS "aiEnabled"
         FROM users u LEFT JOIN user_settings s ON s.username = u.username
         WHERE u.username = $1`,
        [currentUser]
      );
      socket.emit("settings", rows[0] || { showLastSeen: true, showReadReceipts: true, aiEnabled: false });
    } catch (e) {
      console.error("get_settings error", e);
    }
  });

  socket.on("update_settings", async ({ showLastSeen, showReadReceipts, aiEnabled }) => {
    try {
      if (!currentUser) return;
      await pool.query(
        `INSERT INTO user_settings (username, show_last_seen, show_read_receipts, ai_enabled)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (username) DO UPDATE SET show_last_seen = $2, show_read_receipts = $3, ai_enabled = $4`,
        [currentUser, !!showLastSeen, !!showReadReceipts, !!aiEnabled]
      );
      socket.emit("settings", { showLastSeen: !!showLastSeen, showReadReceipts: !!showReadReceipts, aiEnabled: !!aiEnabled });
      await broadcastDirectory(); // last-seen visibility may have just changed
    } catch (e) {
      console.error("update_settings error", e);
    }
  });

  // --- Block / report ---
  socket.on("block_user", async ({ username }) => {
    try {
      if (!currentUser || !username || username === currentUser) return;
      await pool.query(
        `INSERT INTO blocked_users (blocker, blocked) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
        [currentUser, username]
      );
      socket.emit("blocked_users", await getBlockedUsers(currentUser));
    } catch (e) {
      console.error("block_user error", e);
    }
  });

  socket.on("unblock_user", async ({ username }) => {
    try {
      if (!currentUser || !username) return;
      await pool.query(`DELETE FROM blocked_users WHERE blocker = $1 AND blocked = $2`, [currentUser, username]);
      socket.emit("blocked_users", await getBlockedUsers(currentUser));
    } catch (e) {
      console.error("unblock_user error", e);
    }
  });

  socket.on("get_blocked_users", async () => {
    if (!currentUser) return;
    socket.emit("blocked_users", await getBlockedUsers(currentUser));
  });

  socket.on("report_user", async ({ username, reason }) => {
    try {
      if (!currentUser || !username) return;
      await pool.query(
        `INSERT INTO reports (reporter, reported, reason) VALUES ($1, $2, $3)`,
        [currentUser, username, (reason || "").slice(0, 500)]
      );
      socket.emit("report_submitted", { username });
    } catch (e) {
      console.error("report_user error", e);
    }
  });

  socket.on("register_push_token", async ({ token }) => {
    try {
      if (!currentUser || !token) return;
      await pool.query(
        `INSERT INTO push_tokens (username, expo_push_token) VALUES ($1, $2)
         ON CONFLICT (username, expo_push_token) DO UPDATE SET updated_at = now()`,
        [currentUser, token]
      );
    } catch (e) {
      console.error("register_push_token error", e);
    }
  });

  socket.on("post_status", async ({ text }) => {
    try {
      if (!currentUser || !text) return;
      const cleanText = sanitizeText(text).slice(0, 700);
      if (!cleanText) return;
      await pool.query(
        `INSERT INTO statuses (username, text, expires_at) VALUES ($1, $2, now() + interval '24 hours')`,
        [currentUser, cleanText]
      );
      io.emit("statuses_updated");
    } catch (e) {
      console.error("post_status error", e);
    }
  });

  socket.on("get_statuses", async () => {
    try {
      if (!currentUser) return;
      const { rows } = await pool.query(
        `SELECT s.id, s.username, s.text, EXTRACT(EPOCH FROM s.created_at)*1000 AS "createdAt"
         FROM statuses s
         WHERE s.expires_at > now()
         ORDER BY s.created_at DESC`
      );
      socket.emit("statuses", rows);
    } catch (e) {
      console.error("get_statuses error", e);
    }
  });

  socket.on("delete_status", async ({ statusId }) => {
    try {
      if (!currentUser || !statusId) return;
      await pool.query(`DELETE FROM statuses WHERE id = $1 AND username = $2`, [statusId, currentUser]);
      io.emit("statuses_updated");
    } catch (e) {
      console.error("delete_status error", e);
    }
  });

  socket.on("get_chat_list", async () => {
    try {
      if (!currentUser) return;
      socket.emit("chat_list", await getChatList(currentUser));
    } catch (e) {
      console.error("get_chat_list error", e);
    }
  });

  socket.on("toggle_pin", async ({ scope, conversationKey }) => {
    try {
      if (!currentUser || !scope || !conversationKey) return;
      const { rows } = await pool.query(
        `SELECT pinned FROM chat_preferences WHERE username = $1 AND scope = $2 AND conversation_key = $3`,
        [currentUser, scope, conversationKey]
      );
      const newPinned = !(rows[0]?.pinned);
      await pool.query(
        `INSERT INTO chat_preferences (username, scope, conversation_key, pinned)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (username, scope, conversation_key) DO UPDATE SET pinned = $4`,
        [currentUser, scope, conversationKey, newPinned]
      );
      socket.emit("chat_list", await getChatList(currentUser));
    } catch (e) {
      console.error("toggle_pin error", e);
    }
  });

  socket.on("toggle_archive", async ({ scope, conversationKey }) => {
    try {
      if (!currentUser || !scope || !conversationKey) return;
      const { rows } = await pool.query(
        `SELECT archived FROM chat_preferences WHERE username = $1 AND scope = $2 AND conversation_key = $3`,
        [currentUser, scope, conversationKey]
      );
      const newArchived = !(rows[0]?.archived);
      await pool.query(
        `INSERT INTO chat_preferences (username, scope, conversation_key, archived)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (username, scope, conversation_key) DO UPDATE SET archived = $4`,
        [currentUser, scope, conversationKey, newArchived]
      );
      socket.emit("chat_list", await getChatList(currentUser));
    } catch (e) {
      console.error("toggle_archive error", e);
    }
  });

  socket.on("private_message", async ({ to, text, image, replyTo }) => {
    try {
      if (!currentUser || !to || (!text && !image)) return;
      if (isRateLimited(currentUser)) {
        socket.emit("rate_limited", { message: "You're sending messages too fast — slow down." });
        return;
      }
      // Block check works both ways: doesn't matter who blocked whom,
      // messages don't go through either direction once blocked.
      const { rows: blockCheck } = await pool.query(
        `SELECT 1 FROM blocked_users WHERE (blocker = $1 AND blocked = $2) OR (blocker = $2 AND blocked = $1)`,
        [currentUser, to]
      );
      if (blockCheck.length > 0) {
        socket.emit("auth_error_scoped", { message: "This user isn't accepting messages from you." });
        return;
      }
      const { rows } = await pool.query(
        `INSERT INTO messages (sender, receiver, text, image, reply_to_id, reply_to_sender, reply_to_text)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, sender AS "from", text, image, EXTRACT(EPOCH FROM created_at)*1000 AS time,
                   reply_to_id AS "replyToId", reply_to_sender AS "replyToSender", reply_to_text AS "replyToText"`,
        [
          currentUser,
          to,
          sanitizeText(text),
          image || null,
          replyTo?.id || null,
          replyTo?.sender ? String(replyTo.sender).slice(0, 100) : null,
          replyTo?.text ? sanitizeText(replyTo.text).slice(0, 200) : null,
        ]
      );
      const message = rows[0];
      const recipientSocketId = socketsByUser.get(to);
      if (recipientSocketId) io.to(recipientSocketId).emit("new_message", { from: currentUser, message });
      socket.emit("new_message", { from: currentUser, message });

      // Push notification goes out regardless of whether the recipient's
      // socket is connected — a connected socket doesn't guarantee the app
      // is in the foreground. The phone's notification tray decides
      // whether to actually show it (the client suppresses it if that chat
      // is already open).
      push.sendPushToUser(to, {
        title: currentUser,
        body: message.image ? "📷 Photo" : message.text,
        data: { type: "private", conversationKey: currentUser },
      });
    } catch (e) {
      console.error("private_message error", e);
    }
  });

  // Authorization: only the original sender may delete their own message.
  // This prevents anyone else from deleting a message they didn't send.
  socket.on("delete_message", async ({ scope, target, messageId }) => {
    try {
      if (!currentUser || !messageId) return;
      if (scope === "private") {
        const { rows } = await pool.query(
          `DELETE FROM messages WHERE id = $1 AND sender = $2 RETURNING receiver`,
          [messageId, currentUser]
        );
        if (rows.length === 0) return; // not authorized or not found — silently ignore
        const other = rows[0].receiver;
        [currentUser, other].forEach((u) => {
          const sId = socketsByUser.get(u);
          if (sId) io.to(sId).emit("message_deleted", { scope, target: u === currentUser ? other : currentUser, messageId });
        });
      } else if (scope === "group") {
        const { rows } = await pool.query(
          `DELETE FROM group_messages WHERE id = $1 AND sender = $2 AND group_id = $3 RETURNING group_id`,
          [messageId, currentUser, target]
        );
        if (rows.length === 0) return;
        const { rows: members } = await pool.query(
          `SELECT username FROM group_members WHERE group_id = $1`,
          [target]
        );
        members.forEach(({ username }) => {
          const sId = socketsByUser.get(username);
          if (sId) io.to(sId).emit("message_deleted", { scope, target, messageId });
        });
      }
    } catch (e) {
      console.error("delete_message error", e);
    }
  });

  // Only the original sender can edit their own message — checked at the
  // database level (WHERE sender = currentUser), not just trusted from the client.
  socket.on("edit_message", async ({ scope, target, messageId, newText }) => {
    try {
      if (!currentUser || !messageId || !newText) return;
      const cleanText = sanitizeText(newText);

      if (scope === "private") {
        const { rows } = await pool.query(
          `UPDATE messages SET text = $1, edited_at = now()
           WHERE id = $2 AND sender = $3
           RETURNING receiver, EXTRACT(EPOCH FROM edited_at)*1000 AS "editedAt"`,
          [cleanText, messageId, currentUser]
        );
        if (rows.length === 0) return;
        const other = rows[0].receiver;
        const payload = { scope, messageId, newText: cleanText, editedAt: rows[0].editedAt };
        [currentUser, other].forEach((u) => {
          const sId = socketsByUser.get(u);
          if (sId) io.to(sId).emit("message_edited", payload);
        });
      } else if (scope === "group") {
        const { rows } = await pool.query(
          `UPDATE group_messages SET text = $1, edited_at = now()
           WHERE id = $2 AND sender = $3 AND group_id = $4
           RETURNING EXTRACT(EPOCH FROM edited_at)*1000 AS "editedAt"`,
          [cleanText, messageId, currentUser, target]
        );
        if (rows.length === 0) return;
        const payload = { scope, target, messageId, newText: cleanText, editedAt: rows[0].editedAt };
        const { rows: members } = await pool.query(
          `SELECT username FROM group_members WHERE group_id = $1`,
          [target]
        );
        members.forEach(({ username }) => {
          const sId = socketsByUser.get(username);
          if (sId) io.to(sId).emit("message_edited", payload);
        });
      }
    } catch (e) {
      console.error("edit_message error", e);
    }
  });

  // Starring is personal — toggling it only affects what YOU see, so we
  // only reply to the requesting socket, not broadcast to anyone else.
  socket.on("toggle_star", async ({ scope, messageId }) => {
    try {
      if (!currentUser || !messageId || !["private", "group"].includes(scope)) return;
      const existing = await pool.query(
        `SELECT id FROM message_stars WHERE scope = $1 AND message_id = $2 AND username = $3`,
        [scope, messageId, currentUser]
      );
      if (existing.rows.length > 0) {
        await pool.query(`DELETE FROM message_stars WHERE id = $1`, [existing.rows[0].id]);
        socket.emit("star_updated", { messageId, starred: false });
      } else {
        await pool.query(
          `INSERT INTO message_stars (scope, message_id, username) VALUES ($1, $2, $3)`,
          [scope, messageId, currentUser]
        );
        socket.emit("star_updated", { messageId, starred: true });
      }
    } catch (e) {
      console.error("toggle_star error", e);
    }
  });

  socket.on("create_group", async ({ name, members }) => {
    try {
      if (!currentUser || !name || !Array.isArray(members)) return;
      const cleanName = String(name).trim().slice(0, 100);
      if (!cleanName) return;
      if (members.length > 256) return; // sane upper bound
      const allMembers = Array.from(new Set([currentUser, ...members]));
      const inviteCode = await generateUniqueInviteCode();
      const { rows } = await pool.query(
        `INSERT INTO groups (name, created_by, invite_code) VALUES ($1, $2, $3) RETURNING id`,
        [cleanName, currentUser, inviteCode]
      );
      const groupId = rows[0].id;
      for (const m of allMembers) {
        const role = m === currentUser ? "owner" : "member";
        await pool.query(
          `INSERT INTO group_members (group_id, username, role) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
          [groupId, m, role]
        );
      }
      for (const m of allMembers) {
        await broadcastGroupsTo(m);
      }
    } catch (e) {
      console.error("create_group error", e);
    }
  });

  // --- Group management: roles & permissions ---
  // Rules:
  //   owner  → can promote/demote members, remove anyone (except self),
  //            rename the group, regenerate the invite link
  //   admin  → can remove regular members (not other admins/the owner),
  //            regenerate the invite link
  //   member → can view info and leave

  socket.on("get_group_info", async ({ groupId }) => {
    try {
      if (!currentUser || !groupId) return;
      const { rows: my } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (my.length === 0) {
        socket.emit("auth_error_scoped", { message: "You're not a member of this group." });
        return;
      }
      const { rows: groupRows } = await pool.query(
        `SELECT id, name, invite_code AS "inviteCode" FROM groups WHERE id = $1`,
        [groupId]
      );
      if (groupRows.length === 0) return;
      const { rows: members } = await pool.query(
        `SELECT username, role FROM group_members WHERE group_id = $1 ORDER BY
           CASE role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, username`,
        [groupId]
      );
      const isPrivileged = my[0].role === "owner" || my[0].role === "admin";
      socket.emit("group_info", {
        id: groupRows[0].id,
        name: groupRows[0].name,
        myRole: my[0].role,
        members,
        inviteCode: isPrivileged ? groupRows[0].inviteCode : undefined,
      });
    } catch (e) {
      console.error("get_group_info error", e);
    }
  });

  socket.on("update_group_name", async ({ groupId, name }) => {
    try {
      if (!currentUser || !groupId || !name) return;
      const cleanName = String(name).trim().slice(0, 100);
      if (!cleanName) return;
      const { rows: my } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (my.length === 0 || (my[0].role !== "owner" && my[0].role !== "admin")) {
        socket.emit("auth_error_scoped", { message: "Only the owner or an admin can rename the group." });
        return;
      }
      await pool.query(`UPDATE groups SET name = $1 WHERE id = $2`, [cleanName, groupId]);
      const { rows: members } = await pool.query(`SELECT username FROM group_members WHERE group_id = $1`, [groupId]);
      for (const { username } of members) await broadcastGroupsTo(username);
      io.emit("group_renamed", { groupId, name: cleanName }); // harmless broadcast; only relevant clients act on it
    } catch (e) {
      console.error("update_group_name error", e);
    }
  });

  socket.on("promote_member", async ({ groupId, username }) => {
    try {
      if (!currentUser || !groupId || !username || username === currentUser) return;
      const { rows: my } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (my.length === 0 || my[0].role !== "owner") {
        socket.emit("auth_error_scoped", { message: "Only the owner can promote members." });
        return;
      }
      await pool.query(
        `UPDATE group_members SET role = 'admin' WHERE group_id = $1 AND username = $2 AND role = 'member'`,
        [groupId, username]
      );
      const { rows: members } = await pool.query(`SELECT username FROM group_members WHERE group_id = $1`, [groupId]);
      for (const { username: m } of members) await broadcastGroupsTo(m);
      const sId = socketsByUser.get(currentUser);
      if (sId) io.to(sId).emit("group_member_updated", { groupId });
    } catch (e) {
      console.error("promote_member error", e);
    }
  });

  socket.on("demote_member", async ({ groupId, username }) => {
    try {
      if (!currentUser || !groupId || !username) return;
      const { rows: my } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (my.length === 0 || my[0].role !== "owner") {
        socket.emit("auth_error_scoped", { message: "Only the owner can demote admins." });
        return;
      }
      await pool.query(
        `UPDATE group_members SET role = 'member' WHERE group_id = $1 AND username = $2 AND role = 'admin'`,
        [groupId, username]
      );
      const sId = socketsByUser.get(currentUser);
      if (sId) io.to(sId).emit("group_member_updated", { groupId });
    } catch (e) {
      console.error("demote_member error", e);
    }
  });

  socket.on("remove_member", async ({ groupId, username }) => {
    try {
      if (!currentUser || !groupId || !username || username === currentUser) return;
      const { rows: myRow } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      const { rows: targetRow } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, username]
      );
      if (myRow.length === 0 || targetRow.length === 0) return;
      const myRole = myRow[0].role;
      const targetRole = targetRow[0].role;

      // Owner can remove admins/members. Admins can only remove regular members.
      const allowed =
        myRole === "owner" ? targetRole !== "owner" : myRole === "admin" ? targetRole === "member" : false;
      if (!allowed) {
        socket.emit("auth_error_scoped", { message: "You don't have permission to remove this member." });
        return;
      }
      await pool.query(`DELETE FROM group_members WHERE group_id = $1 AND username = $2`, [groupId, username]);
      await broadcastGroupsTo(username);
      const { rows: members } = await pool.query(`SELECT username FROM group_members WHERE group_id = $1`, [groupId]);
      for (const { username: m } of members) {
        await broadcastGroupsTo(m);
        const sId = socketsByUser.get(m);
        if (sId) io.to(sId).emit("group_member_updated", { groupId });
      }
    } catch (e) {
      console.error("remove_member error", e);
    }
  });

  socket.on("leave_group", async ({ groupId }) => {
    try {
      if (!currentUser || !groupId) return;
      const { rows: myRow } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (myRow.length === 0) return;

      if (myRow[0].role === "owner") {
        // Hand ownership to the longest-standing admin, or member if no
        // admin exists, so the group doesn't end up with nobody in charge.
        const { rows: successor } = await pool.query(
          `SELECT username FROM group_members WHERE group_id = $1 AND username != $2
           ORDER BY CASE role WHEN 'admin' THEN 0 ELSE 1 END, joined_at ASC LIMIT 1`,
          [groupId, currentUser]
        );
        if (successor.length > 0) {
          await pool.query(
            `UPDATE group_members SET role = 'owner' WHERE group_id = $1 AND username = $2`,
            [groupId, successor[0].username]
          );
        }
      }

      await pool.query(`DELETE FROM group_members WHERE group_id = $1 AND username = $2`, [groupId, currentUser]);
      await broadcastGroupsTo(currentUser);
      const { rows: members } = await pool.query(`SELECT username FROM group_members WHERE group_id = $1`, [groupId]);
      // If the last person just left, there's no one left to own or see
      // this group — clean it up instead of leaving an orphaned row behind.
      if (members.length === 0) {
        await pool.query(`DELETE FROM groups WHERE id = $1`, [groupId]);
        return;
      }
      for (const { username: m } of members) await broadcastGroupsTo(m);
    } catch (e) {
      console.error("leave_group error", e);
    }
  });

  socket.on("regenerate_invite", async ({ groupId }) => {
    try {
      if (!currentUser || !groupId) return;
      const { rows: my } = await pool.query(
        `SELECT role FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (my.length === 0 || (my[0].role !== "owner" && my[0].role !== "admin")) {
        socket.emit("auth_error_scoped", { message: "Only the owner or an admin can regenerate the invite link." });
        return;
      }
      const newCode = await generateUniqueInviteCode();
      await pool.query(`UPDATE groups SET invite_code = $1 WHERE id = $2`, [newCode, groupId]);
      socket.emit("invite_regenerated", { groupId, inviteCode: newCode });
    } catch (e) {
      console.error("regenerate_invite error", e);
    }
  });

  socket.on("join_via_invite", async ({ code }) => {
    try {
      if (!currentUser || !code) return;
      const { rows: groupRows } = await pool.query(`SELECT id, name FROM groups WHERE invite_code = $1`, [code]);
      if (groupRows.length === 0) {
        socket.emit("invite_error", { message: "This invite link is invalid or has expired." });
        return;
      }
      const groupId = groupRows[0].id;
      await pool.query(
        `INSERT INTO group_members (group_id, username, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING`,
        [groupId, currentUser]
      );
      await broadcastGroupsTo(currentUser);
      socket.emit("joined_group", { groupId, name: groupRows[0].name });
    } catch (e) {
      console.error("join_via_invite error", e);
    }
  });

  socket.on("get_groups", async () => {
    if (!currentUser) return;
    socket.emit("groups", await userGroups(currentUser));
  });

  // 🔒 Critical fix: previously anyone could read ANY group's messages just
  // by knowing (or guessing) its groupId — there was no check that they
  // were actually a member. Now we verify membership FIRST, before
  // returning anything.
  socket.on("get_group_history", async ({ groupId }) => {
    try {
      if (!currentUser || !groupId) return;
      const { rows: memberCheck } = await pool.query(
        `SELECT 1 FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (memberCheck.length === 0) {
        socket.emit("auth_error_scoped", { message: "You're not a member of this group." });
        return;
      }
      const { rows } = await pool.query(
        `SELECT gm.id, gm.sender AS "from", gm.text, gm.image,
                EXTRACT(EPOCH FROM gm.created_at)*1000 AS time,
                gm.reply_to_id AS "replyToId", gm.reply_to_sender AS "replyToSender", gm.reply_to_text AS "replyToText",
                EXTRACT(EPOCH FROM gm.edited_at)*1000 AS "editedAt",
                (s.id IS NOT NULL) AS starred
         FROM group_messages gm
         LEFT JOIN message_stars s ON s.scope = 'group' AND s.message_id = gm.id AND s.username = $2
         WHERE gm.group_id = $1
         ORDER BY gm.created_at ASC`,
        [groupId, currentUser]
      );
      socket.emit("group_history", { groupId, messages: rows });
      await pool.query(
        `INSERT INTO read_state (username, scope, conversation_key, last_read_at)
         VALUES ($1, 'group', $2, now())
         ON CONFLICT (username, scope, conversation_key) DO UPDATE SET last_read_at = now()`,
        [currentUser, groupId]
      );
    } catch (e) {
      console.error("get_group_history error", e);
    }
  });

  socket.on("group_message", async ({ groupId, text, image, replyTo }) => {
    try {
      if (!currentUser || !groupId || (!text && !image)) return;
      if (isRateLimited(currentUser)) {
        socket.emit("rate_limited", { message: "You're sending messages too fast — slow down." });
        return;
      }
      const { rows: memberCheck } = await pool.query(
        `SELECT 1 FROM group_members WHERE group_id = $1 AND username = $2`,
        [groupId, currentUser]
      );
      if (memberCheck.length === 0) return; // not a member — reject

      const { rows } = await pool.query(
        `INSERT INTO group_messages (group_id, sender, text, image, reply_to_id, reply_to_sender, reply_to_text)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, sender AS "from", text, image, EXTRACT(EPOCH FROM created_at)*1000 AS time,
                   reply_to_id AS "replyToId", reply_to_sender AS "replyToSender", reply_to_text AS "replyToText"`,
        [
          groupId,
          currentUser,
          sanitizeText(text),
          image || null,
          replyTo?.id || null,
          replyTo?.sender ? String(replyTo.sender).slice(0, 100) : null,
          replyTo?.text ? sanitizeText(replyTo.text).slice(0, 200) : null,
        ]
      );
      const message = rows[0];
      const { rows: members } = await pool.query(
        `SELECT username FROM group_members WHERE group_id = $1`,
        [groupId]
      );
      members.forEach(({ username }) => {
        const sId = socketsByUser.get(username);
        if (sId) io.to(sId).emit("new_group_message", { groupId, message });
      });

      const { rows: groupRow } = await pool.query(`SELECT name FROM groups WHERE id = $1`, [groupId]);
      const groupName = groupRow[0]?.name || "Group";
      members
        .filter(({ username }) => username !== currentUser)
        .forEach(({ username }) => {
          push.sendPushToUser(username, {
            title: groupName,
            body: `${currentUser}: ${message.image ? "📷 Photo" : message.text}`,
            data: { type: "group", conversationKey: groupId },
          });
        });
    } catch (e) {
      console.error("group_message error", e);
    }
  });

  socket.on("typing", ({ to, isTyping }) => {
    if (!currentUser || !to) return;
    const recipientSocketId = socketsByUser.get(to);
    if (recipientSocketId) io.to(recipientSocketId).emit("typing", { from: currentUser, isTyping });
  });

  socket.on("disconnect", async () => {
    try {
      if (currentUser) {
        socketsByUser.delete(currentUser);
        await pool.query(
          `UPDATE users SET is_online = false, last_seen = now() WHERE username = $1`,
          [currentUser]
        );
        await broadcastDirectory();
      }
    } catch (e) {
      console.error("disconnect error", e);
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`ChatWave backend (PostgreSQL) listening on port ${PORT}`);
});
