const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e7,
});

const socketsByUser = new Map();
const usersMeta = new Map();
const chatHistory = new Map();
const groups = new Map();
const groupHistory = new Map();

function chatKey(a, b) {
  return [a, b].sort().join("__");
}

function directoryList() {
  return Array.from(usersMeta.entries()).map(([name, meta]) => ({
    name,
    online: meta.online,
    lastSeen: meta.lastSeen,
  }));
}

function broadcastDirectory() {
  io.emit("directory", directoryList());
}

function userGroups(name) {
  return Array.from(groups.values()).filter((g) => g.members.includes(name));
}

function broadcastGroupsTo(name) {
  const sId = socketsByUser.get(name);
  if (sId) io.to(sId).emit("groups", userGroups(name));
}

app.get("/", (req, res) => {
  res.send("ChatWave backend is running.");
});

io.on("connection", (socket) => {
  let currentUser = null;

  socket.on("join", (name) => {
    if (!name || typeof name !== "string") return;
    currentUser = name.trim();
    socketsByUser.set(currentUser, socket.id);
    usersMeta.set(currentUser, { online: true, lastSeen: Date.now() });
    broadcastDirectory();
    broadcastGroupsTo(currentUser);
  });

  socket.on("get_history", ({ with: otherUser }) => {
    if (!currentUser || !otherUser) return;
    const key = chatKey(currentUser, otherUser);
    socket.emit("history", { with: otherUser, messages: chatHistory.get(key) || [] });
  });

  socket.on("private_message", ({ to, text, image }) => {
    if (!currentUser || !to || (!text && !image)) return;
    const key = chatKey(currentUser, to);
    const message = {
      id: Date.now() + "-" + Math.random().toString(36).slice(2, 8),
      from: currentUser,
      text: text || "",
      image: image || null,
      time: Date.now(),
    };
    const history = chatHistory.get(key) || [];
    history.push(message);
    chatHistory.set(key, history);

    const recipientSocketId = socketsByUser.get(to);
    if (recipientSocketId) io.to(recipientSocketId).emit("new_message", { from: currentUser, message });
    socket.emit("new_message", { from: currentUser, message });
  });

  socket.on("delete_message", ({ scope, target, messageId }) => {
    if (!currentUser) return;
    if (scope === "private") {
      const key = chatKey(currentUser, target);
      const history = chatHistory.get(key) || [];
      const updated = history.filter((m) => m.id !== messageId);
      chatHistory.set(key, updated);
      [currentUser, target].forEach((u) => {
        const sId = socketsByUser.get(u);
        if (sId) io.to(sId).emit("message_deleted", { scope, target: currentUser === u ? target : currentUser, messageId });
      });
    } else if (scope === "group") {
      const history = groupHistory.get(target) || [];
      const updated = history.filter((m) => m.id !== messageId);
      groupHistory.set(target, updated);
      const g = groups.get(target);
      if (g) {
        g.members.forEach((m) => {
          const sId = socketsByUser.get(m);
          if (sId) io.to(sId).emit("message_deleted", { scope, target, messageId });
        });
      }
    }
  });

  socket.on("create_group", ({ name, members }) => {
    if (!currentUser || !name || !Array.isArray(members)) return;
    const id = "g-" + Date.now() + "-" + Math.random().toString(36).slice(2, 6);
    const allMembers = Array.from(new Set([currentUser, ...members]));
    const group = { id, name, members: allMembers, createdBy: currentUser, createdAt: Date.now() };
    groups.set(id, group);
    groupHistory.set(id, []);
    allMembers.forEach((m) => broadcastGroupsTo(m));
  });

  socket.on("get_groups", () => {
    if (!currentUser) return;
    socket.emit("groups", userGroups(currentUser));
  });

  socket.on("get_group_history", ({ groupId }) => {
    if (!currentUser) return;
    socket.emit("group_history", { groupId, messages: groupHistory.get(groupId) || [] });
  });

  socket.on("group_message", ({ groupId, text, image }) => {
    if (!currentUser || !groupId || (!text && !image)) return;
    const group = groups.get(groupId);
    if (!group || !group.members.includes(currentUser)) return;
    const message = {
      id: Date.now() + "-" + Math.random().toString(36).slice(2, 8),
      from: currentUser,
      text: text || "",
      image: image || null,
      time: Date.now(),
    };
    const history = groupHistory.get(groupId) || [];
    history.push(message);
    groupHistory.set(groupId, history);
    group.members.forEach((m) => {
      const sId = socketsByUser.get(m);
      if (sId) io.to(sId).emit("new_group_message", { groupId, message });
    });
  });

  socket.on("typing", ({ to, isTyping }) => {
    if (!currentUser || !to) return;
    const recipientSocketId = socketsByUser.get(to);
    if (recipientSocketId) io.to(recipientSocketId).emit("typing", { from: currentUser, isTyping });
  });

  socket.on("disconnect", () => {
    if (currentUser) {
      socketsByUser.delete(currentUser);
      usersMeta.set(currentUser, { online: false, lastSeen: Date.now() });
      broadcastDirectory();
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`ChatWave backend listening on port ${PORT}`);
});
