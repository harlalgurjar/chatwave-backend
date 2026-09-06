const { Expo } = require("expo-server-sdk");
const pool = require("./db");

const expo = new Expo();

// Sends a push notification to every device a user has registered.
// Failures for one device never block the others — each is best-effort.
async function sendPushToUser(username, { title, body, data }) {
  try {
    const { rows } = await pool.query(
      `SELECT expo_push_token FROM push_tokens WHERE username = $1`,
      [username]
    );
    const messages = [];
    for (const { expo_push_token } of rows) {
      if (!Expo.isExpoPushToken(expo_push_token)) continue;
      messages.push({ to: expo_push_token, sound: "default", title, body, data });
    }
    if (messages.length === 0) return;

    const chunks = expo.chunkPushNotifications(messages);
    for (const chunk of chunks) {
      try {
        await expo.sendPushNotificationsAsync(chunk);
      } catch (e) {
        console.error("push chunk send error", e);
      }
    }
  } catch (e) {
    console.error("sendPushToUser error", e);
  }
}

module.exports = { sendPushToUser };
