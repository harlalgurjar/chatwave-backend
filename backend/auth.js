const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const pool = require("./db");

// ⚠️ Set a real JWT_SECRET as an environment variable in production.
// If it's left at the default, anyone who reads this source code could
// forge valid login tokens for your server.
const JWT_SECRET = process.env.JWT_SECRET || "dev-secret-change-me";
const TOKEN_EXPIRY = "30d";

// Phone number is optional but, if given, must be unique — it's a second
// way for someone to be found (search_users) and chatted with, exactly
// like a username, in case the other person doesn't remember the username.
function normalizePhone(phone) {
  if (!phone) return null;
  const digits = String(phone).replace(/[^\d+]/g, "");
  if (!digits) return null;
  if (digits.length < 7 || digits.length > 16) {
    throw new Error("Phone number should be 7-16 digits");
  }
  return digits;
}

async function registerUser(username, password, phone) {
  username = (username || "").trim();
  if (!username || username.length < 3) {
    throw new Error("Username must be at least 3 characters");
  }
  if (!/^[a-zA-Z0-9_]+$/.test(username)) {
    throw new Error("Username can only contain letters, numbers, and underscores");
  }
  if (!password || password.length < 6) {
    throw new Error("Password must be at least 6 characters");
  }
  const phoneNumber = normalizePhone(phone);

  const existing = await pool.query("SELECT username FROM users WHERE username = $1", [username]);
  if (existing.rows.length > 0) {
    throw new Error("Username already taken");
  }
  if (phoneNumber) {
    const existingPhone = await pool.query("SELECT username FROM users WHERE phone_number = $1", [phoneNumber]);
    if (existingPhone.rows.length > 0) {
      throw new Error("This phone number is already registered");
    }
  }

  const passwordHash = await bcrypt.hash(password, 10);
  await pool.query(
    `INSERT INTO users (username, password_hash, phone_number, is_online, last_seen)
     VALUES ($1, $2, $3, false, now())`,
    [username, passwordHash, phoneNumber]
  );

  const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: TOKEN_EXPIRY });
  return { username, token };
}

async function loginUser(username, password) {
  username = (username || "").trim();
  const { rows } = await pool.query(
    "SELECT username, password_hash FROM users WHERE username = $1",
    [username]
  );
  // Same error for "no such user" and "wrong password" — this stops an
  // attacker from using the error message to find out which usernames exist.
  const genericError = "Invalid username or password";
  if (rows.length === 0 || !rows[0].password_hash) {
    throw new Error(genericError);
  }
  const valid = await bcrypt.compare(password || "", rows[0].password_hash);
  if (!valid) {
    throw new Error(genericError);
  }

  const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: TOKEN_EXPIRY });
  return { username, token };
}

function verifyToken(token) {
  // Throws if invalid or expired — caller must catch this.
  return jwt.verify(token, JWT_SECRET);
}

module.exports = { registerUser, loginUser, verifyToken };
