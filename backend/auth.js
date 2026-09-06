const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const pool = require("./db");

// ⚠️ Set a real JWT_SECRET as an environment variable in production.
// If it's left at the default, anyone who reads this source code could
// forge valid login tokens for your server.
const JWT_SECRET = process.env.JWT_SECRET || "dev-secret-change-me";
const TOKEN_EXPIRY = "30d";

async function registerUser(username, password) {
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

  const existing = await pool.query("SELECT username FROM users WHERE username = $1", [username]);
  if (existing.rows.length > 0) {
    throw new Error("Username already taken");
  }

  const passwordHash = await bcrypt.hash(password, 10);
  await pool.query(
    `INSERT INTO users (username, password_hash, is_online, last_seen)
     VALUES ($1, $2, false, now())`,
    [username, passwordHash]
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
