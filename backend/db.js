const { Pool } = require("pg");

// DATABASE_URL is provided by your PostgreSQL host (e.g. Render Postgres).
// Set it as an environment variable — never hardcode credentials.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
    ? false
    : { rejectUnauthorized: false },
});

pool.on("error", (err) => {
  console.error("Unexpected database error", err);
});

module.exports = pool;
