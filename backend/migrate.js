const fs = require("fs");
const path = require("path");
const pool = require("./db");

async function runMigration() {
  try {
    const sqlPath = path.join(__dirname, "schema_v10_messenger.sql");
    if (fs.existsSync(sqlPath)) {
      const sql = fs.readFileSync(sqlPath, "utf8");
      await pool.query(sql);
      console.log("✅ Database migration schema_v10_messenger.sql applied successfully.");
    }
  } catch (err) {
    console.error("❌ Migration error:", err.message);
  }
}

runMigration();
