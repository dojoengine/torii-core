#!/usr/bin/env bash
set -euo pipefail

DB_NAME="torii"
DB_USER="torii_user"

echo "[*] Dropping and recreating PostgreSQL database: $DB_NAME"
echo "[*] Using user: $DB_USER"

sudo -u postgres psql <<EOF
-- Drop existing connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${DB_NAME}';

-- Drop DB if exists
DROP DATABASE IF EXISTS ${DB_NAME};

-- Recreate DB
CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};

EOF

echo "[*] Setting privileges inside database…"

sudo -u postgres psql -d "$DB_NAME" <<EOF

-- Ensure schema ownership
ALTER SCHEMA public OWNER TO ${DB_USER};

-- Grant privileges on DB itself
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};

-- Default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES TO ${DB_USER};

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES TO ${DB_USER};

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON FUNCTIONS TO ${DB_USER};

ALTER DATABASE ${DB_NAME} SET bytea_output = 'hex';
ALTER ROLE ${DB_USER}  SET bytea_output = 'hex';
EOF

echo "[✓] Database reset complete."
echo "You can now connect using:"
echo "    psql -U ${DB_USER} -d ${DB_NAME} -W"