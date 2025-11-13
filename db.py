# migrate_sqlite_to_postgres.py
import os
import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import json

# Try to load from .env file if it exists
def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and value:
                        os.environ.setdefault(key, value)

# Load .env file if it exists
load_env_file()

# Load connection info from env
# Example env variables:
#   PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGSSLMODE = os.getenv("PGSSLMODE", "require")  # for Supabase use require

SQLITE_PATH = os.getenv("SQLITE_PATH", "data.db")  # path to your sqlite file

if not all([PGHOST, PGDATABASE, PGUSER, PGPASSWORD]):
    error_msg = (
        "Please set PGHOST, PGDATABASE, PGUSER, PGPASSWORD environment variables.\n\n"
        "You can either:\n"
        "1. Set them as environment variables:\n"
        "   export PGHOST=your_host\n"
        "   export PGDATABASE=your_database\n"
        "   export PGUSER=your_user\n"
        "   export PGPASSWORD=your_password\n\n"
        "2. Create a .env file in the same directory with:\n"
        "   PGHOST=your_host\n"
        "   PGDATABASE=your_database\n"
        "   PGUSER=your_user\n"
        "   PGPASSWORD=your_password\n"
        "   PGPORT=5432 (optional)\n"
        "   PGSSLMODE=require (optional)\n"
    )
    raise SystemExit(error_msg)

def normalize_row(row):
    # row is a dict-like from sqlite
    # normalize tags: if stored as comma-separated string -> JSON array
    tags = row.get("tags")
    if tags is None:
        tags_json = None
    else:
        # if already looks like JSON, attempt parse
        tags = tags.strip()
        try:
            tags_json = json.loads(tags)
        except Exception:
            if tags == "":
                tags_json = None
            else:
                # split by comma and trim
                tags_json = [t.strip() for t in tags.split(",") if t.strip()]
    # normalize dates
    date_val = row.get("date")
    bill_due_date = row.get("bill_due_date")
    def to_date(d):
        if d is None: return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(d, fmt).date()
            except Exception:
                pass
        # fallback: let psycopg parse it (string)
        return d

    return {
        "id": row.get("id"),
        "user_id": row.get("user_id"),
        "category": row.get("category"),
        "amount": row.get("amount"),
        "currency": row.get("currency") or "INR",
        "date": to_date(date_val),
        "description": row.get("description"),
        "tags": json.dumps(tags_json) if tags_json is not None else None,
        "merchant": row.get("merchant"),
        "payment_method": row.get("payment_method"),
        "transaction_type": row.get("transaction_type") or "expense",
        "is_recurring": bool(row.get("is_recurring")) if row.get("is_recurring") is not None else False,
        "recurring_period": row.get("recurring_period"),
        "status": row.get("status") or "paid",
        "bill_due_date": to_date(bill_due_date),
        "attachment_url": row.get("attachment_url"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }

def create_postgres_table(pg_cur):
    """Create the transactions table in PostgreSQL if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        category TEXT NOT NULL,
        amount REAL NOT NULL,
        currency TEXT DEFAULT 'INR',
        date DATE NOT NULL,
        description TEXT,
        tags JSONB,
        merchant TEXT,
        payment_method TEXT,
        transaction_type TEXT DEFAULT 'expense',
        is_recurring BOOLEAN DEFAULT FALSE,
        recurring_period TEXT,
        status TEXT DEFAULT 'paid',
        bill_due_date DATE,
        attachment_url TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    pg_cur.execute(create_table_sql)

def migrate(batch_size=500):
    # connect sqlite
    sq = sqlite3.connect(SQLITE_PATH)
    sq.row_factory = sqlite3.Row
    cur = sq.cursor()
    cur.execute("SELECT * FROM transactions;")
    rows = cur.fetchmany(batch_size)

    # connect postgres
    pg_conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode=PGSSLMODE
    )
    pg_cur = pg_conn.cursor()
    
    # Create table if it doesn't exist
    create_postgres_table(pg_cur)
    pg_conn.commit()

    inserted = 0
    while rows:
        data = []
        for r in rows:
            nr = normalize_row(dict(r))
            data.append((
                nr["id"],
                nr["user_id"],
                nr["category"],
                nr["amount"],
                nr["currency"],
                nr["date"],
                nr["description"],
                nr["tags"],            # JSONB string
                nr["merchant"],
                nr["payment_method"],
                nr["transaction_type"],
                nr["is_recurring"],
                nr["recurring_period"],
                nr["status"],
                nr["bill_due_date"],
                nr["attachment_url"],
                nr["created_at"],
                nr["updated_at"]
            ))

        # Use INSERT ... ON CONFLICT DO NOTHING if you want idempotency (requires unique constraint)
        insert_sql = """
        INSERT INTO transactions
        (id, user_id, category, amount, currency, date, description, tags, merchant,
         payment_method, transaction_type, is_recurring, recurring_period, status,
         bill_due_date, attachment_url, created_at, updated_at)
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
        """
        # use execute_values and cast tags to jsonb
        def template_func(vals):
            # psycopg2 will auto-quote; cast tags to jsonb
            return "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # Build execute_values call with template - easier is to pre-process tags as None or JSON string
        execute_values(pg_cur, insert_sql, data, template=None)
        pg_conn.commit()
        inserted += len(data)
        print(f"Inserted {inserted} rows...")
        rows = cur.fetchmany(batch_size)

    pg_cur.close()
    pg_conn.close()
    sq.close()
    print("Migration finished.")

if __name__ == "__main__":
    migrate()
