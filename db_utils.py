# db_utils.py - Shared PostgreSQL database utilities for async operations
import os
import json
import asyncpg
from datetime import datetime
from typing import Optional, List, Tuple

# Load .env file if it exists
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

# Load .env file
load_env_file()

# Database connection parameters
PGHOST = os.getenv("PGHOST")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGSSLMODE = os.getenv("PGSSLMODE", "require")

# Connection pool (will be initialized on first use)
_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    """Get or create the database connection pool."""
    global _pool
    if _pool is None:
        if not all([PGHOST, PGDATABASE, PGUSER, PGPASSWORD]):
            raise ValueError(
                "Please set PGHOST, PGDATABASE, PGUSER, PGPASSWORD environment variables. "
                "Create a .env file or set them as environment variables."
            )
        
        # Build SSL mode
        ssl_config = "require" if PGSSLMODE == "require" else None
        
        _pool = await asyncpg.create_pool(
            host=PGHOST,
            port=PGPORT,
            database=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD,
            ssl=ssl_config,
            min_size=1,
            max_size=10
        )
    return _pool

async def close_pool():
    """Close the database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

async def init_db():
    """Initialize database - ensure transactions table exists."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

async def insert_tx(user_id: int, category: str, amount: float, date_str: str, 
                   description: Optional[str] = None, tags: Optional[str] = None, 
                   currency: str = 'INR') -> int:
    """Insert a transaction and return the ID."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Parse date string to date object
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            date_obj = datetime.now().date()
        
        # Convert tags to JSON if provided
        tags_json = None
        if tags:
            try:
                tags_json = json.loads(tags)
            except:
                if tags and tags.strip():
                    tags_json = [t.strip() for t in tags.split(",") if t.strip()]
        
        # Get next ID (since we're using INTEGER PRIMARY KEY, not SERIAL)
        max_id_row = await conn.fetchrow("SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM transactions")
        next_id = max_id_row['next_id'] if max_id_row else 1
        
        await conn.execute("""
            INSERT INTO transactions 
            (id, user_id, category, amount, currency, date, description, tags, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, next_id, user_id, category, amount, currency, date_obj, description, 
            json.dumps(tags_json) if tags_json else None)
        return next_id

async def get_transactions(user_id: int, limit: int = 10) -> List[Tuple]:
    """Get recent transactions for a user."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, category, amount, date, description 
            FROM transactions 
            WHERE user_id = $1 
            ORDER BY date DESC, id DESC 
            LIMIT $2
        """, user_id, limit)
        return [(r['id'], r['category'], r['amount'], str(r['date']), r['description']) 
                for r in rows]

async def get_summary(user_id: int, start_date: Optional[str] = None) -> List[Tuple]:
    """Get summary by category for a user."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if start_date:
            rows = await conn.fetch("""
                SELECT category, SUM(amount) as total
                FROM transactions 
                WHERE user_id = $1 AND date >= $2
                GROUP BY category
            """, user_id, start_date)
        else:
            rows = await conn.fetch("""
                SELECT category, SUM(amount) as total
                FROM transactions 
                WHERE user_id = $1
                GROUP BY category
            """, user_id)
        return [(r['category'], float(r['total'])) for r in rows]

async def get_export_data(user_id: int) -> List[Tuple]:
    """Get all transactions for export."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, date, category, amount, currency, description 
            FROM transactions 
            WHERE user_id = $1 
            ORDER BY date
        """, user_id)
        return [(r['id'], str(r['date']), r['category'], r['amount'], 
                r['currency'], r['description']) for r in rows]

