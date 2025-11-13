# db_utils.py - Shared PostgreSQL database utilities for async operations
import os
import json
import asyncio
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
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
_pool: Optional[pool.ThreadedConnectionPool] = None

def _get_pool() -> pool.ThreadedConnectionPool:
    """Get or create the database connection pool (sync)."""
    global _pool
    if _pool is None:
        if not all([PGHOST, PGDATABASE, PGUSER, PGPASSWORD]):
            raise ValueError(
                "Please set PGHOST, PGDATABASE, PGUSER, PGPASSWORD environment variables. "
                "Create a .env file or set them as environment variables."
            )
        
        # Build connection string
        conn_string = f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER} password={PGPASSWORD}"
        if PGSSLMODE == "require":
            conn_string += " sslmode=require"
        
        _pool = pool.ThreadedConnectionPool(1, 10, conn_string)
    return _pool

async def get_connection():
    """Get a connection from the pool (async wrapper)."""
    pool_obj = _get_pool()
    loop = asyncio.get_event_loop()
    conn = await loop.run_in_executor(None, pool_obj.getconn)
    return conn

def return_connection(conn):
    """Return a connection to the pool."""
    pool_obj = _get_pool()
    pool_obj.putconn(conn)

async def close_pool():
    """Close the database connection pool."""
    global _pool
    if _pool:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _pool.closeall)
        _pool = None

async def init_db():
    """Initialize database - ensure transactions table exists."""
    conn = await get_connection()
    try:
        def _init():
            cur = conn.cursor()
            cur.execute("""
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
            conn.commit()
            cur.close()
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _init)
    finally:
        return_connection(conn)

async def insert_tx(user_id: int, category: str, amount: float, date_str: str, 
                   description: Optional[str] = None, tags: Optional[str] = None, 
                   currency: str = 'INR') -> int:
    """Insert a transaction and return the ID."""
    conn = await get_connection()
    try:
        def _insert():
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
            
            cur = conn.cursor()
            # Get next ID (since we're using INTEGER PRIMARY KEY, not SERIAL)
            cur.execute("SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM transactions")
            result = cur.fetchone()
            next_id = result[0] if result else 1
            
            cur.execute("""
                INSERT INTO transactions 
                (id, user_id, category, amount, currency, date, description, tags, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (next_id, user_id, category, amount, currency, date_obj, description, 
                  json.dumps(tags_json) if tags_json else None))
            conn.commit()
            cur.close()
            return next_id
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _insert)
    finally:
        return_connection(conn)

async def get_transactions(user_id: int, limit: int = 10) -> List[Tuple]:
    """Get recent transactions for a user."""
    conn = await get_connection()
    try:
        def _fetch():
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, category, amount, date, description 
                FROM transactions 
                WHERE user_id = %s 
                ORDER BY date DESC, id DESC 
                LIMIT %s
            """, (user_id, limit))
            rows = cur.fetchall()
            cur.close()
            return [(r['id'], r['category'], r['amount'], str(r['date']), r['description']) 
                    for r in rows]
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch)
    finally:
        return_connection(conn)

async def get_summary(user_id: int, start_date: Optional[str] = None) -> List[Tuple]:
    """Get summary by category for a user."""
    conn = await get_connection()
    try:
        def _fetch():
            cur = conn.cursor(cursor_factory=RealDictCursor)
            if start_date:
                cur.execute("""
                    SELECT category, SUM(amount) as total
                    FROM transactions 
                    WHERE user_id = %s AND date >= %s
                    GROUP BY category
                """, (user_id, start_date))
            else:
                cur.execute("""
                    SELECT category, SUM(amount) as total
                    FROM transactions 
                    WHERE user_id = %s
                    GROUP BY category
                """, (user_id,))
            rows = cur.fetchall()
            cur.close()
            return [(r['category'], float(r['total'])) for r in rows]
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch)
    finally:
        return_connection(conn)

async def get_export_data(user_id: int) -> List[Tuple]:
    """Get all transactions for export."""
    conn = await get_connection()
    try:
        def _fetch():
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, date, category, amount, currency, description 
                FROM transactions 
                WHERE user_id = %s 
                ORDER BY date
            """, (user_id,))
            rows = cur.fetchall()
            cur.close()
            return [(r['id'], str(r['date']), r['category'], r['amount'], 
                    r['currency'], r['description']) for r in rows]
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch)
    finally:
        return_connection(conn)
