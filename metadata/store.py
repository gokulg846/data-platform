import sqlite3
import os
from datetime import datetime
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'platform.db')

class DatabaseManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Idempotent database initialization."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Runs Table: Tracks every execution
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                job_id TEXT,
                priority TEXT,
                status TEXT, -- pending, running, success, failed
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                row_count INTEGER,
                error_message TEXT
            )
        ''')

        # SLA Table: Tracks violations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sla_misses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                job_id TEXT,
                miss_type TEXT, -- start_delay, duration_exceeded
                actual_value REAL,
                threshold_value REAL,
                logged_at TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def insert_run(self, run_id: str, job_id: str, priority: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO runs (run_id, job_id, priority, status, start_time)
            VALUES (?, ?, ?, 'pending', ?)
        ''', (run_id, job_id, priority, datetime.now()))
        conn.commit()
        conn.close()

    def update_run_status(self, run_id: str, status: str, 
                          row_count: Optional[int] = None, 
                          error: Optional[str] = None):
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now()
        
        query = "UPDATE runs SET status = ?, end_time = ?"
        params = [status, now]
        
        if row_count is not None:
            query += ", row_count = ?"
            params.append(row_count)
            
        if error is not None:
            query += ", error_message = ?"
            params.append(error)
            
        query += " WHERE run_id = ?"
        params.append(run_id)
        
        cursor.execute(query, params)
        conn.commit()
        conn.close()
        
    def log_sla_miss(self, run_id: str, job_id: str, miss_type: str, 
                     actual: float, threshold: float):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sla_misses (run_id, job_id, miss_type, actual_value, threshold_value, logged_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (run_id, job_id, miss_type, actual, threshold, datetime.now()))
        conn.commit()
        conn.close()
