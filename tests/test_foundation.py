import os
import pytest
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from metadata.store import DatabaseManager
from config.loader import ConfigLoader

def test_db_initialization():
    db_path = "test_platform.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    db = DatabaseManager(db_path)
    conn = db._get_connection()
    cursor = conn.cursor()
    
    # Check tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    assert "runs" in tables
    assert "sla_misses" in tables
    conn.close()
    
    if os.path.exists(db_path):
        os.remove(db_path)

def test_config_loader():
    # Create dummy yaml
    with open("test_jobs.yaml", "w") as f:
        f.write("""
        jobs:
          - id: "test_job"
            type: "ingest"
            priority: "gold"
            sla_duration_seconds: 10
        """)
        
    loader = ConfigLoader("test_jobs.yaml")
    jobs = loader.load_jobs()
    
    assert len(jobs) == 1
    assert jobs[0].id == "test_job"
    assert jobs[0].priority == "gold"
    
    os.remove("test_jobs.yaml")
