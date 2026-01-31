import pytest
import os
import pandas as pd
from ingestion.engine import IngestionEngine
from transformations.engine import TransformationEngine
from config.loader import JobConfig

def test_dummy_data_creation():
    # Ensure dummy data exists
    assert os.path.exists("data/input/transactions.csv")

def test_api_ingestion():
    engine = IngestionEngine()
    config = JobConfig(
        id="test_api", type="ingest", priority="silver", sla_duration_seconds=5,
        source="https://jsonplaceholder.typicode.com/users"
    )
    result = engine.run_job(config)
    assert result['row_count'] > 0
    assert os.path.exists(result['path'])

def test_file_ingestion():
    engine = IngestionEngine()
    config = JobConfig(
        id="test_file", type="ingest", priority="bronze", sla_duration_seconds=5,
        source="data/input/transactions.csv"
    )
    result = engine.run_job(config)
    assert result['row_count'] == 100
    assert os.path.exists(result['path'])

def test_transformation_logic():
    # Requires raw data first, so we mock or rely on previous tests order.
    # For this unit test, we'll manually write the raw files needed by the transform.
    from utils.io import IOManager
    
    # Mock Raw Users
    users = pd.DataFrame([{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}])
    IOManager.write_parquet(users, 'raw', 'ingest_users_api')
    
    # Mock Raw Transactions
    trans = pd.DataFrame([{'user_id': 1, 'amount': 100}, {'user_id': 1, 'amount': 50}])
    IOManager.write_parquet(trans, 'raw', 'ingest_transactions_csv')
    
    engine = TransformationEngine()
    config = JobConfig(
        id="transform_user_ltv", type="transform", priority="gold", sla_duration_seconds=10,
        dependencies=["ingest_users_api", "ingest_transactions_csv"]
    )
    
    result = engine.run_job(config)
    
    # Read back result
    df = pd.read_parquet(result['path'])
    alice = df[df['id'] == 1].iloc[0]
    assert alice['total_spend'] == 150.0
