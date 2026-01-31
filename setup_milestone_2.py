import os

# Define the file contents
files = {
    "data-platform/utils/io.py": """
import pandas as pd
import os
from datetime import datetime

class IOManager:
    BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

    @staticmethod
    def get_path(layer: str, filename: str) -> str:
        folder = os.path.join(IOManager.BASE_PATH, layer)
        os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, filename)

    @staticmethod
    def write_parquet(df: pd.DataFrame, layer: str, job_id: str):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{job_id}_{ts}.parquet"
        path = IOManager.get_path(layer, filename)
        df.to_parquet(path, index=False)
        return path

    @staticmethod
    def read_latest_parquet(layer: str, job_id: str) -> pd.DataFrame:
        folder = os.path.join(IOManager.BASE_PATH, layer)
        if not os.path.exists(folder):
            raise FileNotFoundError(f"Layer {layer} does not exist")
        files = [f for f in os.listdir(folder) if f.startswith(job_id) and f.endswith('.parquet')]
        if not files:
            raise FileNotFoundError(f"No data found for {job_id} in {layer}")
        latest_file = sorted(files)[-1]
        return pd.read_parquet(os.path.join(folder, latest_file))
""",
    "data-platform/ingestion/engine.py": """
import pandas as pd
import requests
import os
from utils.io import IOManager

class IngestionEngine:
    def run_job(self, job_config) -> dict:
        if job_config.source.startswith("http"):
            return self._ingest_api(job_config)
        else:
            return self._ingest_file(job_config)

    def _ingest_api(self, config) -> dict:
        response = requests.get(config.source, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        if df.empty:
            raise ValueError("API returned empty data")
        path = IOManager.write_parquet(df, 'raw', config.id)
        return {"row_count": len(df), "path": path}

    def _ingest_file(self, config) -> dict:
        abs_path = os.path.abspath(config.source)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Source file not found: {abs_path}")
        df = pd.read_csv(abs_path)
        path = IOManager.write_parquet(df, 'raw', config.id)
        return {"row_count": len(df), "path": path}
""",
    "data-platform/transformations/engine.py": """
import pandas as pd
from utils.io import IOManager

class TransformationEngine:
    def run_job(self, job_config) -> dict:
        if job_config.id == 'transform_user_ltv':
            return self._transform_user_ltv(job_config)
        else:
            raise NotImplementedError(f"No transformation logic for {job_config.id}")

    def _transform_user_ltv(self, config) -> dict:
        users_job_id = config.dependencies[0]
        trans_job_id = config.dependencies[1]
        
        users = IOManager.read_latest_parquet('raw', users_job_id)
        transactions = IOManager.read_latest_parquet('raw', trans_job_id)
        
        users['id'] = users['id'].astype(int)
        transactions['user_id'] = transactions['user_id'].astype(int)
        
        ltv = transactions.groupby('user_id')['amount'].sum().reset_index()
        ltv.rename(columns={'amount': 'total_spend'}, inplace=True)
        
        final_df = pd.merge(users, ltv, left_on='id', right_on='user_id', how='left')
        final_df['total_spend'] = final_df['total_spend'].fillna(0)
        
        if final_df['total_spend'].max() < 0:
             raise ValueError("Data Quality Error: Negative LTV detected")
        
        path = IOManager.write_parquet(final_df, 'processed', config.id)
        return {"row_count": len(final_df), "path": path}
""",
    "data-platform/generate_dummy_data.py": """
import pandas as pd
import os
import random

os.makedirs("data/input", exist_ok=True)
data = []
for i in range(100):
    data.append({
        "transaction_id": i,
        "user_id": random.randint(1, 10),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "date": "2023-01-01"
    })
df = pd.DataFrame(data)
df.to_csv("data/input/transactions.csv", index=False)
print("✅ Dummy data created at data/input/transactions.csv")
""",
    "data-platform/tests/test_worker.py": """
import pytest
import os
import pandas as pd
import sys
# Ensure we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.engine import IngestionEngine
from transformations.engine import TransformationEngine
from config.loader import JobConfig
from utils.io import IOManager

def test_dummy_data_creation():
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
"""
}

# Write files
for path, content in files.items():
    # Ensure directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Write file
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.strip())
    print(f"Created: {path}")

print("\\n✅ Milestone 2 setup complete!")