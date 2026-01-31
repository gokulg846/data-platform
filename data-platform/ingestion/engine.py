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