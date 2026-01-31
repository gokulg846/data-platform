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