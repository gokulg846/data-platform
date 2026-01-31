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