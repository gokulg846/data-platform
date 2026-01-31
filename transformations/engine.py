import pandas as pd
from utils.io import IOManager

class TransformationEngine:
    def run_job(self, job_config) -> dict:
        # In a real system, we might use a factory pattern here.
        # For this scope, we map IDs to methods directly.
        if job_config.id == 'transform_user_ltv':
            return self._transform_user_ltv(job_config)
        else:
            raise NotImplementedError(f"No transformation logic for {job_config.id}")

    def _transform_user_ltv(self, config) -> dict:
        # 1. Read Upstream Dependencies
        # We assume dependencies are listed in config.dependencies
        # [0] is users (API), [1] is transactions (CSV)
        users_job_id = config.dependencies[0]
        trans_job_id = config.dependencies[1]
        
        users = IOManager.read_latest_parquet('raw', users_job_id)
        transactions = IOManager.read_latest_parquet('raw', trans_job_id)
        
        # 2. Business Logic: Calculate LTV
        # Join users on ID and sum transaction amounts
        # API users have 'id', 'name', etc.
        # CSV transactions should have 'user_id', 'amount'
        
        # Ensure types match for join
        users['id'] = users['id'].astype(int)
        transactions['user_id'] = transactions['user_id'].astype(int)
        
        ltv = transactions.groupby('user_id')['amount'].sum().reset_index()
        ltv.rename(columns={'amount': 'total_spend'}, inplace=True)
        
        final_df = pd.merge(users, ltv, left_on='id', right_on='user_id', how='left')
        final_df['total_spend'] = final_df['total_spend'].fillna(0)
        
        # 3. Quality Check
        if final_df['total_spend'].max() < 0:
             raise ValueError("Data Quality Error: Negative LTV detected")
        
        # 4. Save
        path = IOManager.write_parquet(final_df, 'processed', config.id)
        return {"row_count": len(final_df), "path": path}
