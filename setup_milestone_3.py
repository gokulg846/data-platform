import os

files = {
    "data-platform/scheduler/core.py": """
import time
import uuid
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from queue import PriorityQueue
from collections import deque

from config.loader import ConfigLoader
from metadata.store import DatabaseManager
from ingestion.engine import IngestionEngine
from transformations.engine import TransformationEngine

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Worker Functions (Must be top-level for pickling)
def execute_job(job_config):
    try:
        if job_config.type == 'ingest':
            engine = IngestionEngine()
            result = engine.run_job(job_config)
        elif job_config.type == 'transform':
            engine = TransformationEngine()
            result = engine.run_job(job_config)
        return {"status": "success", "meta": result}
    except Exception as e:
        return {"status": "failed", "error": str(e)}

class Scheduler:
    def __init__(self, config_path, max_workers=4):
        self.loader = ConfigLoader(config_path)
        self.db = DatabaseManager()
        self.max_workers = max_workers
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        
        # State Tracking
        self.futures = {} # {future: (job_id, run_id, start_time)}
        self.queues = {
            'gold': deque(),
            'silver': deque(),
            'bronze': deque()
        }
        
    def submit_job(self, job_id):
        # 1. Find Job Config
        jobs = self.loader.load_jobs()
        job_config = next((j for j in jobs if j.id == job_id), None)
        if not job_config:
            logger.error(f"Job {job_id} not found in config")
            return

        # 2. Create Run ID & Log to DB
        run_id = str(uuid.uuid4())
        self.db.insert_run(run_id, job_id, job_config.priority)
        
        # 3. Add to Queue with Timestamp (for aging)
        entry = {
            'config': job_config, 
            'run_id': run_id, 
            'queued_at': datetime.now()
        }
        self.queues[job_config.priority].append(entry)
        logger.info(f"Queued {job_id} [{job_config.priority}]")

    def _promote_starved_jobs(self):
        # Simple Aging: If waiting > 20s, promote up
        now = datetime.now()
        
        # Bronze -> Silver
        for _ in range(len(self.queues['bronze'])):
            job = self.queues['bronze'][0]
            wait_time = (now - job['queued_at']).total_seconds()
            if wait_time > 20:
                self.queues['bronze'].popleft()
                self.queues['silver'].append(job)
                logger.info(f"Promoted {job['config'].id} to Silver (Waited {wait_time:.1f}s)")
            else:
                break # Queue is ordered by time, so we can stop

    def _get_next_job(self):
        # Strict Priority Check: Gold -> Silver -> Bronze
        if self.queues['gold']: return self.queues['gold'].popleft()
        if self.queues['silver']: return self.queues['silver'].popleft()
        if self.queues['bronze']: return self.queues['bronze'].popleft()
        return None

    def tick(self):
        # 1. Check for Completed Jobs
        # list() needed to avoid 'dictionary changed size during iteration'
        for future in list(self.futures.keys()):
            if future.done():
                job_id, run_id, start_time = self.futures.pop(future)
                result = future.result()
                
                duration = (datetime.now() - start_time).total_seconds()
                
                if result['status'] == 'success':
                    self.db.update_run_status(run_id, 'success', row_count=result['meta'].get('row_count'))
                    logger.info(f"Job {job_id} SUCCEEDED in {duration:.2f}s")
                else:
                    self.db.update_run_status(run_id, 'failed', error=result['error'])
                    logger.error(f"Job {job_id} FAILED: {result['error']}")

        # 2. Promote Starved Jobs
        self._promote_starved_jobs()

        # 3. Submit New Jobs (if slots available)
        while len(self.futures) < self.max_workers:
            job = self._get_next_job()
            if not job:
                break
            
            # Execute
            future = self.executor.submit(execute_job, job['config'])
            self.futures[future] = (job['config'].id, job['run_id'], datetime.now())
            
            # Update DB to 'running'
            self.db.update_run_status(job['run_id'], 'running')
            
    def run_forever(self):
        logger.info("Scheduler Started. Press Ctrl+C to stop.")
        try:
            while True:
                self.tick()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Scheduler Stopping...")
            self.executor.shutdown(wait=False)
""",
    "data-platform/main.py": """
import sys
import os
import argparse
import time
from scheduler.core import Scheduler

# Add current dir to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description="SLA-Aware Data Platform")
    parser.add_argument('--mode', choices=['scheduler', 'trigger'], required=True)
    parser.add_argument('--job', help="Job ID to trigger (only for trigger mode)")
    
    args = parser.parse_args()
    
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'jobs.yaml')
    scheduler = Scheduler(config_path)

    if args.mode == 'scheduler':
        scheduler.run_forever()
        
    elif args.mode == 'trigger':
        # In a real system, this would send a signal to the daemon.
        # Here, we just simulate a run for testing if the scheduler is imported.
        print("To trigger jobs, currently you must instantiate the scheduler.")
        print("Running Demo Loop:")
        
        # Demo: Submit all jobs defined in config
        jobs = scheduler.loader.load_jobs()
        for j in jobs:
            scheduler.submit_job(j.id)
            
        scheduler.run_forever()

if __name__ == "__main__":
    main()
"""
}

# Write files
for path, content in files.items():
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.strip())
    print(f"Created: {path}")

print("\\n✅ Milestone 3 setup complete!")