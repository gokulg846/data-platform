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