import yaml
import os
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class JobConfig:
    id: str
    type: str
    priority: str
    sla_duration_seconds: int
    source: Optional[str] = None
    dependencies: Optional[List[str]] = None
    schedule_interval: int = 60

class ConfigLoader:
    def __init__(self, config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config not found at {config_path}")
        self.config_path = config_path

    def load_jobs(self) -> List[JobConfig]:
        with open(self.config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        jobs = []
        for j in data.get('jobs', []):
            if j['priority'] not in ['gold', 'silver', 'bronze']:
                raise ValueError(f"Invalid priority for job {j['id']}")
            
            jobs.append(JobConfig(
                id=j['id'],
                type=j['type'],
                priority=j['priority'],
                sla_duration_seconds=j.get('sla_duration_seconds', 60),
                source=j.get('source'),
                dependencies=j.get('dependencies', []),
                schedule_interval=j.get('schedule_interval', 60)
            ))
        return jobs

    def load_sla_targets(self):
        with open(self.config_path, 'r') as f:
            data = yaml.safe_load(f)
        return data.get('defaults', {}).get('sla_targets', {})
