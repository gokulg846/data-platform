Data Pipeline & Job Scheduler

A production-grade, small-scale data platform built in Python. This system incorporates core data engineering principles: priority scheduling, SLA enforcement, DAG dependencies, and observability.

## System Architecture

The system follows a **Control Plane / Data Plane** separation:

1.  **Scheduler:**
    * Runs a continuous event loop (tick frequency: 1s).
    * Manages 3 Priority Queues (**Gold**, **Silver**, **Bronze**).
    * **Aging:** Promotes Bronze jobs to Silver if starved > 20s.
    * **DAG Enforcement:** Ensures Child jobs wait for Parents to succeed.
    * **SLA Monitor:** Checks for start-delays and execution-overruns.

2.  **Worker Pool:**
    * Executes logic in a `ProcessPoolExecutor` (Parallel execution).
    * **Ingestion:** Handles APIs (retry logic) and Local Files.
    * **Transformation:** Pandas-based cleaning and aggregation.
    * **IO Layer:** Writes atomic Parquet files to `./data/raw` and `./data/processed`.

3.  **Observability:**
    * **Metadata Store:** SQLite database tracking every run status and timestamp.
    * **Dashboard:** Streamlit app for real-time monitoring of job history and alerts.

Repository Structure

```text
data-platform/
├── config/
│   ├── jobs.yaml           # Central definition of DAG, SLAs, and Priorities
│   └── loader.py           # Configuration parsing logic
├── ingestion/              # Logic for fetching APIs and parsing CSVs
├── transformations/        # Pandas logic for business rules
├── scheduler/
│   └── core.py             # The "Brain": Priority Queues, DAGs, SLA logic
├── metadata/               # SQLite interface
├── dashboard/              # Streamlit observability app
├── utils/                  # Shared IO and helper functions
├── tests/                  # Pytest integration suite
└── main.py                 # Entry point
```
🚀 How to Run

1. Installation
```bash
# Install dependencies
pip install -r requirements.txt
pip install pyarrow

# Generate required dummy data for the CSV ingestion job
python data-platform/generate_dummy_data.py
```
2. Start the Scheduler

The scheduler runs as a continuous daemon. It will automatically trigger jobs based on the schedule.

Mac/Linux:
```bash
PYTHONPATH=data-platform python data-platform/main.py --mode scheduler
```

Windows:
```
$env:PYTHONPATH="data-platform"; python data-platform/main.py --mode scheduler
```
3. Launch the Dashboard

Open a new terminal to monitor the system in real-time.

```
streamlit run data-platform/dashboard/app.py
```

Configuration Guide

Modify data-platform/config/jobs.yaml to tweak the system behavior.

Testing

Run the integration test suite to verify the ingestion and configuration logic.

```Bash
python -m pytest data-platform/tests
```


