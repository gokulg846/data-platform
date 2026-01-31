# SLA-Aware Data Pipeline & Job Scheduler

A production-grade, small-scale data platform built in Python. This system demonstrates core data engineering principles: priority scheduling, SLA enforcement, DAG dependencies, and observability.

## 🏗 System Architecture

The system follows a **Control Plane / Data Plane** separation:

1.  **Scheduler (The Brain):**
    * Runs a continuous event loop (tick frequency: 1s).
    * Manages 3 Priority Queues (**Gold**, **Silver**, **Bronze**).
    * **Aging:** Promotes Bronze jobs to Silver if starved > 20s.
    * **DAG Enforcement:** Ensures Child jobs wait for Parents to succeed.
    * **SLA Monitor:** Checks for start-delays and execution-overruns.

2.  **Worker Pool (The Muscle):**
    * Executes logic in a `ProcessPoolExecutor` (Parallel execution).
    * **Ingestion:** Handles APIs (retry logic) and Local Files.
    * **Transformation:** Pandas-based cleaning and aggregation.
    * **IO Layer:** Writes atomic Parquet files to `./data/raw` and `./data/processed`.

3.  **Observability (The Eyes):**
    * **Metadata Store:** SQLite database tracking every run status and timestamp.
    * **Dashboard:** Streamlit app for real-time monitoring of job history and alerts.

## 🚀 How to Run

### 1. Setup
```bash
# Clone and Install
git clone <your-repo-url>
cd data-platform
pip install -r requirements.txt
pip install pyarrow

# Generate Dummy Input Data
python data-platform/generate_dummy_data.py