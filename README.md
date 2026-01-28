# **Standard Data Management Framework (SDMF)**

A **modular, scalable, and Python-based Data Management Framework** designed to standardize **data ingestion, validation, transformation, metadata handling, and storage** across enterprise workflows.

This framework eliminates repetitive boilerplate and provides a **consistent structure for building reliable, maintainable data pipelines**.

***

## ✅ **Key Features**

* **Modular Design** – Plug-and-play components for ingestion, validation, transformation, and storage.
* **Schema Alignment & Partitioning** – Built-in support for CDC (Change Data Capture) and MERGE operations.
* **Metadata Management** – Centralized handling of feed specifications and lineage.
* **Scalable** – Works seamlessly with **Spark**, **Delta Lake**, and distributed environments like **Databricks**.
* **Logging & Monitoring** – Custom logging with retention and rotation policies.

***

## 📂 **Project Structure**

    sdmf/
    ├── cli/                # Command-line interface for orchestration
    ├── config/             # Configurations (logging, paths, retention)
    ├── orchestrator/       # Pipeline orchestration logic
    ├── result_generator/   # Excel/Report generation utilities
    ├── utils/              # Helper functions
    └── ...

***

## ⚙️ **Installation**

### **Option 1 (Recommended): Editable Install**

From the project root (where `pyproject.toml` is located):

```bash
pip install -e .
python -m build
```

Then run:

```bash
python -m sdmf.cli.main
```

***

## 🔗 **Dependencies**

Install required packages:

```bash
pip install pyspark==3.5.1 delta-spark==3.1.0
```

***

## 🚀 **Usage**

Run the main orchestrator:

```bash
python -m sdmf.cli.main --config config/config.ini --run_id <unique_run_id>
```

***

## 🛠 **Configuration**

Update `config.ini`:

```ini
[DEFAULT]
outbound_directory_name=sdmf_outbound
log_directory_name=sdmf_logs
temp_log_location=/tmp/
file_hunt_path=/dbfs/FileStore/sdmf/
log_retention_policy_in_days=7

[FILES]
master_spec_name=master_specs.xlsx
```

***

## ✅ **Logging**

* Logs are first written to `/tmp/sdmf_logs` for speed.
* After job completion, logs are moved to the final directory (`file_hunt_path`).
* Automatic cleanup of logs older than **7 days**.

***

## ✅ **Best Practices**

* Use **editable install** for development.
* Keep configs modular for different environments (Dev, QA, Prod).
* Ensure **DBFS or UC volumes** for persistent storage in Databricks.

***

## 📌 **Next Steps**

* Add **unit tests** for core modules.
* Integrate **structured logging** (JSON) for ELK/Splunk.
* Enable **compression for archived logs**.

***
