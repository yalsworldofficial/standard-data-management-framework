# Standard Data Management Framework (SDMF)

A **modular, scalable, and Python-based Data Management Framework** designed to standardize **data ingestion, validation, transformation, metadata handling, and storage** across enterprise workflows.

This framework eliminates repetitive boilerplate and provides a consistent structure for building reliable, maintainable data pipelines.

## About

Created and maintained by **Harsh Handoo**, Data Engineer, SDMF is designed to standardize common data movement patterns and reduce boilerplate in real-world Spark workloads.

SDMF (Standard Data Management Framework) is an open-source Spark-based data engineering framework built for reliable, production-grade data pipelines. It focuses on schema enforcement, incremental processing, and SCD Type-2 handling using Delta Lake.

## Features

- **Modular Design** – Plug-and-play components for ingestion, validation, transformation, and storage.
- **Schema Alignment & Partitioning** – Built-in support for CDC (Change Data Capture) and MERGE operations.
- **Metadata Management** – Centralized handling of feed specifications and lineage.
- **Scalable** – Works seamlessly with **Spark**, **Delta Lake**, and distributed environments like **Databricks**.
- **Logging & Monitoring** – Custom logging with retention and rotation policies.

## Installation

```bash
pip install sdmf
```

## Requirements

### Cluster Resources (Typical)

| Workload                       | Minimum              | Recommended             |
| ------------------------------ | -------------------- | ----------------------- |
| Local development              | 4 vCPU, 8 GB RAM     | 8 vCPU, 16 GB RAM       |
| Small datasets (<10M rows)     | 2 executors × 4 GB   | 4 executors × 8 GB      |
| Medium datasets (10–100M rows) | 4 executors × 8 GB   | 8 executors × 16 GB     |
| Large datasets (>100M rows)    | 8+ executors × 16 GB | Cluster-specific tuning |

### Recommended Production Setup

- Linux-based Spark cluster
- Spark FAIR scheduler enabled
- Delta Lake tables stored on cloud object storage
- Versioned releases via PyPI + GitHub Releases

### Storage

- Local filesystem (dev only)
- HDFS / ADLS / S3 / GCS (recommended)
- DBFS (Databricks)

### Operating System

- Linux (recommended)
- macOS
- Windows (WSL recommended for local development)

⚠️ Production deployments are strongly recommended on Linux-based systems.

Note: **This library is tested on databricks.**

## Usage

## Prerequisites

- Dedicate a directory to SDMF. Example: /sdmf_dir/
- Setup `config.ini` file.

    ```ini
    [DEFAULT]
    outbound_directory_name=sdmf_outbound
    log_directory_name=sdmf_logs
    temp_log_location=/sdmf_dir/temp
    file_hunt_path=/sdmf_dir/
    log_retention_policy_in_days=7
    max_concurrent_batches=4

    [FILES]
    master_spec_name = master_specs.xlsx

    [LINEAGE_DIAGRAM]
    BOX_WIDTH=4.4
    BOX_HEIGHT=2.2
    X_GAP=2.0
    Y_GAP=2.5
    ROOT_GAP=2.0
    ```

- Setup Master Spec `master_spec.xlsx` (can be renamed in config) file.

  - feed_id
  - system_name
  - subsystem_name
  - category
  - sub_category
  - data_flow_direction
  - residing_layer
  - feed_name
  - feed_type
  - feed_specs
  - load_type
  - target_unity_catalog
  - target_schema_name
  - target_table_name
  - suggested_feed_name
  - parallelism_group_number
  - parent_feed_id
  - is_active

- Feed Spec JSON

    ```json
    {
        "primary_key": "col1",
        "composite_key": [],
        "partition_keys": [],
        "vacuum_hours": 168,
        "source_table_name": "test.test",
        "selection_query":null,
        "selection_schema": {
            "type": "struct",
            "fields": [
                {
                    "name": "col1",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "comment": "test"
                    }
                },
                {
                    "name": "col2",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "comment": "test"
                    }
                },
                {
                    "name": "col3",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "comment": "test"
                    }
                },
                {
                    "name": "col4",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "comment": "test"
                    }
                }
            ]
        },
        "standard_checks": [
            {
                "check_sequence": [
                    "_check_primary_key"
                ],
                "column_name": "col1",
                "threshold": 0
            },
            {
                "check_sequence": [
                    "_check_nulls"
                ],
                "column_name": "col2",
                "threshold": 0
            }
        ],
        "comprehensive_checks": [
            {
                "check_name": "Some unique check name",
                "query": "Select 1;",
                "severity": "WARNING",
                "threshold": 0,
                "load_stage": "PRE_LOAD",
                "dependency_dataset": []
            },
            {
                "check_name": "Some unique check name 1",
                "query": "Select 1;",
                "severity": "WARNING",
                "threshold": 0,
                "load_stage": "PRE_LOAD",
                "dependency_dataset": []
            },
            {
                "check_name": "Some unique check name 2",
                "query": "Select 1;",
                "severity": "WARNING",
                "threshold": 0,
                "load_stage": "PRE_LOAD",
                "dependency_dataset": []
            },
            {
                "check_name": "Some unique check name 3",
                "query": "Select 1;",
                "severity": "WARNING",
                "threshold": 0,
                "load_stage": "POST_LOAD",
                "dependency_dataset": [
                    "demo.customers"
                ]
            }
        ]
    }
    ```

- Ensure Spark FAIR scheduler is enabled.

    ```bash
    #!/bin/bash

    echo "Configuring Spark FAIR scheduler..."

    cat <<EOF >> /databricks/spark/conf/spark-defaults.conf
    spark.scheduler.mode FAIR
    EOF

    echo "Spark FAIR scheduler enabled."
    ```

    ```python
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
            .appName("SDMF")
            .config("spark.scheduler.mode", "FAIR")
            .getOrCreate()
    )
    ```

## Execution

```python
import configparser
from sdmf import Orchestrator

spark # available spark session

cfg = configparser.ConfigParser()
cfg.read("/sdmf_dir/config.ini")
myOrchestrator = Orchestrator(spark, config=cfg)
myOrchestrator.run()
```

## Logging

- Logs are first written to specified log directory in `config.ini`.
