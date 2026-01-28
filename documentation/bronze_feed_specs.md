# **JSON Schema Documentation**

This document describes the structure, rules, and validation checks defined in the provided JSON schema.
It is written in a **dataset-agnostic**, **domain-neutral** format suitable for metadata frameworks, ingestion pipelines, or validation engines.

---

## **1. Schema Overview**

The schema defines:

* Primary key configuration
* Composite key definitions
* Partitioning information
* Selection-level schema describing fields and types
* Column-level standard validation checks
* Dataset-wide comprehensive rules

The schema is designed to be used by automated validation or processing systems.

---

## **2. Key Configuration**

### **Primary Key**

Defines the logical primary key of the dataset.

| Key           | Value      |
| ------------- | ---------- |
| `primary_key` | `id/item_id/location_id` |

### **Composite Keys**

List of columns that form a multi-column key.

```
[item_id, location_id]
```

### **Partition Keys**

Columns used for partition-based storage or computation.

```
[location_id, item_id]
```

---

## **3. Selection Schema**

Defines the structural schema of the dataset.

### **Type:** `struct`

### **Fields**

| Field Name | Type   | Nullable | Description                            |
| ---------- | ------ | -------- | -------------------------------------- |
| `alpha3_b` | string | true     | Field in schema (example placeholder). |
| `alpha3_t` | string | true     | Field in schema (example placeholder). |
| `alpha2`   | string | true     | Field in schema (example placeholder). |
| `english`  | string | true     | Field in schema (example placeholder). |


---

## **4. Standard Checks**

Standard checks apply to specific columns based on configured rules.

| Column Name    | Check Sequence                      | Threshold | Meaning                              |
| -------------- | ----------------------------------- | --------- | ------------------------------------ |
| `alpha3-b`     | `_check_primary_key`                | `0`       | Enforces primary key constraints. Threshold should be in float 0 to 1   |
| `English Name` | `_check_nulls`, `_check_duplicates` | `0`       | Enforces no nulls and no duplicates. Threshold should be in float 0 to 1  |

---

## **5. Comprehensive Checks**

Rules that apply at the dataset or batch level.

| Rule No | Rule Query | Processing Stage |SEVERITY| Description                        |
| ------- | ---------- | ---------------- |--------| ---------------------------------- |
| `1`     | *SELECT * FROM <Stage INCR DATASET> WHERE <Some COLUMN> IS NULL AND <SOME OTHER COLUMN> >= 1;*  | <pre_load / post_load>        |<WARNING/ERROR>| Some checks are complex which should be performed before or after loading data. Pre load checks if failed would stop ingestion. |

---

## **6. Full JSON Schema**

```json
{
    "primary_key": "alpha3-b",
    "composite_key": [],
    "partition_keys": [],
    "selection_schema": {
        "type": "struct",
        "fields": [
            {
                "name": "alpha3_b",
                "type": "string",
                "nullable": true
            },
            {
                "name": "alpha3_t",
                "type": "string",
                "nullable": true
            },
            {
                "name": "alpha2",
                "type": "string",
                "nullable": true
            },
            {
                "name": "english",
                "type": "string",
                "nullable": true
            }
        ]
    },
    "standard_checks": [
        {
            "check_sequence": [
                "_check_primary_key"
            ],
            "column_name": "alpha3-b",
            "threshold": 0
        },
        {
            "check_sequence": [
                "_check_nulls",
                "_check_duplicates"
            ],
            "column_name": "English Name",
            "threshold": 0
        }
    ],
    "comprehensive_checks": [
        {
            "rule_no": 1,
            "rule_query": "SELECT * FROM yals_world_uc.staging.t_source_incr_country_codes WHERE len(alpha3_b) >= 2;",
            "processing_stage": "pre_load",
            "severity":"WARNING"
        }
    ]
}
```
