## json


```json
{
  "vacuum_hours": 168,
  "partition_keys": [],

  "selection_schema": {
    "mode": "enforced",
    "schema": {
      "type": "struct",
      "fields": [
        {
          "name": "State ID",
          "type": "string",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "State",
          "type": "string",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "Year",
          "type": "integer",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "Population",
          "type": "float",
          "nullable": true,
          "metadata": {}
        }
      ]
    }
  },

  "ingestion_config": {

    /* =========================
       REQUEST
       ========================= */
    "request": {
      "url": "https://api.datausa.io/tesseract/data.jsonrecords",
      "method": "GET",
      "headers": {},
      "params": {
        "cube": "acs_yg_total_population_5",
        "measures": "Population",
        "drilldowns": "State,Year"
      },
      "body": null,
      "timeout": 30
    },

    /* =========================
       RESPONSE
       ========================= */
    "response": {
      "format": "json",               // json | parquet | csv
      "content_type": "auto",         // auto | application/json | application/parquet
      "encoding": "utf-8",
      "compression": "none",          // none | gzip | zip
      "file_layout": "single",        // single | multi
      "data_path": "data",            // JSON path (optional)
      "infer_schema": false
    },

    /* =========================
       PAGINATION
       ========================= */
    "pagination": {
      "enabled": true,
      "strategy": "offset_compound",  // page | cursor | offset_compound

      "params": {
        "limit": "limit",
        "offset": "offset",
        "page": "page",
        "cursor": "cursor"
      },

      "page_size": 100,
      "start": 0,
      "max_pages": 1000,

      "response_paths": {
        "cursor": "next_cursor",
        "total": "total",
        "page": "page"
      }
    },

    /* =========================
       RETRY
       ========================= */
    "retry": {
      "enabled": true,
      "max_attempts": 3,
      "backoff_factor": 2.0,
      "max_backoff": 30,
      "retry_statuses": [429, 500, 502, 503, 504]
    },

    /* =========================
       DEDUPLICATION
       ========================= */
    "deduplication": {
      "enabled": true,
      "strategy": "row_hash",         // none | distinct | row_hash
      "hash_columns": []              // empty = all columns
    },

    /* =========================
       METADATA
       ========================= */
    "metadata": {
      "source": "datausa",
      "feed_type": "api",
      "owner": "data-platform",
      "sla": "daily"
    }
  }
}

```