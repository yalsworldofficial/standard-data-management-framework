#!/bin/bash

echo "Configuring Spark FAIR scheduler..."

cat <<EOF >> /databricks/spark/conf/spark-defaults.conf
spark.scheduler.mode FAIR
EOF

echo "Spark FAIR scheduler enabled."
