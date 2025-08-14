#!/bin/bash

# Finance Data Processing Cluster initialization script

echo "Starting Finance Data Processing Cluster initialization..."

# Create log directories
mkdir -p /dbfs/logs/finance_pipeline
chmod 777 /dbfs/logs/finance_pipeline

# Create checkpoint directories
mkdir -p /dbfs/checkpoints/finance_pipeline
chmod 777 /dbfs/checkpoints/finance_pipeline

# Install additional system packages if needed
apt-get update
apt-get install -y jq

# Set environment variables
echo "export FINANCE_DATA_ENV=production" >> /databricks/spark/conf/spark-env.sh

# Configure custom metrics collection
cat > /tmp/metrics.properties << EOF
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=metrics.internal
*.sink.graphite.port=2003
*.sink.graphite.period=10
*.sink.graphite.prefix=finance.data.pipeline
EOF

cp /tmp/metrics.properties /databricks/spark/conf/metrics.properties

echo "Finance Data Processing Cluster initialization completed."