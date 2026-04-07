#!/bin/bash

wait_for_port() {
  local host=$1; local port=$2; local service=$3
  echo "wait for $service ($host:$port)..."
  while ! timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; do sleep 2; done
  echo "$service ready!"
}

wait_for_port "postgres" "5432" "PostgreSQL"
wait_for_port "clickhouse" "8123" "ClickHouse"

echo "starting Spark ETL..."

JARS=$(ls /opt/spark/custom_jars/*.jar | tr '\n' ',' | sed 's/,$//')
CP=$(ls /opt/spark/custom_jars/*.jar | tr '\n' ':' | sed 's/:$//')

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "$JARS" \
  --driver-class-path "$CP" \
  /opt/spark/jobs/etl.py 2>&1 | tee /opt/spark/jobs/spark.log