#!/bin/bash

set -a; source .env; set +a

mkdir -p jars
cd jars

echo "Clean up"
rm -f *.jar

echo "downloading jars for Spark ${SPARK_VERSION}..."

echo "downloading Postgres JDBC ${PG_JDBC_VER}..."
curl -L -O "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VER}/postgresql-${PG_JDBC_VER}.jar"

echo "downloading ClickHouse JDBC ${CH_JDBC_VER}..."
curl -L -O "https://github.com/ClickHouse/clickhouse-java/releases/download/v${CH_JDBC_VER}/clickhouse-jdbc-${CH_JDBC_VER}-all.jar"

mv *.jar jars/
echo "Done!"
cd ..