"""Spark/Delta Bronze and Silver stages, stored on MinIO and registered in Hive."""
import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from observatory.contracts import TABLES, cast_query, dedupe_query, validity


def emit(name, value):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(f'morocco.{name}:{value}|g'.encode(), (os.environ.get('STATSD_HOST', 'statsd-exporter'), 8125))
    except OSError:
        pass


def session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName('morocco-medallion')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.hadoop.hive.metastore.uris', os.environ.get('HIVE_METASTORE_URI', 'thrift://hive-metastore:9083'))
            .config('spark.sql.hive.metastore.version', '3.1.3')
            .config('spark.sql.hive.metastore.jars', 'maven')
            .config('spark.hadoop.fs.s3a.endpoint', os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'))
            .config('spark.hadoop.fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])
            .config('spark.hadoop.fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.sql.warehouse.dir', 's3a://warehouse/morocco')
            .enableHiveSupport().getOrCreate())


def run(stage):
    from pyspark.sql import functions as F
    spark = session()
    started = time.time()
    audits = []
    try:
        spark.sql(f"create database if not exists {stage} location 's3a://{stage}/morocco'")
        if stage == 'silver':
            spark.sql("create database if not exists quarantine location 's3a://silver/observatory/_quarantine'")
        for table in TABLES:
            if stage == 'bronze':
                frame = spark.read.option('recursiveFileLookup', 'true').parquet(f's3a://landing/observatory/normalized/{table}/')
                frame = (frame.withColumn('_row_hash', F.sha2(F.to_json(F.struct(*[F.col(c) for c in frame.columns])), 256))
                         .withColumn('_source_file', F.input_file_name()).withColumn('_ingested_at', F.current_timestamp()))
                frame.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('path', f's3a://bronze/observatory/{table}').saveAsTable(f'bronze.{table}')
                count = spark.table(f'bronze.{table}').count()
                audits.append({'table': table, 'accepted': count, 'rejected': 0})
            else:
                spark.sql(cast_query(table)).createOrReplaceTempView('candidates')
                rejected = spark.sql(f"select *, 'invalid_contract' as reject_reason from candidates where not coalesce(({validity(table)}), false)")
                rejected.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('path', f's3a://silver/observatory/_quarantine/{table}').saveAsTable(f'quarantine.{table}')
                rejected_count = rejected.count()
                emit(f'dq.rejected.{table}', rejected_count)
                if rejected_count:
                    raise ValueError(f'{table}: {rejected_count} records quarantined; Gold publication blocked')
                clean = spark.sql(dedupe_query(table))
                clean.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('path', f's3a://silver/observatory/{table}').saveAsTable(f'silver.{table}')
                count = spark.table(f'silver.{table}').count()
                audits.append({'table': table, 'accepted': count, 'rejected': rejected_count})
            emit(f'rows.{stage}.{table}', count)
            print(json.dumps({'stage': stage, **audits[-1]}), flush=True)
        emit(f'duration.{stage}', time.time() - started)
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['bronze', 'silver'])
    run(parser.parse_args().stage)
