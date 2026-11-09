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
from observatory.run_scope import DATA, active_key, receipt_path, schema, write_json


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
    key = active_key()
    namespace = schema(stage)
    quarantine = schema('quarantine')
    prefix = f'observatory/runs/{key}' if key else 'observatory'
    if stage == 'bronze' or key:
        from observatory.storage import STAGING_MANIFEST, client, read_manifest, staged_inputs
        s3 = client()
        if key:
            _, manifest_digest = read_manifest(s3, STAGING_MANIFEST, key)
            evidence = receipt_path().parent
            if stage == 'silver':
                previous = json.loads((evidence / 'bronze.json').read_text())
                if previous['manifest_sha256'] != manifest_digest:
                    raise ValueError('Normalized Landing changed after Bronze')
        if stage == 'bronze':
            inputs = staged_inputs(s3)
            if key and read_manifest(s3, STAGING_MANIFEST, key)[1] != manifest_digest:
                raise ValueError('Normalized Landing changed while reading Bronze inputs')
    spark = session()
    started = time.time()
    audits = []
    table = None
    report = {'run_key': key, 'stage': stage, 'status': 'running', 'tables': audits}
    report_path = DATA / 'metadata' / f'quality_spark_{stage}.json'
    if key:
        (evidence / f'{stage}.json').unlink(missing_ok=True)
        write_json(report_path, report)
    try:
        spark.sql(f"create database if not exists {namespace} location 's3a://{stage}/{prefix}'")
        if stage == 'silver':
            spark.sql(f"create database if not exists {quarantine} location 's3a://silver/{prefix}/_quarantine'")
        for table in TABLES:
            if stage == 'bronze':
                frame = spark.read.option('recursiveFileLookup', 'true').parquet(*inputs[table])
                if not frame.take(1):
                    raise ValueError(f'{table}: empty Bronze input; Gold publication blocked')
                frame = (frame.withColumn('_row_hash', F.sha2(F.to_json(F.struct(*[F.col(c) for c in frame.columns])), 256))
                         .withColumn('_source_file', F.input_file_name()).withColumn('_ingested_at', F.current_timestamp()))
                frame.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('path', f's3a://bronze/{prefix}/{table}').saveAsTable(f'{namespace}.{table}')
                count = spark.table(f'{namespace}.{table}').count()
                audits.append({'table': table, 'accepted': count, 'rejected': 0})
            else:
                spark.sql(cast_query(table, bronze_schema=schema('bronze'))).createOrReplaceTempView('candidates')
                candidates = spark.table('candidates')
                if not candidates.take(1):
                    raise ValueError(f'{table}: empty Silver input; Gold publication blocked')
                rejected = spark.sql(f"select *, 'invalid_contract' as reject_reason from candidates where not coalesce(({validity(table)}), false)")
                rejected.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('path', f's3a://silver/{prefix}/_quarantine/{table}').saveAsTable(f'{quarantine}.{table}')
                rejected_count = rejected.count()
                emit(f'dq.rejected.{table}', rejected_count)
                input_count = candidates.count() if key else None
                if rejected_count:
                    audits.append({'table': table, 'input_rows': input_count, 'accepted': 0,
                                   'rejected': rejected_count, 'duplicates': 0})
                    raise ValueError(f'{table}: {rejected_count} records quarantined; Gold publication blocked')
                clean = spark.sql(dedupe_query(table))
                clean.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('path', f's3a://silver/{prefix}/{table}').saveAsTable(f'{namespace}.{table}')
                count = spark.table(f'{namespace}.{table}').count()
                audits.append({'table': table, 'accepted': count, 'rejected': rejected_count,
                               'input_rows': input_count, 'duplicates': input_count - count if key else 0})
            emit(f'rows.{stage}.{table}', count)
            print(json.dumps({'stage': stage, **audits[-1]}), flush=True)
        emit(f'duration.{stage}', time.time() - started)
        if key:
            report.update(status='passed', manifest_sha256=manifest_digest)
            write_json(evidence / f'{stage}.json', report)
    except Exception:
        report.update(status='failed', rule=f'{stage}_input_contract', partition=table,
                      expected='nonempty valid input', actual='stage failed')
        raise
    finally:
        if key:
            write_json(report_path, report)
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['bronze', 'silver'])
    run(parser.parse_args().stage)
