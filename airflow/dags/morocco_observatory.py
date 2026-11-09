"""Canonical acquisition -> MinIO -> Spark medallion -> dbt -> BI publication DAG."""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

from observatory.run_scope import DATA, SOURCE_STAGES, run_key, write_json

ROOT = '/workspace'


def record_source_quality(context):
    task = context['task_instance']
    passed = task.state == 'success'
    write_json(DATA / 'metadata' / f'quality_{task.task_id}.json',
               {'run_key': run_key(task.run_id), 'stage': task.task_id,
                'status': 'passed' if passed else 'failed', 'rule': 'source_stage_contract',
                'partition': 'source inputs', 'expected': 'verified stage completion', 'actual': task.state})


with DAG(
    dag_id='morocco_observatory',
    description='Verified historical climate and contemporary OSM; tested Gold publication',
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule='0 4 1 * *', catchup=False, max_active_runs=1,
    default_args={'owner': 'morocco-data', 'retries': 2, 'retry_delay': timedelta(minutes=3),
                  'execution_timeout': timedelta(hours=3)},
    tags=['morocco', 'medallion', 'dbt', 'minio'],
) as dag:
    def task(name, command):
        callback = record_source_quality if name in SOURCE_STAGES else None
        return BashOperator(task_id=name, bash_command='set -euo pipefail\n' + command,
                            cwd=ROOT, append_env=True, on_success_callback=callback, on_failure_callback=callback,
                            env={'PYTHONPATH': ROOT, 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
                                 'OBSERVATORY_AIRFLOW_RUN_ID': '{{ run_id }}',
                                 'OBSERVATORY_AIRFLOW_LOGICAL_DATE': '{{ logical_date.isoformat() }}'})

    acquire = task('acquire_and_verify', '''python observatory/download_climate.py --start-year 2015 --end-year 2024
python observatory/verify_climate.py
python observatory/download_era5.py --start-year 2015 --end-year 2024''')
    raw_landing = task('upload_verified_raw_landing', 'python -m observatory.storage sync-raw')
    normalize = task('decode_provider_formats', 'python observatory/warehouse.py prepare --from-landing')
    landing = task('upload_verified_minio_landing', 'python -m observatory.storage sync')
    spark = 'spark-submit --master spark://spark-master:7077 --conf spark.driver.host=airflow-scheduler --conf spark.cores.max=2 --conf spark.executor.memory=1g observatory/spark_medallion.py '
    bronze = task('spark_bronze', spark + 'bronze')
    silver = task('spark_silver_quality_gate', spark + 'silver')
    gold = task('dbt_build_gold', 'python -m observatory.guardrails build')
    docs = task('dbt_document_lineage', 'python -m observatory.guardrails docs')
    publish = task('publish_tested_bi_snapshot', 'python -m observatory.publish')
    acquire >> raw_landing >> normalize >> landing >> bronze >> silver >> gold >> docs >> publish
