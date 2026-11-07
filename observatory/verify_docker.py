"""Verify the running Docker pipeline against MinIO, Spark, PostgreSQL and Streamlit."""
import json
import os
from datetime import datetime, timezone
from urllib.request import urlopen

from pyhive import hive
from psycopg import sql

from observatory.contracts import TABLES
from observatory.serving import connection
from observatory.storage import DATA, client
from observatory.warehouse import GOLD_TABLES, validate_dbt_results


def main():
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import create_session

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == 'morocco_observatory').order_by(DagRun.execution_date.desc()).first()
        if run is None or run.state != 'success':
            raise ValueError('The latest Airflow DAG run must be successful')
        tasks = {task.task_id: task.state for task in session.query(TaskInstance).filter(
            TaskInstance.dag_id == run.dag_id, TaskInstance.run_id == run.run_id).all()}
        if len(tasks) != 9 or any(state != 'success' for state in tasks.values()):
            raise ValueError('All nine Airflow stages must be successful')
        airflow = {'run_id': run.run_id, 'state': run.state, 'tasks': tasks}
    s3 = client()
    raw = json.loads(s3.get_object(Bucket='landing', Key='observatory/metadata/raw_manifest.json')['Body'].read())
    for entry in raw['files']:
        remote = s3.head_object(Bucket='landing', Key=entry['key'])
        if remote['ContentLength'] != entry['bytes'] or remote['Metadata'].get('sha256') != entry['sha256']:
            raise ValueError('Landing object does not match its committed manifest')
    delta = {}
    for bucket, tables in [('bronze', TABLES), ('silver', TABLES), ('gold', GOLD_TABLES)]:
        keys = [obj['Key'] for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket)
                for obj in page.get('Contents', [])]
        delta[bucket] = {}
        for table in tables:
            prefix = f'{"morocco" if bucket == "gold" else "observatory"}/{table}/_delta_log/'
            commits = [key for key in keys if key.startswith(prefix) and key.endswith('.json')]
            if not commits:
                raise ValueError(f'Missing Delta commits for {bucket}.{table}')
            delta[bucket][table] = len(commits)
    with connection() as postgres:
        marker = postgres.execute('select publication_id,table_counts from gold.publication where singleton').fetchone()
        metadata = dict(postgres.execute('select name,payload from gold.dashboard_metadata').fetchall())
        validate_dbt_results(metadata['dbt_run_results'], 'spark')
        if metadata['snapshot']['meta']['gold_transformation_engine'] != 'dbt-spark':
            raise ValueError('PostgreSQL must contain the distributed Spark publication')
        counts = {table: postgres.execute(sql.SQL('select count(*) from gold.{}').format(sql.Identifier(table))).fetchone()[0]
                  for table in GOLD_TABLES}
        if counts != marker[1]:
            raise ValueError('PostgreSQL does not match its publication receipt')
    spark = hive.Connection(host=os.environ.get('SPARK_THRIFT_HOST', 'spark-thrift-server'), port=10000, auth='NOSASL')
    try:
        cursor = spark.cursor()
        for table, count in counts.items():
            cursor.execute(f'select count(*) from gold.{table}')
            if cursor.fetchone()[0] != count:
                raise ValueError(f'Spark/PostgreSQL reconciliation failed for {table}')
    finally:
        spark.close()
    pointer = json.loads(s3.get_object(Bucket='exports', Key='observatory/current.json')['Body'].read())
    if pointer['publication_id'] != marker[0]:
        raise ValueError('MinIO exports and PostgreSQL publication IDs differ')
    with urlopen('http://streamlit:8501/_stcore/health', timeout=10) as response:
        health = response.status
        if health != 200:
            raise ValueError('Streamlit is unhealthy')
    result = {'verified_at': datetime.now(timezone.utc).isoformat(), 'distributed_stack_executed': True,
              'publication_id': marker[0], 'airflow': airflow, 'landing_objects_verified': len(raw['files']),
              'raw_source_bytes': sum(row['bytes'] for row in raw['files'] if '/raw/' in row['key']),
              'delta_commits': delta, 'gold_row_counts': counts, 'dbt_target': 'spark',
              'streamlit_http_health': health}
    (DATA / 'metadata/docker_verification.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
