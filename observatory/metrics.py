"""Expose persisted pipeline evidence and internal dependency probes to Prometheus."""
import json
import logging
import socket
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from prometheus_client import Gauge, start_http_server

from observatory.run_scope import SOURCE_STAGES

DATA = Path('/workspace/data')
logging.basicConfig(level=logging.INFO)
last_success = Gauge('morocco_pipeline_last_success_timestamp_seconds', 'Timestamp of last committed production publication')
gold_rows = Gauge('morocco_gold_location_months', 'Location-months in the last committed publication')
dbt_failures = Gauge('morocco_dbt_failed_checks', 'Failed/error dbt nodes from the latest build artifact')
source_bytes = Gauge('morocco_verified_climate_source_bytes', 'Acquisition-manifest climate bytes, not derived file size')
publication_exists = Gauge('morocco_publication_exists', 'Whether a successful production publication has been recorded')
reachable = Gauge('morocco_dependency_reachable', 'Internal TCP dependency probe; not an application-level health check', ['service'])


guardrail_failed = Gauge('morocco_guardrail_failed', 'Latest guardrail outcome per stage', ['stage'])
promotion_pending = Gauge('morocco_promotion_pending', 'Whether local exports need publication recovery')
drift_warnings = Gauge('morocco_seasonal_drift_warnings', 'Seasonal warnings in the latest candidate Gold snapshot')
source_verified = Gauge('morocco_source_verified_timestamp_seconds', 'Time the provider inputs were last verified', ['provider'])
gold_coverage = Gauge('morocco_quality_coverage', 'Expected and actual coverage', ['kind', 'measure'])
silver_duplicates = Gauge('morocco_silver_duplicate_rows', 'Rows removed during Silver deduplication', ['table'])
hour_discrepancy = Gauge('morocco_gold_hour_discrepancy', 'Absolute discrepancy in expected observation hours')


def update_quality():
    for stage in (*SOURCE_STAGES, 'spark_bronze', 'spark_silver', 'dbt', 'publish'):
        path = DATA / 'metadata' / f'quality_{stage}.json'
        report = json.loads(path.read_text()) if path.exists() else {}
        guardrail_failed.labels(stage).set(int(report.get('status') == 'failed'))
        if stage == 'spark_silver':
            for table in report.get('tables', []):
                silver_duplicates.labels(table['table']).set(table.get('duplicates', 0))
        if stage == 'publish':
            drift_warnings.set(len(report.get('warnings', [])))
            hour_discrepancy.set(report.get('hour_discrepancy', 0))
            promotion_pending.set(int(report.get('status') == 'promoting'
                                      or (DATA / 'gold/.promotion-pending').exists()))
            for kind in ('expected', 'actual'):
                measurements = report.get(kind)
                if isinstance(measurements, dict):
                    for measure, value in measurements.items():
                        gold_coverage.labels(kind, measure).set(value)
    for provider, filename, field in (
        ('chirps', 'climate_downloads.json', 'final_verification_utc'),
        ('era5_land', 'era5_downloads.json', 'verified_at_utc'),
        ('osm', 'osm_downloads.json', 'verified_at'),
    ):
        path = DATA / 'metadata' / filename
        value = json.loads(path.read_text()).get(field) if path.exists() else None
        source_verified.labels(provider).set(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() if value else 0)


class Alerts(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != '/alerts':
            self.send_error(404)
            return
        size = min(int(self.headers.get('Content-Length', '0')), 1_000_000)
        payload = json.loads(self.rfile.read(size))
        for alert in payload.get('alerts', []):
            logging.warning('platform_alert %s', json.dumps({'status': alert.get('status'), 'labels': alert.get('labels'), 'annotations': alert.get('annotations')}))
        self.send_response(200)
        self.end_headers()


def update():
    update_quality()
    path = DATA / 'metadata/publication.json'
    publication_exists.set(int(path.exists()))
    if path.exists():
        state = json.loads(path.read_text())
        last_success.set(state['last_success_timestamp_seconds'])
        gold_rows.set(state['gold_rows'])
    artifact = DATA / 'metadata/dbt_candidate_run_results.json'
    if not artifact.exists():
        artifact = DATA / 'metadata/dbt_run_results.json'
    if artifact.exists():
        checks = json.loads(artifact.read_text())['results']
        dbt_failures.set(sum(row['status'] not in ['success', 'pass'] for row in checks if not row['unique_id'].startswith('exposure.')))
    manifest = DATA / 'metadata/climate_downloads.json'
    if manifest.exists():
        recorded = json.loads(manifest.read_text())
        source_bytes.set(sum(row['bytes'] for row in recorded['files']) if recorded.get('final_verification_utc') else 0)
    for service, host, port in [('postgres', 'postgres', 5432), ('hive', 'hive-metastore', 9083),
                                ('spark_sql', 'spark-thrift-server', 10000), ('airflow_scheduler', 'airflow-scheduler', 8974)]:
        try:
            with socket.create_connection((host, port), timeout=2):
                reachable.labels(service).set(1)
        except OSError:
            reachable.labels(service).set(0)


def main():
    start_http_server(9105)
    threading.Thread(target=HTTPServer(('0.0.0.0', 9106), Alerts).serve_forever, daemon=True).start()
    while True:
        try:
            update()
        except (ValueError, OSError, KeyError):
            logging.exception('metrics refresh failed')
        time.sleep(15)


if __name__ == '__main__':
    main()
