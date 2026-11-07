"""Expose persisted pipeline evidence and internal dependency probes to Prometheus."""
import json
import logging
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from prometheus_client import Gauge, start_http_server

DATA = Path('/workspace/data')
logging.basicConfig(level=logging.INFO)
last_success = Gauge('morocco_pipeline_last_success_timestamp_seconds', 'Timestamp of last committed production publication')
gold_rows = Gauge('morocco_gold_location_months', 'Location-months in the last committed publication')
dbt_failures = Gauge('morocco_dbt_failed_checks', 'Failed/error dbt nodes from the latest build artifact')
source_bytes = Gauge('morocco_verified_climate_source_bytes', 'Acquisition-manifest climate bytes, not derived file size')
publication_exists = Gauge('morocco_publication_exists', 'Whether a successful production publication has been recorded')
reachable = Gauge('morocco_dependency_reachable', 'Internal TCP dependency probe; not an application-level health check', ['service'])


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
    path = DATA / 'metadata/publication.json'
    publication_exists.set(int(path.exists()))
    if path.exists():
        state = json.loads(path.read_text())
        last_success.set(state['last_success_timestamp_seconds'])
        gold_rows.set(state['gold_rows'])
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
