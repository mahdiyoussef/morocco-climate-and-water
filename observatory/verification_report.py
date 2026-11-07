"""Summarize recorded checks without claiming that static tests ran the cluster."""
import json
import sys
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from observatory.warehouse import validate_dbt_results
DATA = ROOT / 'data'


def main():
    dbt = json.loads((DATA / 'metadata/dbt_run_results.json').read_text())
    validate_dbt_results(dbt, 'duckdb')
    suites = ET.parse(ROOT / '.tools/morocco-tests.xml').getroot().findall('.//testsuite')
    tests = {key: sum(int(suite.attrib.get(key, 0)) for suite in suites) for key in ['tests', 'failures', 'errors', 'skipped']}
    if tests['failures'] or tests['errors'] or not tests['tests']:
        raise ValueError('Test results have not passed')
    with urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=5) as response:
        health = {'status': response.status, 'body': response.read().decode()}
    report = {
        'verified_at': datetime.now(timezone.utc).isoformat(),
        'scope': 'Real source-driven local medallion verification and dbt; dashboard application tests; static platform wiring',
        'dbt': {'target': 'local (DuckDB)', 'invocation_id': dbt['metadata']['invocation_id'],
                'statuses': dict(Counter(row['status'] for row in dbt['results']))},
        'silver': json.loads((DATA / 'metadata/silver_verification.json').read_text()),
        'pytest': tests,
        'streamlit_http_health': health,
        'powerbi': json.loads((ROOT / 'powerbi/validation.json').read_text()),
        'gold': json.loads((DATA / 'gold/provenance.json').read_text())['tables'],
        'distributed_stack_executed': False,
        'docker_compose_cli_validated_locally': False,
        'limitations': ['Docker engine is not installed in this workspace.',
                        'MinIO, Spark/Hive, Airflow, PostgreSQL and monitoring integration require the documented Docker smoke run.',
                        'Compose dependencies, mounts, ports and backend wiring were checked with YAML-based tests; the standalone CLI download did not complete.',
                        'No browser visual-rendering or Power BI Desktop execution check was performed.'],
    }
    path = DATA / 'metadata/platform_verification.json'
    path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    dbt_checks = sum(row['unique_id'].startswith('test.') for row in dbt['results'])
    print(f'Recorded local verification: {tests["tests"]} tests; {dbt_checks} dbt checks; cluster execution explicitly unverified.')


if __name__ == '__main__':
    main()
