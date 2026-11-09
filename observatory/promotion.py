"""Promote one verified Gold export and recover across storage boundaries."""
import json
import re
import tempfile
from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from psycopg.types.json import Jsonb

from observatory.guardrails import verified_body
from observatory.run_scope import write_json
from observatory.warehouse import GOLD_TABLES

CURRENT = 'observatory/current.json'
ASSETS = {'snapshot.json', 'provenance.json', 'osm.geojson', 'footprint.geojson', 'dbt_run_results.json'}
METRICS = {'rainfall_mm': 50.0, 'temperature_c': 3.0, 'soil_moisture': .15}


def inspect_gold(staged, source, previous, run):
    meta = source['meta']
    sites = set(meta['site_ids'])
    years = meta['years']
    expected = {(site, year, month) for site in sites for year in years for month in range(1, 13)}
    fact = staged['fact_climate_monthly']
    rows = [dict(zip(fact['columns'], row, strict=True)) for row in fact['rows']]
    keys = {(row['site'], row['year'], row['month']) for row in rows}
    invalid_hours = sum(abs(row['hours'] - 24 * monthrange(row['year'], row['month'])[1])
                        for row in rows if (row['site'], row['year'], row['month']) in expected)
    report = {'run_key': run, 'stage': 'publish', 'partition': 'fact_climate_monthly',
              'expected': {'sites': meta['site_count'], 'months': len(years) * 12, 'rows': len(expected)},
              'actual': {'sites': len({key[0] for key in keys}), 'months': len({key[1:] for key in keys}),
                         'rows': len(rows)}, 'missing_keys': len(expected - keys),
              'unexpected_keys': len(keys - expected), 'duplicates': len(rows) - len(keys),
              'hour_discrepancy': invalid_hours, 'warnings': []}
    report['status'] = 'failed' if (not expected or len(sites) != meta['site_count'] or keys != expected
                                    or report['duplicates'] or invalid_hours) else 'passed'
    report['rule'] = 'gold_site_month_hour_coverage'
    if report['status'] == 'passed' and previous:
        def seasonal(records):
            groups = defaultdict(list)
            for row in records:
                groups[row['site'], row['month']].append(row)
            return {key: {metric: mean(row[metric] for row in group) for metric in METRICS}
                    for key, group in groups.items()}

        baseline = seasonal(previous.get('monthly', []))
        candidate = seasonal(rows)
        for key in sorted(baseline.keys() & candidate.keys()):
            for metric, threshold in METRICS.items():
                old, new = baseline[key][metric], candidate[key][metric]
                if abs(new - old) >= threshold:
                    report['warnings'].append({'rule': 'seasonal_drift', 'partition': list(key),
                                               'metric': metric, 'expected': old, 'actual': new})
    return report


def stage_download(s3, receipt, directory):
    version = receipt['publication_id']
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', version):
        raise ValueError('Invalid export publication ID')
    prefix = f'observatory/runs/{version}/'
    expected = ASSETS | {f'{table}.csv' for table in GOLD_TABLES}
    names = set()
    for entry in receipt['files']:
        name = entry['key'].removeprefix(prefix)
        if name not in expected or name in names:
            raise ValueError('Invalid or duplicate object in the promotion receipt')
        names.add(name)
        (directory / name).write_bytes(verified_body(s3, 'exports', entry))
    if names != expected:
        raise ValueError('Promotion receipt is missing required exports')
    for name in ('snapshot.json', 'provenance.json'):
        document = json.loads((directory / name).read_text())
        if document.get('meta', document).get('publication_id') != version:
            raise ValueError(f'{name} does not match the committed publication')
    if json.loads((directory / 'provenance.json').read_text())['tables'] != receipt['tables']:
        raise ValueError('Gold export row counts do not match the promotion receipt')


def finish(conn, s3, data, receipt):
    """Replay the local and MinIO writes from the committed PostgreSQL receipt."""
    version = receipt['publication_id']
    gold = data / 'gold'
    gold.mkdir(parents=True, exist_ok=True)
    pending = gold / '.promotion-pending'
    with tempfile.TemporaryDirectory(dir=gold.parent) as directory:
        staged = Path(directory)
        stage_download(s3, receipt, staged)
        write_json(pending, {'publication_id': version})
        for file in staged.iterdir():
            file.replace(gold / file.name)
        pointer = {name: receipt[name] for name in ('publication_id', 'published_at', 'files')}
        s3.put_object(Bucket='exports', Key=CURRENT, Body=json.dumps(pointer).encode(),
                      ContentType='application/json')
        remote = json.loads(s3.get_object(Bucket='exports', Key=CURRENT)['Body'].read())
        local = json.loads((gold / 'snapshot.json').read_text())['meta']['publication_id']
        if remote != pointer or local != version:
            raise ValueError('MinIO or local publication does not match PostgreSQL')
        state = {'publication_id': version, 'last_success_timestamp_seconds': datetime.fromisoformat(receipt['published_at']).timestamp(),
                 'gold_rows': receipt['tables']['fact_climate_monthly'], 'dbt_tests_failed': 0}
        write_json(data / 'metadata' / 'dbt_run_results.json', json.loads((gold / 'dbt_run_results.json').read_text()))
        write_json(data / 'metadata' / 'publication.json', state)
        report = json.loads((gold / 'provenance.json').read_text()).get('quality', {})
        if report:
            write_json(data / 'metadata/quality_publish.json', {**report, 'status': 'passed'})
        pending.unlink()
        conn.execute("update gold.dashboard_metadata set payload = %s where name = 'promotion'",
                     (Jsonb({**receipt, 'complete': True}),))


def timestamp():
    return datetime.now(timezone.utc).isoformat()
