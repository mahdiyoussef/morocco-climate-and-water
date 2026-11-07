"""Generate operations panels using the metric names emitted by this platform."""
import json
from pathlib import Path

specs = [
    ('Verified climate source volume', 'morocco_verified_climate_source_bytes', 'bytes'),
    ('Gold location-months', 'morocco_gold_location_months', 'none'),
    ('Last successful publication', 'morocco_pipeline_last_success_timestamp_seconds * 1000', 'dateTimeAsIso'),
    ('Failed dbt nodes', 'morocco_dbt_failed_checks', 'none'),
    ('Medallion row counts', 'morocco_medallion_rows', 'none'),
    ('Internal dependency probes', 'morocco_dependency_reachable', 'none'),
    ('Quarantined records', 'morocco_quarantined_rows', 'none'),
    ('Task completions by state', 'sum by(state) (airflow_task_finished_total)', 'none'),
    ('PostgreSQL availability', 'pg_up', 'none'),
    ('Scrape targets', 'up', 'none'),
]
panels = []
for i, (title, expr, unit) in enumerate(specs):
    panels.append(dict(id=i + 1, title=title, type='stat' if i < 4 else 'timeseries',
                       gridPos=dict(x=(i % 2) * 12, y=(i // 2) * 8, w=12, h=8),
                       datasource=dict(type='prometheus', uid='morocco-prometheus'),
                       targets=[dict(refId='A', expr=expr, legendFormat='{{service}}{{stage}} {{table}}{{state}} {{job}}')],
                       fieldConfig=dict(defaults=dict(unit=unit), overrides=[])))
panels.append(dict(id=11, title='Airflow task logs', type='logs', gridPos=dict(x=0, y=40, w=24, h=10),
                   datasource=dict(type='loki', uid='morocco-loki'),
                   targets=[dict(refId='A', expr='{job="morocco_airflow"}')], options=dict(showTime=True)))
doc = dict(uid='morocco-operations', title='Morocco · Data Platform Operations', schemaVersion=40,
           version=1, refresh='30s', time={'from': 'now-6h', 'to': 'now'}, panels=panels,
           tags=['morocco', 'medallion', 'dbt'])
path = Path(__file__).parent / 'grafana/dashboards/operations.json'
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(doc, indent=2), encoding='utf-8')
