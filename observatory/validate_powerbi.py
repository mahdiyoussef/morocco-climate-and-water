"""Validate PBIR JSON against Microsoft's schemas and check local model bindings.

This does not execute DAX or render visuals; those checks need Power BI Desktop.
"""
import csv
import argparse
import hashlib
import json
import re
import sys
import urllib.request
from pathlib import Path
import warnings

import jsonschema

ROOT = Path(__file__).resolve().parents[1]
BI = ROOT / 'powerbi'
CACHE = ROOT / '.tools/powerbi_schemas'


def fetch(url):
    CACHE.mkdir(parents=True, exist_ok=True)
    target = CACHE / (hashlib.sha256(url.encode()).hexdigest() + '.json')
    if not target.exists():
        request = urllib.request.Request(url, headers={'User-Agent': 'Morocco-PowerBI-Validation/1.0'})
        with urllib.request.urlopen(request, timeout=60) as response:
            target.write_bytes(response.read())
    return json.loads(target.read_text(encoding='utf-8-sig'))


def validate(path, schema_url):
    document = json.loads(path.read_text(encoding='utf-8'))
    schema = fetch(schema_url)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        resolver = jsonschema.RefResolver(base_uri=schema_url, referrer=schema, handlers={'https': fetch})
    jsonschema.Draft7Validator(schema, resolver=resolver).validate(document)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--postgres', action='store_true', help='Check live read-only PostgreSQL source columns and dimensions')
    args = parser.parse_args()
    pg = None
    if args.postgres:
        sys.path.insert(0, str(ROOT))
        from observatory.serving import connection
        pg = connection()
    documents = list((BI / 'Observatory.Report').rglob('*.json')) + [BI / 'Observatory.Report/definition.pbir']
    validated = 0
    for path in documents:
        doc = json.loads(path.read_text(encoding='utf-8'))
        if '$schema' in doc:
            validate(path, doc['$schema'])
            validated += 1
    validate(BI / 'Morocco.pbip', 'https://raw.githubusercontent.com/microsoft/powerbi-desktop-samples/main/item-schemas/common/pbip-1.0.json')
    model = json.loads((BI / 'Observatory.SemanticModel/model.bim').read_text())['model']
    tables = {table['name']: table for table in model['tables']}
    for table in tables.values():
        expression = '\n'.join(table['partitions'][0]['source']['expression'])
        source = re.search(r'Schema="gold", Item="([a-z_]+)"', expression)
        if source is None or 'PostgreSQL.Database(PostgresServer, PostgresDatabase' not in expression or 'File.Contents' in expression:
            raise ValueError('Unrecognized Gold source for ' + table['name'])
        if pg:
            headers = [row[0] for row in pg.execute('select column_name from information_schema.columns where table_schema=%s and table_name=%s', ('gold', source.group(1))).fetchall()]
        else:
            with (ROOT / 'data/gold' / (source.group(1) + '.csv')).open(encoding='utf-8', newline='') as stream:
                headers = csv.DictReader(stream).fieldnames or []
        missing = {column['sourceColumn'] for column in table['columns']} - set(headers)
        if missing:
            raise ValueError(f'{table["name"]}: missing Gold columns {sorted(missing)}')
    for rel in model['relationships']:
        for role in ('from', 'to'):
            table = tables[rel[role + 'Table']]
            if rel[role + 'Column'] not in {col['name'] for col in table['columns']}:
                raise ValueError('Invalid relationship ' + rel['name'])
    for path in (BI / 'Observatory.Report').rglob('visual.json'):
        doc = json.loads(path.read_text())
        for role in doc['visual'].get('query', {}).get('queryState', {}).values():
            for projection in role['projections']:
                kind, expression = next(iter(projection['field'].items()))
                table = tables[expression['Expression']['SourceRef']['Entity']]
                expected = table.get('measures', []) if kind == 'Measure' else table['columns']
                if expression['Property'] not in {c['name'] for c in expected}:
                    raise ValueError('Invalid visual field in ' + str(path))
    for filename, key in [('dim_location.csv', 'id'), ('dim_month.csv', 'year_month')]:
        with (ROOT / 'data/gold' / filename).open(encoding='utf-8') as stream:
            values = [row[key] for row in csv.DictReader(stream)]
        if len(values) != len(set(values)):
            raise ValueError('Duplicate dimension key in ' + filename)
    result = {'pbir_json_documents_validated': validated, 'pbip_validated': True,
              'gold_table_source_contracts_validated': len(tables),
              'serving_source': 'PostgreSQL', 'live_postgresql_source_verified': args.postgres,
              'relationship_and_visual_bindings_validated': True,
              'power_bi_desktop_rendering_verified': False,
              'limitation': 'Power BI Desktop is required to execute DAX, refresh the model and verify visual rendering.'}
    (BI / 'validation.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    if pg:
        pg.close()
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
