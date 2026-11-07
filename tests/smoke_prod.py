"""Standalone bundle acceptance check. Run with cwd set to the isolated prod folder."""
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path

from streamlit.testing.v1 import AppTest

root = Path.cwd()
assert importlib.util.find_spec('observatory') is None, 'Pipeline package must be absent from this isolated check'
manifest_body = (root / 'bundle_manifest.json').read_bytes()
manifest = json.loads(manifest_body)
for name, info in manifest['files'].items():
    body = (root / name).read_bytes()
    assert len(body) == info['bytes'] and hashlib.sha256(body).hexdigest() == info['sha256'], name
for line in (root / 'requirements.txt').read_text().splitlines():
    name, version = line.split('==')
    assert importlib.metadata.version(name) == version

app = AppTest.from_file(root / 'streamlit_app.py', default_timeout=90).run()
assert not app.exception and not app.error
app.toggle(key='online_map').set_value(False).run()
pages = list(app.radio(key='page').options)
assert len(pages) == 7 and 'Indicators' in pages
for page in pages:
    app.radio(key='page').set_value(page).run()
    assert not app.exception and not app.error, page
    print('Passed standalone page:', page)
app.slider(key='years').set_value((2015, 2024)).run()
app.radio(key='page').set_value('Indicators').run()
before = next(metric.value for metric in app.metric if metric.label == 'Annual rainfall variability')
app.multiselect(key='months').set_value([]).run()
assert not app.exception and not app.error
assert next(metric.value for metric in app.metric if metric.label == 'Annual rainfall variability') == before
app.multiselect(key='locations').set_value([]).run()
assert any('Choose at least one location' in item.value for item in app.info)

report = {'publication_id': manifest['publication_id'], 'pages_passed': pages,
          'network': 'disabled by Docker', 'inherited_backend': os.environ.get('OBSERVATORY_DATA_BACKEND'),
          'pipeline_package_available': False, 'requirements_match_runtime': True,
          'empty_months_preserve_annual_profiles': True, 'empty_locations_recovery': True,
          'app_sha256': hashlib.sha256((root / 'dashboard/app.py').read_bytes()).hexdigest(),
          'bundle_manifest_sha256': hashlib.sha256(manifest_body).hexdigest()}
if os.environ.get('REPORT_DIR'):
    (Path(os.environ['REPORT_DIR']) / 'prod_smoke.json').write_text(json.dumps(report, indent=2) + '\n')
print('Standalone bundle passed all pages and filter scope checks without network or pipeline dependencies.')
