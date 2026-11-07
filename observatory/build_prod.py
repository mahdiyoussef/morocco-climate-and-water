"""Build a standalone, credential-free Streamlit package from whitelisted Gold assets."""
import csv
import hashlib
import json
import re
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROD = ROOT / 'prod'
TABLES = ['dim_location', 'dim_month', 'fact_climate_monthly', 'fact_infrastructure', 'source_audit',
          'mart_monthly_anomalies', 'mart_water_year', 'mart_site_trends', 'mart_spatial_rainfall']
ENTRYPOINT = '''"""Standalone Gold snapshot app. Run: streamlit run streamlit_app.py"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from dashboard.app import main

# Explicit arguments keep this package independent of inherited database settings.
main(data_root=ROOT / "data", backend="local")
'''
README = '''# Deploy AtlasPulse on Streamlit Community Cloud

This self-contained folder includes the dashboard, nine published Gold CSV tables,
map geometry, source attribution, calculation documentation and quality evidence.
It reads a fixed, published 2015–2024 snapshot. It needs no database credentials,
PostgreSQL, MinIO, Spark, Airflow, raw archives or paid external service.

## Recommended: deploy this folder as its own repository

1. Create a GitHub repository and put the **contents of this folder** at its root.
   Include the hidden `.streamlit/config.toml` file, the `data/` folder and the
   `dashboard/` package. `streamlit_app.py` should be at repository root.
2. Sign in to [Streamlit Community Cloud](https://share.streamlit.io/), choose
   **Create app**, then select your repository and branch.
3. Set **Main file path** to `streamlit_app.py`.
4. In **Advanced settings**, select **Python 3.12**. Leave Secrets empty.
5. Choose your app URL and click **Deploy**.

Community Cloud offers free hosting and runs the application from GitHub. Its
runtime limits and availability policies can change; this bundle is approximately
3–4 MB, with only a small published dataset loaded into memory.

If deploying from the original full repository instead, select
`prod/streamlit_app.py` and keep `prod/requirements.txt` beside it. Community Cloud
runs from repository root: copy this folder's `.streamlit/config.toml` to the
repository-root `.streamlit/config.toml` in your deployment branch as well. The
entrypoint resolves its data relative to its own file, independently of cwd.

## Run locally

```bash
python -m pip install -r requirements.txt
python -m streamlit run streamlit_app.py
```

Open http://localhost:8501. The optional online CARTO basemap can be turned off;
all observations, local geometry, charts and exports remain available.

## Update the published dataset

This app serves the packaged snapshot, not a live database. In the source project,
finish a successful pipeline publication, then run:

```bash
python observatory/analyze_indicators.py
python observatory/build_prod.py
```

Commit the refreshed app/data files to your deployment repository. Community Cloud
redeploys code and data changes from GitHub. `bundle_manifest.json` records file
sizes, SHA-256 checksums, table row counts and the source publication ID.
The ZIP created by the builder contains this folder's contents at archive root.

See [INDICATORS.md](INDICATORS.md) for 24 supported indicators, formulas, measured
findings and interpretation limits. The two analysis CSV files are in
`data/analysis/`. Source attribution is preserved in the dashboard and Gold assets.

## Official deployment references

- [Free Community Cloud hosting](https://docs.streamlit.io/deploy/streamlit-community-cloud)
- [Repository and entrypoint setup](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy)
- [File organization](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/file-organization)
- [App dependencies](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/app-dependencies)
'''


def main():
    outputs = []
    def write(relative, text):
        target = PROD / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding='utf-8')
        outputs.append(target)
    def copy(source, relative):
        target = PROD / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / source, target)
        outputs.append(target)
    for name in ['__init__.py', 'app.py', 'charts.py', 'data.py', 'indicators.py']:
        copy('dashboard/' + name, 'dashboard/' + name)
    copy('dashboard/requirements-cloud.txt', 'requirements.txt')
    write('streamlit_app.py', ENTRYPOINT)
    config = (ROOT / '.streamlit/config.toml').read_text()
    config = re.sub(r'(?m)^address\s*=.*$', 'address = "0.0.0.0"', config)
    write('.streamlit/config.toml', config)
    copy('dashboard/.gitignore.prod', '.gitignore')
    for name in [*[table + '.csv' for table in TABLES], 'snapshot.json', 'provenance.json', 'osm.geojson', 'footprint.geojson']:
        copy('data/gold/' + name, 'data/gold/' + name)
    for name in ['climate_downloads.json', 'era5_downloads.json', 'processing_verification.json', 'dbt_run_results.json']:
        copy('data/metadata/' + name, 'data/metadata/' + name)
    copy('docs/indicators.md', 'INDICATORS.md')
    for name in ['indicator_catalog.csv', 'indicator_profiles_2015_2024.csv']:
        copy('docs/' + name, 'data/analysis/' + name)
    readme = README
    gallery = ROOT / 'docs/screenshots/gallery.md'
    if gallery.exists():
        screenshot_manifest = json.loads((ROOT / 'docs/screenshots/manifest.json').read_text())
        expected_images = {'overview.png', 'indicators.png', 'climate-explorer.png', 'compare-locations.png',
                           'water-climate-insights.png', 'infrastructure.png', 'data-methodology.png'}
        entries = {entry['file']: entry for entry in screenshot_manifest['captures']}
        if set(entries) != {'docs/screenshots/' + name for name in expected_images}:
            raise ValueError('Screenshot gallery must contain exactly the seven known views')
        for name in sorted(expected_images):
            relative = 'docs/screenshots/' + name
            if hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() != entries[relative]['sha256']:
                raise ValueError('Screenshot checksum mismatch: ' + name)
            copy(relative, relative)
        copy('docs/screenshots/manifest.json', 'docs/screenshots/manifest.json')
        readme += '\n\n' + gallery.read_text() + '\n'
    write('README.md', readme)
    counts = {}
    for table in TABLES:
        with (PROD / 'data/gold' / (table + '.csv')).open(newline='') as stream:
            counts[table] = sum(1 for _ in csv.DictReader(stream))
    provenance = json.loads((PROD / 'data/gold/provenance.json').read_text())
    if counts != provenance['tables']:
        raise ValueError('Packaged Gold row counts do not match the publication receipt')
    snapshot = json.loads((PROD / 'data/gold/snapshot.json').read_text())
    manifest = {'built_at': datetime.now(timezone.utc).isoformat(), 'backend': 'local Gold snapshot',
                'publication_id': snapshot['meta'].get('publication_id'), 'python_version': '3.12',
                'gold_row_counts': counts, 'files': {}}
    for path in sorted(outputs):
        body = path.read_bytes()
        manifest['files'][path.relative_to(PROD).as_posix()] = {'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
    write('bundle_manifest.json', json.dumps(manifest, indent=2) + '\n')
    archive = ROOT / 'prod-streamlit.zip'
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as output:
        for path in sorted(outputs):
            output.write(path, path.relative_to(PROD).as_posix())
    print(f'Built {PROD}: {len(outputs)} files, {sum(p.stat().st_size for p in outputs):,} bytes')
    print(f'Archive: {archive} ({archive.stat().st_size:,} bytes)')
    print('Gold publication:', manifest['publication_id'])


if __name__ == '__main__':
    main()
