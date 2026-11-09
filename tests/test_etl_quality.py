"""Exercise provider decoding, the Landing commit and Spark stage boundaries."""
import hashlib
import io
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import numpy as np
import pyarrow.parquet as pq
import pytest
import rasterio
from botocore.exceptions import ClientError
from rasterio.transform import from_origin

from observatory import pipeline, spark_medallion, storage
from observatory.contracts import TABLES


@pytest.fixture
def provider_workspace(tmp_path, monkeypatch):
    raw = tmp_path / 'raw'
    (raw / 'chirps').mkdir(parents=True)
    processed = tmp_path / 'processed'
    processed.mkdir()
    monkeypatch.setattr(pipeline, 'RAW', raw)
    monkeypatch.setattr(pipeline, 'PROCESSED', processed)
    monkeypatch.setattr(pipeline, 'SITES', [{'id': 'sample', 'name': 'Sample', 'lat': 0.5, 'lon': 0.5}])
    return raw, processed


@pytest.mark.parametrize('damage', [None, 'missing_value', 'substitute_timestamp', 'out_of_range'])
def test_era5_requires_aligned_complete_hourly_series(provider_workspace, tmp_path, monkeypatch, damage):
    start = datetime(2024, 1, 1)
    times = [(start + timedelta(hours=n)).isoformat(timespec='minutes') for n in range(366 * 24)]
    hourly = {'time': times, 'temperature_2m': [18.0] * len(times), 'soil_moisture_0_to_7cm': [0.3] * len(times)}
    if damage == 'missing_value':
        hourly['temperature_2m'].pop()
    elif damage == 'substitute_timestamp':
        times[3] = '2024-01-01T03:30'
    elif damage == 'out_of_range':
        hourly['soil_moisture_0_to_7cm'][3] = 1.2
    payload = tmp_path / 'era5.json'
    payload.write_text(json.dumps([{'latitude': .5, 'longitude': .5, 'hourly': hourly}]))
    monkeypatch.setattr(pipeline, 'download', lambda *_: payload)

    if damage:
        with pytest.raises(ValueError, match='misaligned|physical bounds'):
            pipeline.read_era5([2024])
        assert not list(provider_workspace[1].rglob('*.parquet'))
    else:
        months, count = pipeline.read_era5([2024])
        assert len(months['sample', 2024, 2]['temperature']) == 29 * 24
        assert count == 8784
        assert pq.ParquetFile(next(provider_workspace[1].rglob('*.parquet'))).metadata.num_rows == count


@pytest.mark.parametrize('rainfall', [5.0, float('inf')])
def test_chirps_clips_footprint_and_rejects_empty_valid_grid(provider_workspace, tmp_path, monkeypatch, rainfall):
    raw, _ = provider_workspace
    (raw / 'chirps/chirps-v3.0.2024.monthly.nc').touch()
    raster = tmp_path / 'climate.tif'
    open_raster = rasterio.open
    with open_raster(raster, 'w', driver='GTiff', width=5, height=5, count=12,
                     dtype='float32', transform=from_origin(-2, 3, 1, 1)) as writer:
        writer.write(np.full((12, 5, 5), rainfall, dtype=np.float32))
    monkeypatch.setattr(pipeline.rasterio, 'open', lambda _: open_raster(raster))
    geometry = {'type': 'MultiPolygon', 'coordinates': [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]}
    if np.isfinite(rainfall):
        sampled, cells = pipeline.read_rain([2024], geometry)
        assert cells == len(sampled) == 12
        assert {row['rainfall_mm'] for row in sampled} == {5.0}
    else:
        with pytest.raises(ValueError, match='No valid CHIRPS footprint cells'):
            pipeline.read_rain([2024], geometry)


def test_osm_decoder_maps_nearby_services_and_road(provider_workspace, tmp_path, monkeypatch):
    extract = tmp_path / 'sample.osm'
    extract.write_text('''<osm version="0.6" generator="test">
      <node id="1" lat="0.5" lon="0.5"><tag k="amenity" v="school"/></node>
      <node id="2" lat="0.5" lon="0.51"/>
      <node id="3" lat="5" lon="5"><tag k="amenity" v="hospital"/></node>
      <way id="4"><nd ref="1"/><nd ref="2"/><tag k="highway" v="residential"/></way>
    </osm>''')
    monkeypatch.setattr(pipeline, 'download', lambda *_: extract)
    counts, features, stats = pipeline.osm_context()
    assert counts['sample'] == {'education': 1, 'healthcare': 0, 'water': 0, 'road_km': 1.1}
    assert [feature['properties']['osm_id'] for feature in features] == ['node/1']
    assert stats == {'nodes': 3, 'ways': 1}


def fingerprint(body):
    return {'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}


class LandingStore:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        body = self.objects[Key]
        return {'ContentLength': len(body), 'Metadata': {'sha256': fingerprint(body)['sha256']}}

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.objects[key] = Path(path).read_bytes()

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body

    def get_object(self, *, Bucket, Key):
        return {'Body': io.BytesIO(self.objects[Key])}


def test_normalized_landing_retry_preserves_previous_manifest(tmp_path, monkeypatch):
    store = LandingStore()
    monkeypatch.setattr(storage, 'DATA', tmp_path)
    monkeypatch.setattr(storage, 'client', lambda: store)
    monkeypatch.setattr(storage, 'ensure_buckets', lambda _: None)
    metadata = tmp_path / 'metadata'
    metadata.mkdir()
    for name in ('climate_downloads', 'era5_downloads', 'osm_downloads', 'source_snapshot'):
        (metadata / f'{name}.json').write_text('{}')

    inputs = {}
    for table in TABLES:
        name = f'bronze/{table}/part.parquet'
        path = tmp_path / name
        path.parent.mkdir(parents=True)
        path.write_bytes(table.encode())
        inputs[table] = [{'path': name, **fingerprint(path.read_bytes())}]
    manifest = metadata / 'bronze_inputs.json'
    manifest.write_text(json.dumps({'tables': inputs}))

    storage.sync()
    committed = store.objects[storage.STAGING_MANIFEST]
    old_paths = storage.staged_inputs(store)
    assert set(old_paths) == set(TABLES)
    assert all(len(paths) == 1 for paths in old_paths.values())

    # Upload a legitimate change first, then encounter a corrupted file.
    first, last = next(iter(TABLES)), next(reversed(TABLES))
    first_path = tmp_path / inputs[first][0]['path']
    first_path.write_bytes(b'new version')
    inputs[first][0].update(fingerprint(b'new version'))
    manifest.write_text(json.dumps({'tables': inputs}))
    (tmp_path / inputs[last][0]['path']).write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='does not match its manifest'):
        storage.sync()
    assert store.objects[storage.STAGING_MANIFEST] == committed
    assert storage.staged_inputs(store) == old_paths
    assert store.objects[old_paths[first][0].removeprefix('s3a://landing/')] == first.encode()


@pytest.mark.parametrize('damage', ['missing', 'duplicate', 'traversal', 'glob'])
def test_bronze_manifest_rejects_invalid_inventory(damage):
    store = LandingStore()
    files = [{'key': f'{storage.NORMALIZED_PREFIX}{table}/part.parquet', **fingerprint(b'example')}
             for table in TABLES]
    if damage == 'missing':
        files.pop()
    elif damage == 'duplicate':
        files.append(files[0])
    else:
        files[0]['key'] = files[0]['key'].replace('part.parquet', '../part.parquet' if damage == 'traversal' else '*.parquet')
    store.objects[storage.STAGING_MANIFEST] = json.dumps({'files': files}).encode()
    with pytest.raises(ValueError, match='missing|duplicate|Invalid normalized'):
        storage.staged_inputs(store)


@pytest.mark.parametrize('stage', ['bronze', 'silver'])
def test_spark_blocks_empty_inputs_and_closes_session(monkeypatch, stage):
    # The local suite exercises control flow without provisioning a Spark cluster.
    monkeypatch.setitem(sys.modules, 'pyspark.sql', SimpleNamespace(functions=Mock()))
    spark = MagicMock()
    monkeypatch.setattr(spark_medallion, 'session', lambda: spark)
    monkeypatch.setattr(storage, 'client', Mock())
    table = next(iter(TABLES))
    key = f's3a://landing/observatory/normalized/{table}/committed.parquet'
    monkeypatch.setattr(storage, 'staged_inputs', lambda _: {table: [key]})
    spark.read.option.return_value.parquet.return_value.take.return_value = []
    spark.table.return_value.take.return_value = []
    with pytest.raises(ValueError, match=f'empty {stage.title()} input'):
        spark_medallion.run(stage)
    if stage == 'bronze':
        spark.read.option.return_value.parquet.assert_called_once_with(key)
    spark.stop.assert_called_once()


@pytest.mark.parametrize('swap_months', [False, True])
def test_chirps_archive_verification_requires_ordered_months(tmp_path, swap_months):
    from netCDF4 import Dataset, date2num
    from observatory.download_climate import inspect

    archive = tmp_path / 'climate.nc'
    dates = [datetime(2024, month, 1) for month in range(1, 13)]
    if swap_months:
        dates[1], dates[2] = dates[2], dates[1]
    with Dataset(archive, 'w') as ds:
        ds.createDimension('time', 12)
        ds.createDimension('lat', 1)
        ds.createDimension('lon', 1)
        time = ds.createVariable('time', 'f8', ('time',))
        time.units = 'days since 2024-01-01'
        time[:] = date2num(dates, time.units)
        precip = ds.createVariable('precip', 'f4', ('time', 'lat', 'lon'))
        precip.units = 'mm/month'
        precip[:] = 5
    if swap_months:
        with pytest.raises(ValueError, match='Unexpected dates'):
            inspect(archive, 2024)
    else:
        assert inspect(archive, 2024)['units'] == 'mm/month'


def test_airflow_requires_each_stage_to_succeed_before_publication():
    from runpy import run_path

    pytest.importorskip('airflow.models.dag')
    path = Path(__file__).resolve().parents[1] / 'airflow/dags/morocco_observatory.py'
    dag = run_path(str(path))['dag']
    order = ['acquire_and_verify', 'upload_verified_raw_landing', 'decode_provider_formats',
             'upload_verified_minio_landing', 'spark_bronze', 'spark_silver_quality_gate', 'dbt_build_gold',
             'dbt_document_lineage', 'publish_tested_bi_snapshot']
    assert set(dag.task_ids) == set(order)
    for index, task_id in enumerate(order):
        task = dag.get_task(task_id)
        assert task.upstream_task_ids == ({order[index - 1]} if index else set())
        assert task.trigger_rule == 'all_success'
    assert dag.max_active_runs == 1
