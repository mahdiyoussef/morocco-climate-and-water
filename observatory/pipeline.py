"""Combine CHIRPS, ERA5-Land and OSM into regional Parquet and native BI input tables.

Gold transformation uses dbt through observatory/warehouse.py; this CLI delegates to it.
Downloads are cached; errors never fall back to generated observations.
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import math
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import netCDF4
import osmium
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import requests
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data/raw'
PROCESSED = ROOT / 'data/processed'
PUBLIC = ROOT / 'data/gold'
SITES = json.loads((ROOT / 'observatory/sites.json').read_text(encoding='utf-8'))
CHIRPS = 'https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/africa/tifs/'
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'AtlasPulse-Morocco-Research/1.0', 'Accept-Encoding': 'identity'})
MANIFEST = {}


def download(url: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        print(f'Downloading {path.name}', flush=True)
        for attempt in range(3):
            try:
                with SESSION.get(url, stream=True, timeout=(30, 180)) as response:
                    response.raise_for_status()
                    with path.with_suffix(path.suffix + '.partial').open('wb') as output:
                        for chunk in response.iter_content(1024 * 1024):
                            output.write(chunk)
                    length = response.headers.get('Content-Length')
                    if length and path.with_suffix(path.suffix + '.partial').stat().st_size != int(length):
                        raise ValueError(f'Incomplete download: {url}')
                    path.with_suffix(path.suffix + '.partial').replace(path)
                break
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(2 ** (attempt + 1))
        time.sleep(1)  # CHC asks clients to stay below about one request/second.
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    MANIFEST[str(path.relative_to(ROOT))] = {
        'url': url, 'bytes': path.stat().st_size, 'sha256': digest.hexdigest(),
    }
    return path


def footprint():
    path = download('https://download.geofabrik.de/africa/morocco.poly', RAW / 'osm/morocco.poly')
    rings, current = [], []
    for line in path.read_text().splitlines()[1:]:
        parts = line.split()
        if len(parts) == 2:
            current.append([float(parts[0]), float(parts[1])])
        elif line.strip() == 'END' and current:
            rings.append([current])
            current = []
    if not rings:
        raise ValueError('No polygon in Geofabrik footprint')
    return {'type': 'MultiPolygon', 'coordinates': rings}


def read_rain(years, geometry):
    records, grid_count = [], 0
    coords = np.array([p for poly in geometry['coordinates'] for ring in poly for p in ring])
    left, bottom = coords.min(axis=0)
    right, top = coords.max(axis=0)
    for year in years:
        for month in range(1, 13):
            filename = f'chirps-v3.0.{year}.monthly.nc'
            path = RAW / 'chirps' / filename
            if not path.exists():
                raise FileNotFoundError(f'Run observatory/download_climate.py first: missing {filename}')
            # GDAL reads only the selected spatial window from the annual NetCDF.
            with rasterio.open(f'NETCDF:{path}:precip') as src:
                win = from_bounds(left, bottom, right, top, src.transform).round_offsets().round_lengths()
                values = src.read(month, window=win, masked=True)
                transform = src.window_transform(win)
                valid = geometry_mask([geometry], values.shape, transform, invert=True) & ~np.ma.getmaskarray(values) & (values.data >= 0)
                rr, cc = np.where(valid)
                xx, yy = rasterio.transform.xy(transform, rr, cc)
                grid_count += len(rr)
                table = pa.table({'year': [year] * len(rr), 'month': [month] * len(rr),
                                  'lon': np.asarray(xx, dtype=np.float32), 'lat': np.asarray(yy, dtype=np.float32),
                                  'rainfall_mm': values.data[rr, cc].astype(np.float32)})
                dest = PROCESSED / f'chirps_grid/year={year}/month={month:02d}/part.parquet'
                dest.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(table, dest, compression='zstd')
                for site in SITES:
                    row, col = src.index(site['lon'], site['lat'])
                    patch = src.read(month, window=rasterio.windows.Window(col - 2, row - 2, 5, 5), masked=True)
                    candidates = [(float((r - 2) ** 2 + (c - 2) ** 2), r, c)
                                  for r in range(patch.shape[0]) for c in range(patch.shape[1])
                                  if not np.ma.getmaskarray(patch)[r, c] and patch.data[r, c] >= 0]
                    if not candidates:
                        raise ValueError(f'No valid CHIRPS pixel near {site["name"]}')
                    _, r, c = min(candidates)
                    lon, lat = src.xy(row - 2 + r, col - 2 + c)
                    records.append({'site': site['id'], 'year': year, 'month': month,
                                    'rainfall_mm': round(float(patch.data[r, c]), 3),
                                    'chirps_lat': lat, 'chirps_lon': lon})
            print(f'CHIRPS {year}-{month:02d}: {len(rr):,} footprint cells', flush=True)
    pq.write_table(pa.Table.from_pylist(records), PROCESSED / 'chirps_sites.parquet', compression='zstd')
    return records, grid_count


def read_era5(years):
    monthly, count = {}, 0
    for year in years:
        params = {'latitude': ','.join(str(s['lat']) for s in SITES),
                  'longitude': ','.join(str(s['lon']) for s in SITES),
                  'start_date': f'{year}-01-01', 'end_date': f'{year}-12-31',
                  'hourly': 'temperature_2m,soil_moisture_0_to_7cm',
                  'models': 'era5_land', 'timezone': 'UTC'}
        url = 'https://archive-api.open-meteo.com/v1/archive?' + requests.compat.urlencode(params)
        path = download(url, RAW / f'era5/era5_land_{year}.json')
        data = json.loads(path.read_text())
        if not isinstance(data, list) or len(data) != len(SITES):
            raise ValueError('Unexpected Open-Meteo response or missing locations')
        for site, payload in zip(SITES, data):
            hours = payload['hourly']
            expected = (366 if calendar.isleap(year) else 365) * 24
            if (len(hours['time']) != expected or len(set(hours['time'])) != expected
                    or hours['time'][0] != f'{year}-01-01T00:00'
                    or hours['time'][-1] != f'{year}-12-31T23:00'):
                raise ValueError(f'Incomplete ERA5 year for {site["id"]}')
            rows = []
            for t, temp, soil in zip(hours['time'], hours['temperature_2m'], hours['soil_moisture_0_to_7cm']):
                if temp is None or soil is None:
                    raise ValueError(f'Missing ERA5 values for {site["id"]} {t}')
                if not -60 <= temp <= 65 or not 0 <= soil <= 1:
                    raise ValueError('ERA5 value outside physical bounds')
                key = (site['id'], year, int(t[5:7]))
                bucket = monthly.setdefault(key, {'temperature': [], 'soil': [],
                                                  'era5_lat': payload['latitude'], 'era5_lon': payload['longitude']})
                bucket['temperature'].append(temp)
                bucket['soil'].append(soil)
                rows.append({'site': site['id'], 'time': t, 'temperature_c': temp, 'soil_moisture': soil})
            dest = PROCESSED / f'era5_hourly/year={year}/site={site["id"]}/part.parquet'
            dest.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), dest, compression='zstd')
            count += len(rows)
        print(f'ERA5-Land {year}: {len(SITES) * expected:,} hourly records', flush=True)
    return monthly, count


def km(lon1, lat1, lon2, lat2):
    dx = math.radians(lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    dy = math.radians(lat2 - lat1)
    return 6371 * math.hypot(dx, dy)


def osm_context():
    path = download('https://download.geofabrik.de/africa/morocco-latest.osm.pbf', RAW / 'osm/morocco.osm.pbf')
    counts = {s['id']: {'healthcare': 0, 'education': 0, 'water': 0, 'road_km': 0.0} for s in SITES}
    features = []

    class Handler(osmium.SimpleHandler):
        nodes = 0
        ways = 0

        def category(self, tags):
            a = tags.get('amenity', '')
            if a in {'hospital', 'clinic', 'doctors', 'pharmacy'} or tags.get('healthcare'):
                return 'healthcare'
            if a in {'school', 'college', 'university', 'kindergarten'}:
                return 'education'
            if a in {'drinking_water', 'water_point'} or tags.get('water') == 'reservoir' or tags.get('man_made') in {'water_tower', 'water_works'}:
                return 'water'
            return None

        def add(self, lon, lat, tags, oid, kind):
            category = self.category(tags)
            if not category:
                return
            near = [s for s in SITES if km(lon, lat, s['lon'], s['lat']) <= 20]
            if not near:
                return
            for site in near:
                counts[site['id']][category] += 1
            features.append({'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [round(lon, 6), round(lat, 6)]},
                             'properties': {'category': category, 'name': tags.get('name', category.replace('_', ' ')),
                                            'osm_id': f'{kind}/{oid}', 'sites': [s['id'] for s in near]}})

        def node(self, node):
            self.nodes += 1
            if not len(node.tags):
                return
            if node.location.valid():
                self.add(node.location.lon, node.location.lat, node.tags, node.id, 'node')

        def way(self, way):
            self.ways += 1
            category = self.category(way.tags)
            road = way.tags.get('highway') in {'motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'unclassified'}
            if not category and not road:
                return
            try:
                points = [(n.lon, n.lat) for n in way.nodes]
            except osmium.InvalidLocationError:
                return
            if not points:
                return
            lon, lat = np.mean(points, axis=0)
            if category:
                self.add(float(lon), float(lat), way.tags, way.id, 'way')
            if road:
                min_lon = min(p[0] for p in points)
                max_lon = max(p[0] for p in points)
                min_lat = min(p[1] for p in points)
                max_lat = max(p[1] for p in points)
                nearby = [s for s in SITES if min_lat - .21 <= s['lat'] <= max_lat + .21 and min_lon - .26 <= s['lon'] <= max_lon + .26]
                for first, second in zip(points, points[1:]):
                    mid_lon, mid_lat = (first[0] + second[0]) / 2, (first[1] + second[1]) / 2
                    for site in nearby:
                        if abs(mid_lat - site['lat']) < .21 and abs(mid_lon - site['lon']) < .26 and km(mid_lon, mid_lat, site['lon'], site['lat']) <= 20:
                            counts[site['id']]['road_km'] += km(*first, *second)

    handler = Handler()
    handler.apply_file(str(path), locations=True, idx='flex_mem')
    (PROCESSED / 'osm_stats.json').write_text(json.dumps({'nodes': handler.nodes, 'ways': handler.ways}), encoding='utf-8')
    for site in counts.values():
        site['road_km'] = round(site['road_km'], 1)
    (PROCESSED / 'osm_services.geojson').write_text(json.dumps({'type': 'FeatureCollection', 'features': features}, ensure_ascii=False), encoding='utf-8')
    pq.write_table(pa.Table.from_pylist([{'site': s['id'], 'road_km': counts[s['id']]['road_km']} for s in SITES]), PROCESSED / 'osm_roads.parquet')
    print(f'OSM: {handler.nodes:,} nodes, {handler.ways:,} ways, {len(features):,} local service objects', flush=True)
    return counts, features, {'nodes': handler.nodes, 'ways': handler.ways}



def main():
    import sys
    sys.path.insert(0, str(ROOT))
    from observatory.warehouse import prepare_sources, local_silver, run_dbt, export_local
    parser = argparse.ArgumentParser(description='Compatibility entry point: all Gold transformations use dbt.')
    parser.add_argument('--start-year', type=int, default=2015)
    parser.add_argument('--end-year', type=int, default=2024)
    args = parser.parse_args()
    acquisition = json.loads((ROOT / 'data/metadata/climate_downloads.json').read_text(encoding='utf-8'))
    if [args.start_year, args.end_year] != acquisition['years']:
        parser.error('The requested years must match the verified acquisition manifest.')
    prepare_sources()
    local_silver()
    run_dbt()
    export_local()


if __name__ == '__main__':
    main()
