"""Download validated CHIRPS climate archives and record actual bytes on disk.

Default scope: ten complete annual NetCDF files, 2015–2024, from UCSB CHC.
Files are global source rasters; regional clipping is a later processing step.
Only completed climate inputs count toward the minimum dataset size.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = 'https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/global/netcdf/by_year/'
SIGNATURE = b'\x89HDF\r\n\x1a\n'


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def download(url, path):
    part = path.with_suffix(path.suffix + '.partial')
    headers = {'User-Agent': 'AtlasPulse-Morocco-Research/1.0', 'Accept-Encoding': 'identity'}
    last_error = None
    for attempt in range(3):
        try:
            offset = part.stat().st_size if part.exists() else 0
            request_headers = dict(headers)
            if offset:
                request_headers['Range'] = f'bytes={offset}-'
            with urllib.request.urlopen(urllib.request.Request(url, headers=request_headers), timeout=120) as response:
                resumed = response.status == 206
                if offset and not resumed:
                    offset = 0
                content_range = response.headers.get('Content-Range', '')
                expected = int(content_range.split('/')[-1]) if resumed else int(response.headers['Content-Length'])
                if resumed and not content_range.startswith(f'bytes {offset}-'):
                    raise ValueError('Server returned an unexpected byte range')
                if shutil.disk_usage(path.parent).free < expected - offset + 256 * 1024 * 1024:
                    raise OSError('Not enough free disk space for download and a 256 MiB reserve')
                received = offset
                last_log = time.monotonic()
                with part.open('ab' if resumed else 'wb') as output:
                    while block := response.read(1024 * 1024):
                        output.write(block)
                        received += len(block)
                        if time.monotonic() - last_log > 15:
                            print(f'  {path.name}: {received / 1024**2:.1f}/{expected / 1024**2:.1f} MiB', flush=True)
                            last_log = time.monotonic()
                if received != expected:
                    raise OSError(f'Incomplete response: {received} of {expected} bytes')
                with part.open('rb') as stream:
                    signature = stream.read(8)
                if signature != SIGNATURE and not signature.startswith(b'CDF'):
                    raise ValueError('Downloaded response is not NetCDF/HDF5 data')
                part.replace(path)
                return expected, response.headers.get('Last-Modified')
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
            if attempt == 2:
                raise
            print(f'  Retry {attempt + 1}: {type(exc).__name__}', flush=True)
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(str(last_error))


def inspect(path, year):
    try:
        import netCDF4
    except ImportError:
        return {'validation': 'HDF5 signature, HTTP length and SHA-256; install netCDF4 for variable checks'}
    with netCDF4.Dataset(path) as ds:
        precip = ds.variables.get('precip')
        if precip is None:
            raise ValueError(f'No precipitation variable in {path.name}')
        if precip.shape[0] != 12:
            raise ValueError(f'Expected 12 monthly time steps in {path.name}')
        times = ds.variables['time']
        dates = netCDF4.num2date(times[:], times.units)
        if [(d.year, d.month) for d in dates] != [(year, month) for month in range(1, 13)]:
            raise ValueError(f'Unexpected dates in {path.name}')
        return {'validation': 'NetCDF opened; precipitation variable and all 12 months verified',
                'dimensions': dict(zip(precip.dimensions, precip.shape)), 'units': getattr(precip, 'units', None)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start-year', type=int, default=2015)
    p.add_argument('--end-year', type=int, default=2024)
    p.add_argument('--minimum-bytes', type=int, default=1024**3 + 1)
    args = p.parse_args()
    if not 1981 <= args.start_year <= args.end_year <= 2025:
        p.error('Choose complete years between 1981 and 2025')
    directory = ROOT / 'data/raw/chirps'
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = ROOT / 'data/metadata/climate_downloads.json'
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {'project': 'Morocco Climate & Water Observatory', 'dashboard': 'Power BI',
                'source': 'UCSB Climate Hazards Center, CHIRPS v3',
                'source_url': BASE, 'coverage': 'Global source archives; Morocco clipping is not yet applied.',
                'years': [args.start_year, args.end_year], 'minimum_bytes': args.minimum_bytes,
                'files': [], 'total_climate_bytes': 0, 'minimum_size_met': False}
    for year in range(args.start_year, args.end_year + 1):
        name = f'chirps-v3.0.{year}.monthly.nc'
        path = directory / name
        url = BASE + name
        action = 'verifying cached annual archive' if path.exists() else 'downloading official annual archive'
        print(f'CHIRPS {year}: {action}', flush=True)
        modified = None
        expected = None
        if not path.exists():
            expected, modified = download(url, path)
        with path.open('rb') as stream:
            signature = stream.read(8)
        if signature != SIGNATURE and not signature.startswith(b'CDF'):
            raise ValueError(f'Invalid source archive {name}')
        record = {'path': str(path.relative_to(ROOT)).replace('\\', '/'), 'url': url, 'year': year,
                  'bytes': path.stat().st_size, 'http_content_length': expected, 'last_modified': modified,
                  'sha256': digest(path), 'verified_at_utc': datetime.now(timezone.utc).isoformat(),
                  **inspect(path, year)}
        manifest['files'].append(record)
        manifest['total_climate_bytes'] = sum(f['bytes'] for f in manifest['files'])
        manifest['minimum_size_met'] = manifest['total_climate_bytes'] >= args.minimum_bytes
        manifest['updated_at_utc'] = datetime.now(timezone.utc).isoformat()
        temp = manifest_path.with_suffix('.json.partial')
        temp.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        temp.replace(manifest_path)
        print(f'  Verified {record["bytes"]:,} bytes. Total climate data: {manifest["total_climate_bytes"]:,} bytes', flush=True)
        time.sleep(1)
    if not manifest['minimum_size_met']:
        raise SystemExit(f'Climate dataset is below the required minimum of {args.minimum_bytes:,} bytes')
    print(f'COMPLETE: {len(manifest["files"])} climate archives; {manifest["total_climate_bytes"]:,} bytes; minimum satisfied.', flush=True)


if __name__ == '__main__':
    main()
