"""Acquire ERA5-Land climate inputs for the same twelve Morocco sample locations."""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline import MANIFEST, ROOT, SITES, read_era5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-year', type=int, default=2015)
    parser.add_argument('--end-year', type=int, default=2024)
    args = parser.parse_args()
    if not 1950 <= args.start_year <= args.end_year <= 2025:
        parser.error('Select complete years between 1950 and 2025')
    records, hours = read_era5(range(args.start_year, args.end_year + 1))
    manifest = {'source': 'ERA5-Land, delivered by Open-Meteo', 'model': 'era5_land',
                'variables': ['temperature_2m', 'soil_moisture_0_to_7cm'],
                'years': [args.start_year, args.end_year], 'locations': len(SITES),
                'hourly_records': hours, 'site_months': len(records),
                'total_bytes': sum(r['bytes'] for r in MANIFEST.values()),
                'files': MANIFEST, 'verified_at_utc': datetime.now(timezone.utc).isoformat()}
    target = ROOT / 'data/metadata/era5_downloads.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(f'ERA5-Land complete: {hours:,} hourly records; {manifest["total_bytes"]:,} source bytes', flush=True)


if __name__ == '__main__':
    main()
