"""Verify every completed climate archive and the actual size gate after acquisition."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_climate import ROOT, digest, inspect


def main():
    path = ROOT / 'data/metadata/climate_downloads.json'
    manifest = json.loads(path.read_text(encoding='utf-8'))
    start, end = manifest['years']
    records = manifest['files']
    if sorted(r['year'] for r in records) != list(range(start, end + 1)):
        raise SystemExit('The requested year range has not finished downloading')
    total = 0
    for record in records:
        source = ROOT / record['path']
        if source.stat().st_size != record['bytes']:
            raise ValueError(f'Size mismatch: {source.name}')
        if digest(source) != record['sha256']:
            raise ValueError(f'Checksum mismatch: {source.name}')
        validation = inspect(source, record['year'])
        if 'dimensions' not in validation:
            raise ValueError('netCDF4 is required for final verification')
        if validation['units'] != 'mm/month':
            raise ValueError(f'Unexpected rainfall units: {source.name}')
        record.update(validation)
        total += source.stat().st_size
        print(f'PASS {source.name}: {source.stat().st_size:,} bytes, 12 months, precipitation, SHA-256', flush=True)
    if total < manifest['minimum_bytes']:
        raise SystemExit('Completed climate files have not reached the minimum size')
    manifest.update(total_climate_bytes=total, minimum_size_met=True,
                    final_verification_utc=datetime.now(timezone.utc).isoformat())
    path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    summary = [
        '# Climate acquisition verification', '',
        f'- Completed archives: **{len(records)}** ({start}–{end})',
        f'- Actual climate source size: **{total:,} bytes** ({total / 1e9:.3f} GB / {total / 1024**3:.3f} GiB)',
        f'- Required minimum: **{manifest["minimum_bytes"]:,} bytes**',
        '- Result: **PASS — completed climate data exceeds 1 GiB**',
        '- Every archive opened as NetCDF; all 12 months, precipitation units and SHA-256 checksums verified.',
        '- Coverage: global raw CHIRPS source rasters. Morocco-only subsets will be smaller.',
        '- Dashboard target: Power BI Desktop; no custom web dashboard.', '',
        '| Year | Bytes on disk | SHA-256 |', '| --- | ---: | --- |',
    ]
    summary += [f'| {r["year"]} | {r["bytes"]:,} | `{r["sha256"]}` |' for r in records]
    (path.parent / 'verification.md').write_text('\n'.join(summary) + '\n', encoding='utf-8')
    print(f'PASS: {total:,} bytes across {len(records)} completed archives', flush=True)


if __name__ == '__main__':
    main()
