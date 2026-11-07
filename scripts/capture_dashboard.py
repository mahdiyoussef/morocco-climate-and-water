#!/usr/bin/env python3
"""Capture seven dashboard views and refresh the generated README gallery.

Install: python -m pip install -r requirements-screenshots.txt -r prod/requirements.txt
Browser: python -m playwright install --only-shell chromium
Run:     python scripts/capture_dashboard.py
Existing Docker app: python scripts/capture_dashboard.py --url http://localhost:8501
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
START = '<!-- dashboard-screenshots:start -->'
END = '<!-- dashboard-screenshots:end -->'
VIEWS = [
    ('Overview', 'overview', 'Morocco, in perspective.', False),
    ('Indicators', 'indicators', 'Read the signals in the data.', True),
    ('Climate explorer', 'climate-explorer', 'The shape of a changing climate.', True),
    ('Compare locations', 'compare-locations', 'Every location tells a different story.', False),
    ('Water & climate insights', 'water-climate-insights', 'Turn observations into understanding.', True),
    ('Infrastructure', 'infrastructure', 'Put the climate in context.', False),
    ('Data & methodology', 'data-methodology', 'Trace every insight to its source.', False),
]


def gallery_markdown() -> str:
    lines = [START, '## Dashboard screenshots', '',
             'Representative desktop views captured from the real dashboard by the source project’s automated screenshot workflow.', '',
             '![Dashboard overview](docs/screenshots/overview.png)', '',
             '<details>', '<summary>View the other six dashboard sections</summary>', '']
    for name, slug, _, _ in VIEWS[1:]:
        lines += [f'### {name}', '', f'![{name}](docs/screenshots/{slug}.png)', '']
    return '\n'.join([*lines, '</details>', END])


def update_readme(path: Path, gallery: str) -> None:
    text = path.read_text(encoding='utf-8')
    if START in text or END in text:
        if text.count(START) != 1 or text.count(END) != 1:
            raise ValueError('README screenshot markers are incomplete or duplicated')
        pattern = re.compile(re.escape(START) + r'.*?' + re.escape(END), re.S)
        text = pattern.sub(lambda _: gallery, text, count=1)
    elif '## Actual data' in text:
        text = text.replace('## Actual data', gallery + '\n\n## Actual data', 1)
    else:
        text = text.rstrip() + '\n\n' + gallery + '\n'
    temporary = path.with_name(path.name + '.screenshots.tmp')
    temporary.write_text(text, encoding='utf-8')
    temporary.replace(path)


def wait_for_server(url: str, process, log: Path) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'Streamlit exited; inspect {log}')
        try:
            with urlopen(url.rstrip('/') + '/_stcore/health', timeout=3) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(.5)
    raise TimeoutError(f'Dashboard health did not become ready at {url}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--url', help='Capture an already-running dashboard instead of starting the prod snapshot')
    parser.add_argument('--browser-executable', type=Path, help='Use an installed Chromium-based browser')
    parser.add_argument('--no-readme', action='store_true', help='Generate images without changing either README or the production bundle')
    parser.add_argument('--width', type=int, default=1440)
    parser.add_argument('--height', type=int, default=1300)
    args = parser.parse_args()
    if args.width < 1000 or args.height < 800:
        parser.error('Use a desktop viewport of at least 1000 × 800')
    from playwright.sync_api import sync_playwright, expect

    runtime = ROOT / 'data/runtime/screenshots'
    runtime.mkdir(parents=True, exist_ok=True)
    destination = ROOT / 'docs/screenshots'
    log = runtime / 'streamlit.log'
    process = None
    log_stream = None
    url = args.url
    try:
        if url:
            parts = urlsplit(url)
            if parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username or parts.password:
                parser.error('--url must be an HTTP(S) URL without embedded credentials')
        else:
            if importlib.util.find_spec('streamlit') is None:
                raise RuntimeError('Install prod/requirements.txt in this Python environment, or use --url for the running Docker app')
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            url = f'http://127.0.0.1:{port}'
            log_stream = log.open('w', encoding='utf-8')
            process = subprocess.Popen([sys.executable, '-m', 'streamlit', 'run',
                str(ROOT / 'prod/streamlit_app.py'), '--server.address=127.0.0.1', f'--server.port={port}',
                '--server.headless=true', '--browser.gatherUsageStats=false'], cwd=ROOT,
                stdin=subprocess.DEVNULL, stdout=log_stream, stderr=subprocess.STDOUT)
        wait_for_server(url, process, log)
        with tempfile.TemporaryDirectory(prefix='capture-', dir=runtime) as temporary:
            stage = Path(temporary)
            captures = []
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    executable_path=args.browser_executable, headless=True, args=['--enable-unsafe-swiftshader'])
                try:
                    context = browser.new_context(viewport={'width': args.width, 'height': args.height},
                        device_scale_factor=1, color_scheme='light', locale='en-US', timezone_id='Africa/Casablanca',
                        reduced_motion='reduce')
                    page = context.new_page()
                    page.set_default_timeout(30000)
                    page.goto(url, wait_until='domcontentloaded')
                    expect(page.get_by_role('heading', name='MOROCCO / CLIMATE & WATER INTELLIGENCE', level=1)).to_be_visible()
                    sidebar = page.get_by_test_id('stSidebar')
                    basemap = sidebar.get_by_label('Online basemap', exact=True)
                    if basemap.is_checked():
                        sidebar.get_by_text('Online basemap', exact=True).click()
                    expect(basemap).not_to_be_checked()
                    for name, slug, title, full_history in VIEWS:
                        radio = sidebar.get_by_role('radio', name=name, exact=True)
                        if not radio.is_checked():
                            sidebar.get_by_text(name, exact=True).click()
                        expect(radio).to_be_checked()
                        expect(page.get_by_role('heading', name=title, exact=True)).to_be_visible()
                        sliders = sidebar.get_by_test_id('stSlider').get_by_role('slider')
                        lower, upper = sliders.first, sliders.last
                        expected = lower.get_attribute('min' if full_history else 'max')
                        if lower.input_value() != expected:
                            lower.press('Home' if full_history else 'End')
                        expect(lower).to_have_value(expected)
                        years = [int(lower.input_value()), int(upper.input_value())]
                        expect(page.get_by_text(f'{years[0]}–{years[1]} ·', exact=False).first).to_be_visible()
                        expect(page.get_by_test_id('stStatusWidget')).to_be_hidden(timeout=90000)
                        expect(page.get_by_test_id('stException')).to_have_count(0)
                        errors = page.get_by_text(re.compile(r'^(?:Gold data is unavailable|(?:Type|Reference|Syntax|Range)Error:)'))
                        expect(errors).to_have_count(0)
                        if name in {'Overview', 'Infrastructure'}:
                            map_chart = page.get_by_test_id('stDeckGlJsonChart').first
                            expect(map_chart.locator('canvas').first).to_be_visible(timeout=30000)
                        if page.locator('.js-plotly-plot').count():
                            expect(page.locator('.js-plotly-plot').first).to_be_visible()
                        page.evaluate('document.fonts.ready')
                        page.get_by_test_id('stMain').evaluate('(element) => element.scrollTo(0, 0)')
                        sidebar.get_by_test_id('stSidebarContent').evaluate('(element) => element.scrollTo(0, 0)')
                        page.mouse.move(0, 0)
                        page.wait_for_timeout(1000)  # Finish chart/canvas rendering after Streamlit's rerun.
                        path = stage / (slug + '.png')
                        page.screenshot(path=str(path), animations='disabled', caret='hide')
                        body = path.read_bytes()
                        captures.append({'page': name, 'file': f'docs/screenshots/{path.name}',
                            'year_scope': years, 'bytes': len(body),
                            'sha256': hashlib.sha256(body).hexdigest()})
                        print(f'Captured {name}: {len(body):,} bytes', flush=True)
                except Exception:
                    if 'page' in locals():
                        page.screenshot(path=str(runtime / 'failure.png'))
                        (runtime / 'failure.html').write_text(page.content(), encoding='utf-8')
                    raise
                finally:
                    browser.close()
            destination.mkdir(parents=True, exist_ok=True)
            for capture in captures:
                path = Path(capture['file'])
                shutil.move(str(stage / path.name), destination / path.name)
            snapshot = json.loads((ROOT / 'prod/bundle_manifest.json').read_text())
            manifest = {'captured_at': datetime.now(timezone.utc).isoformat(),
                'source': url.split('?', 1)[0], 'mode': 'existing server' if args.url else 'bundled Gold snapshot',
                'snapshot_publication_id': None if args.url else snapshot['publication_id'],
                'viewport': {'width': args.width, 'height': args.height}, 'online_basemap': False, 'captures': captures}
            (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
            (destination / 'gallery.md').write_text(gallery_markdown() + '\n', encoding='utf-8')
            if not args.no_readme:
                update_readme(ROOT / 'README.md', gallery_markdown())
                subprocess.run([sys.executable, str(ROOT / 'observatory/build_prod.py')], cwd=ROOT, check=True)
            print(f'Saved {len(captures)} screenshots in {destination}; README gallery updated: {not args.no_readme}', flush=True)
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if log_stream is not None:
            log_stream.close()


if __name__ == '__main__':
    main()
