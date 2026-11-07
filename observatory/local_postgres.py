"""Start a workspace-local PostgreSQL server from official EDB Windows binaries."""
import argparse
import hashlib
import json
import io
import os
import subprocess
import sys
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from observatory.serving import load_environment

VERSION = '15.19-1'
URL = f'https://get.enterprisedb.com/postgresql/postgresql-{VERSION}-windows-x64-binaries.zip'
ARCHIVE = ROOT / f'.tools/postgresql-{VERSION}.zip'
BIN = ROOT / '.tools/postgres/pgsql/bin'
PGDATA = ROOT / 'data/runtime/postgres'


def download():
    import requests
    if (BIN / 'pg_ctl.exe').exists():
        print('Workspace PostgreSQL binaries already exist.')
        return
    partial = ARCHIVE.with_suffix('.zip.partial')
    # Read the ZIP directory and selected members with HTTP ranges. The official
    # distribution also contains large GUI tools that this server does not use.
    class RemoteArchive(io.RawIOBase):
        def __init__(self):
            self.session = requests.Session()
            response = self.session.get(URL, headers={'Range': 'bytes=0-0'}, timeout=(20, 60))
            response.raise_for_status()
            if response.status_code != 206 or 'Content-Range' not in response.headers:
                raise ValueError('Official download host did not support byte ranges')
            self.length = int(response.headers['Content-Range'].rsplit('/', 1)[1])
            self.position = 0
            self.cache = {}

        def seek(self, offset, whence=0):
            self.position = offset if whence == 0 else self.position + offset if whence == 1 else self.length + offset
            return self.position

        def tell(self):
            return self.position

        def read(self, size=-1):
            end = self.length if size < 0 else min(self.position + size, self.length)
            blocks = []
            while self.position < end:
                page = self.position // (1024 * 1024)
                start = page * 1024 * 1024
                if page not in self.cache:
                    finish = min(start + 1024 * 1024, self.length)
                    if partial.exists() and finish <= partial.stat().st_size:
                        with partial.open('rb') as stream:
                            stream.seek(start)
                            body = stream.read(finish - start)
                    else:
                        response = self.session.get(URL, headers={'Range': f'bytes={start}-{finish - 1}'}, timeout=(20, 60))
                        response.raise_for_status()
                        if response.status_code != 206 or not response.headers.get('Content-Range', '').startswith(f'bytes {start}-'):
                            raise ValueError('Unexpected HTTP range response')
                        body = response.content
                    if len(body) != finish - start:
                        raise ValueError('Incomplete official ZIP byte range')
                    self.cache[page] = body
                    if len(self.cache) > 8:
                        self.cache.pop(next(iter(self.cache)))
                body = self.cache[page]
                offset = self.position - start
                amount = min(len(body) - offset, end - self.position)
                blocks.append(body[offset:offset + amount])
                self.position += amount
            return b''.join(blocks)

    if not ARCHIVE.exists():
        destination = ROOT / '.tools/postgres'
        destination.mkdir(parents=True, exist_ok=True)
        remote = RemoteArchive()
        entries = []
        with zipfile.ZipFile(remote) as archive:
            for entry in archive.infolist():
                if not entry.filename.startswith(('pgsql/bin/', 'pgsql/lib/', 'pgsql/share/')):
                    continue
                target = (destination / entry.filename).resolve()
                if not target.is_relative_to(destination.resolve()):
                    raise ValueError('Archive path escaped the workspace runtime directory')
                archive.extract(entry, destination)
                entries.append(entry.filename)
        if not (BIN / 'pg_ctl.exe').exists():
            raise ValueError('Official ZIP is missing the PostgreSQL server binaries')
        (destination / 'download.json').write_text(json.dumps({'url': URL, 'archive_bytes': remote.length, 'method': 'HTTP byte ranges; ZIP member CRC verified', 'extracted_members': len(entries)}, indent=2))
        print(f'Extracted {len(entries)} PostgreSQL bin/lib/share members from the official archive; GUI tools were not downloaded.')
        return
    if not ARCHIVE.exists():
        offset = partial.stat().st_size if partial.exists() else 0
        response = requests.get(URL, headers={'Range': f'bytes={offset}-'} if offset else {}, stream=True, timeout=(20, 60))
        response.raise_for_status()
        mode = 'ab' if offset and response.status_code == 206 else 'wb'
        written = offset if mode == 'ab' else 0
        print(f'Downloading official PostgreSQL {VERSION} binaries.', flush=True)
        last_update = time.monotonic()
        with partial.open(mode) as output:
            for block in response.iter_content(1024 * 1024):
                output.write(block)
                written += len(block)
                if time.monotonic() - last_update > 20:
                    print(f'PostgreSQL archive: {written / 1e6:.1f} MB received', flush=True)
                    last_update = time.monotonic()
        partial.replace(ARCHIVE)
    destination = ROOT / '.tools/postgres'
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ARCHIVE) as archive:
        for entry in archive.infolist():
            if not entry.filename.startswith(('pgsql/bin/', 'pgsql/lib/', 'pgsql/share/')):
                continue
            target = (destination / entry.filename).resolve()
            if not target.is_relative_to(destination.resolve()):
                raise ValueError('Archive path escaped the workspace runtime directory')
            archive.extract(entry, destination)
    if not (BIN / 'pg_ctl.exe').exists():
        raise ValueError('PostgreSQL archive is missing pg_ctl.exe')
    digest = hashlib.sha256()
    with ARCHIVE.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    (destination / 'download.json').write_text(json.dumps({'url': URL, 'bytes': ARCHIVE.stat().st_size, 'sha256': digest.hexdigest()}, indent=2))
    print('Extracted PostgreSQL bin/lib/share under .tools; no system installation or Windows service was created.')


def command(name, *args, check=True):
    result = subprocess.run([str(BIN / (name + '.exe')), *map(str, args)], capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    if check and result.returncode:
        raise RuntimeError(f'{name} failed: {result.stderr or result.stdout}')
    return result


def start():
    load_environment()
    PGDATA.parent.mkdir(parents=True, exist_ok=True)
    if not (PGDATA / 'PG_VERSION').exists():
        password = PGDATA.parent / '.initdb-password'
        try:
            password.write_text(os.environ['POSTGRES_PASSWORD'], encoding='utf-8')
            command('initdb', '-D', PGDATA, '-U', os.environ['POSTGRES_USER'], '--pwfile', password,
                    '--auth-host=scram-sha-256', '--auth-local=scram-sha-256', '--encoding=UTF8', '--locale=C')
        finally:
            password.unlink(missing_ok=True)
    if command('pg_ctl', '-D', PGDATA, 'status', check=False).returncode != 0:
        command('pg_ctl', '-D', PGDATA, '-l', PGDATA.parent / 'postgres.log', '-o', '-h 127.0.0.1 -p 5432', '-w', 'start')
    print('Workspace PostgreSQL is listening on 127.0.0.1:5432.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['download', 'start', 'stop'])
    stage = parser.parse_args().stage
    if stage == 'download':
        download()
    elif stage == 'start':
        start()
    else:
        command('pg_ctl', '-D', PGDATA, '-m', 'fast', '-w', 'stop')
