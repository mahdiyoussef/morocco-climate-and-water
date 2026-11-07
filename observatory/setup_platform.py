"""Generate local-only platform credentials without replacing existing configuration."""
import base64
import secrets
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    target = ROOT / '.env.morocco'
    values = {
        'POSTGRES_USER': 'morocco_admin', 'POSTGRES_PASSWORD': secrets.token_hex(24),
        'MINIO_ROOT_USER': 'morocco_storage', 'MINIO_ROOT_PASSWORD': secrets.token_hex(24),
        'PIPELINE_WRITER_PASSWORD': secrets.token_hex(24), 'BI_READER_PASSWORD': secrets.token_hex(24),
        'AIRFLOW_FERNET_KEY': base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        'AIRFLOW_SECRET_KEY': secrets.token_hex(32), 'AIRFLOW_ADMIN_PASSWORD': secrets.token_hex(16),
        'GRAFANA_PASSWORD': secrets.token_hex(16), 'SUPERSET_SECRET_KEY': secrets.token_hex(32),
        'SUPERSET_ADMIN_PASSWORD': secrets.token_hex(16), 'AIRFLOW_UID': str(os.getuid()) if hasattr(os, 'getuid') else '50000',
    }
    if target.exists():
        print('.env.morocco already exists; retained.')
    else:
        with target.open('x', encoding='utf-8', newline='\n') as output:
            output.write('# Local development credentials; ignored by Git.\n')
            output.writelines(f'{key}={value}\n' for key, value in values.items())
        print('Created .env.morocco; credentials were not printed.')
    for directory in ['data/runtime/airflow-logs', 'dbt/target', 'dbt/logs']:
        (ROOT / directory).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    main()
