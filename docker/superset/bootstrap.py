"""Register the read-only Gold database in the optional SQL exploration service."""
import os
from urllib.parse import quote
from superset.app import create_app
from superset.extensions import db
from superset.models.core import Database

app = create_app()
with app.app_context():
    name = 'Morocco Gold (read only)'
    database = db.session.query(Database).filter_by(database_name=name).one_or_none()
    if database is None:
        database = Database(database_name=name)
        db.session.add(database)
    password = quote(os.environ['BI_READER_PASSWORD'], safe='')
    database.sqlalchemy_uri = f'postgresql+psycopg2://bi_reader:{password}@postgres:5432/observatory'
    database.expose_in_sqllab = True
    database.allow_dml = False
    database.allow_ctas = False
    database.allow_cvas = False
    db.session.commit()
print('Registered read-only Morocco Gold connection; credentials were not printed.')
