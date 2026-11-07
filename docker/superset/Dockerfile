FROM apache/superset:4.1.1
USER root
RUN pip install --no-cache-dir psycopg2-binary redis
USER superset
COPY docker/superset/config.py /app/pythonpath/superset_config.py
COPY docker/superset/bootstrap.py /app/pythonpath/bootstrap.py
