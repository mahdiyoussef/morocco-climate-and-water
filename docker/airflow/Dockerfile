FROM apache/airflow:2.10.5-python3.12 AS dependencies
USER root
RUN apt-get -o Acquire::Retries=5 update && apt-get -o Acquire::Retries=5 install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
COPY observatory/requirements.txt /tmp/requirements-data.txt
RUN pip install --no-cache-dir --timeout 300 --retries 10 "apache-airflow==2.10.5" -r /tmp/requirements-data.txt "boto3>=1.35,<2" "psycopg[binary]>=3.2,<4" "prometheus-client>=0.21,<1" 'PyHive[hive_pure_sasl]>=0.7,<0.8' statsd
RUN python -m venv /home/airflow/dbt-venv && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --timeout 300 --retries 10 'dbt-core>=1.10.6,<1.11' 'dbt-spark[PyHive]>=1.9,<1.10'
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}" PYTHONPATH=/workspace
FROM dependencies
USER root
COPY --from=morocco-spark:local /usr/local/lib/python3.11/site-packages/pyspark/ /home/airflow/.local/lib/python3.12/site-packages/pyspark/
COPY --from=morocco-spark:local /usr/local/lib/python3.11/site-packages/py4j/ /home/airflow/.local/lib/python3.12/site-packages/py4j/
RUN ln -s /home/airflow/dbt-venv/bin/dbt /usr/local/bin/dbt
USER airflow
RUN pip install --no-cache-dir --timeout 300 --retries 10 'pandas>=2.2,<3' && python -c "import pyarrow as pa; pa.Table.from_pylist([{'value': 1}])"
WORKDIR /workspace
