FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --timeout 300 --retries 10 pyspark==3.5.3 delta-spark==3.2.1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
# Bake jars into the image so tasks and the Thrift service do not fetch at runtime.
COPY docker/spark/bootstrap.py /opt/bootstrap.py
RUN mkdir -p /tmp/jars && spark-submit --master 'local[1]' --packages io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5 --conf spark.jars.ivy=/tmp/jars /opt/bootstrap.py && find /tmp/jars/jars -name '*.jar' -exec cp {} ${SPARK_HOME}/jars/ \;
WORKDIR /workspace
COPY docker/spark/metrics.properties /usr/local/lib/python3.11/site-packages/pyspark/conf/metrics.properties
COPY docker/spark/start.py /opt/start.py
ENTRYPOINT ["python", "/opt/start.py"]
