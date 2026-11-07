FROM eclipse-temurin:8-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl postgresql-client && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz | tar -xz -C /opt && mv /opt/hadoop-3.3.4 /opt/hadoop
RUN curl -fsSL https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz | tar -xz -C /opt && mv /opt/apache-hive-3.1.3-bin /opt/hive
RUN curl -fsSL https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar -o /opt/hive/lib/postgres.jar
RUN rm -f /opt/hive/lib/guava-*.jar && cp /opt/hadoop/share/hadoop/common/lib/guava-*.jar /opt/hive/lib/
ENV HADOOP_HOME=/opt/hadoop HIVE_HOME=/opt/hive HADOOP_OPTIONAL_TOOLS=hadoop-aws
COPY docker/hive/hive-site.xml /opt/hive/conf/hive-site.xml
COPY docker/hive/start.sh /opt/start.sh
ENTRYPOINT ["bash", "/opt/start.sh"]
