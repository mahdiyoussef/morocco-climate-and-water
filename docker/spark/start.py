"""Foreground Spark processes with the same Hive, Delta and object-store settings."""
import os
import sys

os.umask(0o002)

home = os.environ['SPARK_HOME']
mode = sys.argv[1]
common = {
    'spark.master': 'spark://spark-master:7077',
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    'spark.sql.warehouse.dir': 's3a://warehouse/morocco',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.hive.metastore.jars': 'maven',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': os.environ.get('AWS_ACCESS_KEY_ID', ''),
    'spark.hadoop.fs.s3a.secret.key': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    'spark.sql.session.timeZone': 'UTC',
    'spark.driver.memory': '1g',
    'spark.executor.memory': '1g',
    'spark.cores.max': '2',
    'spark.ui.prometheus.enabled': 'true',
    'spark.metrics.conf.*.sink.prometheusServlet.class': 'org.apache.spark.metrics.sink.PrometheusServlet',
    'spark.metrics.conf.*.sink.prometheusServlet.path': '/metrics/prometheus',
}
if mode == 'master':
    args = [home + '/bin/spark-class', 'org.apache.spark.deploy.master.Master', '--host', 'spark-master', '--webui-port', '8080']
elif mode == 'worker':
    args = [home + '/bin/spark-class', 'org.apache.spark.deploy.worker.Worker', '--cores', '2', '--memory', '2g', 'spark://spark-master:7077']
elif mode == 'thrift':
    args = [home + '/bin/spark-submit', '--class', 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2']
    for key, value in common.items():
        args += ['--conf', f'{key}={value}']
    args += ['--conf', 'spark.driver.host=spark-thrift-server', '--name', 'Morocco SQL',
             'spark-internal', '--hiveconf', 'hive.server2.thrift.port=10000',
             '--hiveconf', 'hive.server2.authentication=NOSASL']
else:
    raise ValueError(mode)
os.execv(args[0], args)
