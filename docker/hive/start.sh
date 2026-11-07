#!/bin/bash
set -euo pipefail
export HADOOP_CLASSPATH="$("$HADOOP_HOME/bin/hadoop" classpath --glob):$HADOOP_HOME/share/hadoop/tools/lib/*"
if ! "$HIVE_HOME/bin/schematool" -dbType postgres -info; then
  "$HIVE_HOME/bin/schematool" -dbType postgres -initSchema
fi
exec "$HIVE_HOME/bin/hive" --service metastore -p 9083
