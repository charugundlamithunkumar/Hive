#!/usr/bin/env bash
# Start a loopback-only HiveServer2 using the local Hadoop job runner and Derby.
set -euo pipefail
: "${HIVE_HOME:?Set HIVE_HOME to the extracted Hive 4.1.0 directory}"
: "${HADOOP_HOME:?Set HADOOP_HOME to the Hadoop installation directory}"
: "${JAVA_HOME:?Set JAVA_HOME to the JDK 17 directory}"

port=${HIVE_PORT:-10000}
if [[ ! "$port" =~ ^[1-9][0-9]{0,4}$ ]] || (( port > 65535 )); then
    echo "HIVE_PORT must be an integer between 1 and 65535" >&2
    exit 2
fi

state=${HIVE_STATE_DIR:-"$HOME/.local/state/nasa-hive"}
mkdir -p "$state"
cd "$state"
state=$(pwd -P)
xml_state=$(printf '%s' "$state" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')
export HADOOP_CONF_DIR="$state/hadoop-conf"
export HIVE_CONF_DIR="$state/hive-conf"
mkdir -p "$HADOOP_CONF_DIR" "$HIVE_CONF_DIR" "$state/warehouse"
export HADOOP_HEAPSIZE_MAX=${HADOOP_HEAPSIZE_MAX:-2048}
cat > "$HADOOP_CONF_DIR/core-site.xml" <<'XML'
<configuration>
  <property><name>fs.defaultFS</name><value>file:///</value></property>
</configuration>
XML
cat > "$HIVE_CONF_DIR/hive-site.xml" <<XML
<configuration>
  <property><name>hive.server2.thrift.bind.host</name><value>127.0.0.1</value></property>
  <property><name>hive.server2.thrift.port</name><value>$port</value></property>
  <property><name>hive.server2.webui.port</name><value>0</value></property>
  <property><name>hive.server2.authentication</name><value>NOSASL</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.execution.engine</name><value>mr</value></property>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>file://$xml_state/warehouse</value></property>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:$xml_state/metastore_db;create=true</value></property>
</configuration>
XML
if [[ ! -d "$state/metastore_db" ]]; then
    "$HIVE_HOME/bin/schematool" -dbType derby -initSchema
fi
exec "$HIVE_HOME/bin/hiveserver2"
