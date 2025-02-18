#!/bin/bash

## USER CONFIGURED PARAMS ##
HIVE_METASTORE_VERSION="X.X.X"
HIVE_METASTORE_URIS="thrift://clusterXX-vm0.bdauto.wandisco.com:9083"
HIVE_PRINCIPAL="hive@WANDISCO.HADOOP"
HIVE_KEYTAB="/etc/security/keytabs/hive.service.keytab"
HDFS_PRINCIPAL="hdfs@WANDISCO.HADOOP"
HDFS_KEYTAB="/etc/security/keytabs/hdfs.headless.keytab"
SPARK_DL_LINK="https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3-scala2.13.tgz"
DB_NAME="delta_db"
HIVE_CONNECTOR_DL_LINK="https://github.com/delta-io/delta/releases/download/v3.2.0/delta-hive-assembly_2.13-3.2.0.zip"
EXTERNAL_PATH="/delta/"
VOLUME_1000=100
BIGINT=True
TIMESTAMP=True
STRUCT=True
ITERATIONS=1
############################

mkdir -p $EXTERNAL_PATH
chown -R hive:hadoop $EXTERNAL_PATH
if [ ! -d ./spark-*-bin-hadoop*-scala* ]; then
  wget --no-check-certificate $SPARK_DL_LINK
  tar xzvf spark-*-bin-hadoop*-scala*.tgz
  rm ./spark-*-bin-hadoop*-scala*.tgz
  wget -P ./spark-*-bin-hadoop*-scala*/jars/ https://repo1.maven.org/maven2/com/johnsnowlabs/nlp/spark-nlp_2.12/5.3.1/spark-nlp_2.12-5.3.1.jar
  wget -P ./spark-*-bin-hadoop*-scala*/jars/ https://repo1.maven.org/maven2/org/tensorflow/tensorflow/1.15.0/tensorflow-1.15.0.jar
  wget -P ./spark-*-bin-hadoop*-scala*/jars/ https://repo1.maven.org/maven2/org/tensorflow/ndarray/0.4.0/ndarray-0.4.0.jar
  wget -P ./spark-*-bin-hadoop*-scala*/jars/ https://repo1.maven.org/maven2/org/tensorflow/tensorflow-core-platform/0.5.0/tensorflow-core-platform-0.5.0.jar
  wget -P ./spark-*-bin-hadoop*-scala*/jars/ https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.2.0/delta-spark_2.13-3.2.0.jar
  wget -P ./spark-*-bin-hadoop*-scala*/jars/ https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
  if [ ! -d /opt/hive-aux-jars ]; then
    mkdir /opt/hive-aux-jars
    wget $HIVE_CONNECTOR_DL_LINK
    unzip delta-hive-assembly_*.zip
    cp delta-hive-assembly_*.jar /opt/hive-aux-jars
    rm -rf delta-hive-assembly_* __MACOSX/
  fi
  cp /etc/hadoop/conf/hdfs-site.xml ./spark-*-bin-hadoop*-scala*/conf/
  cp /etc/hadoop/conf/core-site.xml ./spark-*-bin-hadoop*-scala*/conf/
  cp /etc/hive/conf/hive-site.xml ./spark-*-bin-hadoop*-scala*/conf/
  cp /etc/hadoop/conf/yarn-site.xml ./spark-*-bin-hadoop*-scala*/conf/
  kinit -kt $HDFS_KEYTAB $HDFS_PRINCIPAL
  hdfs dfs -mkdir -p $EXTERNAL_PATH
  hdfs dfs -chown -R hive $EXTERNAL_PATH
  kdestroy
fi
pip3 install virtualenv
virtualenv env_spark -p /usr/bin/python3
source env_spark/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements.txt
for ((i = 1; i <= "$ITERATIONS"; i++)); do
  if [ "$i" = 1 ]; then
    DB_NAME_ITER="$DB_NAME"
  else
    DB_NAME_ITER="$DB_NAME"_"$i"
  fi
  ./spark-*-bin-hadoop*-scala*/bin/spark-submit \
    --packages io.delta:delta-spark_2.13:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.kerberos.keytab=$HIVE_KEYTAB \
    --conf spark.kerberos.principal=$HIVE_PRINCIPAL \
    --conf spark.hadoop.hive.metastore.uris=$HIVE_METASTORE_URIS \
    --conf spark.sql.hive.metastore.version=$HIVE_METASTORE_VERSION \
    --conf spark.sql.hive.metastore.jars=maven \
    --conf spark.driver.memory=2G \
    main.py $EXTERNAL_PATH $DB_NAME_ITER $VOLUME_1000 $BIGINT $TIMESTAMP $STRUCT
  kinit -kt $HIVE_KEYTAB $HIVE_PRINCIPAL
  hive -e "use $DB_NAME_ITER;
  SET hive.tez.input.format=io.delta.hive.HiveInputFormat;
  ADD JAR /opt/hive-aux-jars/delta-hive-assembly_2.13-3.2.0.jar;
  !run $EXTERNAL_PATH/TABLECREATE.sql;
  show tables;"
done
kdestroy
rm -r $EXTERNAL_PATH
