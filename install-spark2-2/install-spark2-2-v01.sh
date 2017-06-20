#!/bin/bash

newspark="spark-2.2.0-bin-hadoop2.7"
http://home.apache.org/~pwendell/spark-releases/spark-2.2.0-rc4-bin/spark-2.2.0-bin-hadoop2.7.tgz

SPARK_DIR="$(readlink -f "/usr/hdp/current/spark2-client")"
SPARK_CONF_DIR="$(readlink -f "/usr/hdp/current/spark2-client/conf")"
CURRENT_DIR=${SPARK_DIR%/spark2}
HADOOP_DIR="$CURRENT_DIR/hadoop"
HADOOP_YARN_DIR="$CURRENT_DIR/hadoop-yarn"

## Download & Install Binary
cd "/tmp"
curl "http://home.apache.org/~pwendell/spark-releases/spark-2.2.0-rc4-bin/$newspark.tgz" | tar xzf -
cd "$newspark"
rm -r "jars/hadoop"* "conf"
ln -s "$SPARK_CONF_DIR" "conf"
cd ..
rm -r "$SPARK_DIR"
mv "$newspark" "$SPARK_DIR"

# Create symlinks
sudo ln -sfn "$SPARK_DIR/yarn/spark-2.2.0-yarn-shuffle.jar" \
   "$HADOOP_DIR/lib/spark-yarn-shuffle.jar"
sudo ln -sf $HADOOP_DIR /usr/hdp/current/hadoop
sudo ln -sf $HADOOP_YARN_DIR /usr/hdp/current/hadoop-yarn   

echo "Spark 2.2.0-rc4 installation completed"
