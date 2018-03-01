#!/bin/bash

newspark="spark-2.3.0"

SPARK_DIR="$(readlink -f "/usr/hdp/current/spark2-client")"
SPARK_CONF_DIR="$(readlink -f "/usr/hdp/current/spark2-client/conf")"
CURRENT_DIR=${SPARK_DIR%/spark2}
HADOOP_DIR="$CURRENT_DIR/hadoop"
HADOOP_YARN_DIR="$CURRENT_DIR/hadoop-yarn"

## Download & Install Binary
cd "/tmp"
curl "http://apache.claz.org/spark/$newspark/$newspark-bin-hadoop2.7.tgz" | tar xzf -
cd "$newspark"
rm -r "jars/hadoop"* "conf"
ln -s "$SPARK_CONF_DIR" "conf"
cd ..
rm -r "$SPARK_DIR"
mv "$newspark" "$SPARK_DIR"

# Create symlinks
sudo ln -sfn "$SPARK_DIR/yarn/$newspark-yarn-shuffle.jar" \
   "$HADOOP_DIR/lib/spark-yarn-shuffle.jar"
sudo ln -sf $HADOOP_DIR /usr/hdp/current/hadoop
sudo ln -sf $HADOOP_YARN_DIR /usr/hdp/current/hadoop-yarn   

echo "$newspark installation completed"
