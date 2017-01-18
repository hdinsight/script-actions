#!/bin/bash

newspark="spark-2.1.0-bin-hadoop2.7"

SPARK_DIR="$(readlink -f "/usr/hdp/current/spark2-client")"
SPARK_CONF_DIR="$(readlink -f "/usr/hdp/current/spark2-client/conf")"
CURRENT_DIR=${SPARK_DIR%/spark}
HADOOP_DIR="$CURRENT_DIR/hadoop"
HADOOP_YARN_DIR="$CURRENT_DIR/hadoop-yarn"

## Download & Install Binary
cd "/tmp"
curl "https://www.apache.org/dist/spark/spark-2.1.0/$newspark.tgz" | tar xzf -
cd "$newspark"
rm -r "jars/hadoop"* "conf"
ln -s "$SPARK_CONF_DIR" "conf"
cd ..
rm -r "$SPARK_DIR"
mv "$newspark" "$SPARK_DIR"

# Create symlinks
sudo ln -sfn "$SPARK_DIR/yarn/spark-2.1.0-yarn-shuffle.jar" \
   "$HADOOP_DIR/lib/spark-yarn-shuffle.jar"
sudo ln -s $HADOOP_DIR /usr/hdp/current/hadoop
sudo ln -s $HADOOP_YARN_DIR /usr/hdp/current/hadoop-yarn   

echo "Spark 2.1 installation completed"
