# hdinsight-spark2.0-script-action
Script action to install Spark 2.0 on top of HDInsight Spark 1.6.x cluster (for development and experimental purposes).

This script action is limited at the moment only to basic Spark services in command line. 

Working Components: livy (jar submission), spark-sql, pyspark, spark-submit, spark-shell 

Non-working Components: Jupyter, livy (interactive)

## Installation instructions

1. Create HDInsight Spark cluster version 3.4 (Spark 1.6.1)
2. Run script action: `install-spark2-v01.sh` on this cluster. 

    Go to Azure portal > open cluster blade > open Script Actions tile > click Submit new and follow instructions. The script action is provided in this repository.
3. Update class path in the cluster configuration. 

    Open Ambari portal of the cluster, go to Spark > Configs > Advanced spark-env and update SPARK_DIST_CLASSPATH variable to the following value:

	```bash
    export SPARK_DIST_CLASSPATH=$SPARK_DIST_CLASSPATH:/usr/hdp/current/spark-historyserver/conf/:/usr/hdp/current/spark-client/jars/datanucleus-api-jdo-3.2.6.jar:/usr/hdp/current/spark-client/jars/datanucleus-rdbms-3.2.9.jar:/usr/hdp/current/spark-client/jars/datanucleus-core-3.2.10.jar:/etc/hadoop/conf/:/usr/lib/hdinsight-datalake/*:/usr/hdp/current/hadoop-client/hadoop-azure.jar:/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar:/usr/lib/hdinsight-logging/mdsdclient-1.0.jar:/usr/lib/hdinsight-logging/microsoft-log4j-etwappender-1.0.jar:/usr/lib/hdinsight-logging/json-simple-1.1.jar:/usr/hdp/current/hadoop/client/slf4j-log4j12.jar:/usr/hdp/current/hadoop/client/slf4j-api.jar:/usr/hdp/current/hadoop/hadoop-common.jar:/usr/hdp/current/hadoop/hadoop-azure.jar:/usr/hdp/current/hadoop/client/log4j.jar:/usr/hdp/current/hadoop/client/commons-configuration-1.6.jar:/usr/hdp/current/hadoop/lib/*:/usr/hdp/current/hadoop/client/*:/usr/hdp/current/spark-client/conf/:/usr/hdp/current/hadoop-yarn/hadoop-yarn-server-web-proxy.jar:/usr/hdp/current/spark-client/jars/spark-yarn_2.11-2.0.0.jar:/usr/hdp/current/spark-client/jars/*:
	```

4. Update `spark.yarn.jars` property on spark-defaults. 

    Open Ambari portal of the cluster, go to Spark > Configs > Custom spark-defaults.
	
	4.1. Remove `spark.yarn.jar` property
	
	4.2. Add `spark.yarn.jars` property and set its value to `local:///usr/hdp/current/spark-client/jars/*`. Note it's plural - spark.yarn.jar**s**
    
5. Add `spark.yarn.jars` update on spark-thrift-sparkconf. 

    Open Ambari portal of the cluster, go to Spark > Configs > Custom spark-thrift-sparkconf.
	
    Add `spark.yarn.jars` property and set its value to `local:///usr/hdp/current/spark-client/jars/*`. 

6. Restart affected Ambari services.
    
7. In an ssh session launch `spark-shell`, `spark-submit`, etc.
