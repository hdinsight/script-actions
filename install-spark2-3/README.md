# hdinsight-spark2.3-script-action
Script action to install Spark 2.3.0 on top of HDInsight Spark 2.x cluster (for development and experimental purposes).

This script action is limited at the moment only to basic Spark services in command line. 

## Installation instructions

1. Create HDInsight Spark cluster version 3.6 (Spark 2.1.1)

2. Run script action: `install-spark2-3-v01.sh` on this cluster. 

    Go to Azure portal > open cluster blade > open Script Actions tile > click Submit new and follow instructions. The script action is provided in this repository.
    
    When you are on the "Submit script action" blade, you will see "Bash script URI" field. You need to make sure that the `install-spark2-3-v01.sh` is stored in an Azure Storage Blob, and make the link public.
    
    OR
    
    You can just add https://raw.githubusercontent.com/hdinsight/script-actions/master/install-spark2-3/install-spark2-3-v01.sh to "Bash script URI".
    
3. Update class path in the cluster configuration. 

    Open Ambari portal of the cluster, go to Spark2 > Configs > Advanced spark2-env and update SPARK_DIST_CLASSPATH variable to the following value:

	```bash
    export SPARK_DIST_CLASSPATH=$SPARK_DIST_CLASSPATH:/usr/hdp/current/spark-historyserver/conf/:/usr/hdp/current/spark2-client/jars/datanucleus-api-jdo-3.2.6.jar:/usr/hdp/current/spark2-client/jars/datanucleus-rdbms-3.2.9.jar:/usr/hdp/current/spark2-client/jars/datanucleus-core-3.2.10.jar:/etc/hadoop/conf/:/usr/lib/hdinsight-datalake/*:/usr/hdp/current/hadoop-client/hadoop-azure.jar:/usr/hdp/current/hadoop-client/lib/azure-storage-4.2.0.jar:/usr/lib/hdinsight-logging/mdsdclient-1.0.jar:/usr/lib/hdinsight-logging/microsoft-log4j-etwappender-1.0.jar:/usr/lib/hdinsight-logging/json-simple-1.1.jar:/usr/hdp/current/hadoop-client/client/slf4j-log4j12.jar:/usr/hdp/current/hadoop/client/slf4j-api.jar:/usr/hdp/current/hadoop/hadoop-common.jar:/usr/hdp/current/hadoop-client/hadoop-azure.jar:/usr/hdp/current/hadoop-client/client/log4j.jar:/usr/hdp/current/hadoop-client/client/commons-configuration-1.6.jar:/usr/hdp/current/hadoop-client/lib/*:/usr/hdp/current/hadoop-client/client/*:/usr/hdp/current/spark2-client/conf/:/usr/hdp/current/hadoop-yarn-client/hadoop-yarn-server-web-proxy.jar:/usr/hdp/current/spark2-client/jars/spark-yarn_2.11-2.3.0.jar:/usr/hdp/current/spark2-client/jars/*:
	```
   
4. Restart affected Ambari services.
    
6. Now you are ready to use Spark 2.3 on the cluster. In ssh session launch `spark-shell`, `spark-submit`, etc.

