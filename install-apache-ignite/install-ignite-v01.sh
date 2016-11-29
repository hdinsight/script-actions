#! /bin/bash


## BASIC FUNCTIONS ##
function package_exists() {
    return dpkg -l "$1" &> /dev/null
}

usage() {
    echo ""
    echo "Usage: sudo -E bash install-ignite-app.sh";
    echo "This script does NOT require Ambari username and password";
    exit 132;
}

# RUN ONLY USING ROOT
if [ "$(id -u)" != "0" ]; then
    echo "[ERROR] The script has to be run as root."
    usage
fi

# INSTALL XMLSTARLET PACKAGE
if ! package_exists xmlstarlet ; then
	apt install xmlstarlet
fi

## IMPORT HELPER MODULE ##
echo "import HDInsight utilities script"
wget -O /tmp/HDInsightUtilities-v01.sh -q https://hdiconfigactions.blob.core.windows.net/linuxconfigactionmodulev01/HDInsightUtilities-v01.sh && source /tmp/HDInsightUtilities-v01.sh && rm -f /tmp/HDInsightUtilities-v01.sh

## GET AMBARI USERNAME AND PASSWORD ##
USERID=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)
echo "USERID=$USERID"
PASSWD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python)

PRIMARYHEADNODE=`get_primary_headnode`
SECONDARYHEADNODE=`get_secondary_headnode`

## Check if values retrieved are empty, if yes, exit with error ##
if [[ -z $PRIMARYHEADNODE ]]; then
	echo "Could not determine primary headnode."
	exit 139
fi
    
if [[ -z $SECONDARYHEADNODE ]]; then
	echo "Could not determine secondary headnode."
	exit 140
fi

## DEFINE ENVIRONMENT VARIABLES ##
export HADOOP_HOME="/usr/hdp/current/hadoop-client";
export HADOOP_COMMON_HOME="/usr/hdp/current/hadoop-client";
export HADOOP_HDFS_HOME="/usr/hdp/current/hadoop-hdfs-client";
export HADOOP_MAPRED_HOME="/usr/hdp/current/hadoop-mapreduce-client";

IGNITE_BINARY="apache-ignite-hadoop-1.7.0-bin";
IGNITE_BINARY_URI="https://www.apache.org/dist/ignite/1.7.0/$IGNITE_BINARY.zip";
IGNITE_TMPFOLDER=/tmp/ignite
export IGNITE_HOME_DIR="/hadoop/ignite";
export IGNITE_HOME="$IGNITE_HOME_DIR/$IGNITE_BINARY";

AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
PORT=8080
ACTIVEAMBARIHOST=headnodehost

export AMBARI_ADMIN=$USERID
export AMBARI_PWD=$PASSWD

echo "Defined necessary environment variables before defining functions.."

## ASSISTING FUNCTIONS ##
checkHostNameAndSetClusterName() {
    fullHostName=$(hostname -f)
    echo "fullHostName=$fullHostName"
    CLUSTERNAME=$(sed -n -e 's/.*\.\(.*\)-ssh.*/\1/p' <<< $fullHostName)
    if [ -z "$CLUSTERNAME" ]; then
        CLUSTERNAME=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)
        if [ $? -ne 0 ]; then
            echo "[ERROR] Cannot determine cluster name. Exiting!"
            exit 133
        fi
    fi
    echo "Cluster Name=$CLUSTERNAME"  
}

validateUsernameAndPassword() {
    coreSiteContent=$(bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT get $ACTIVEAMBARIHOST $CLUSTERNAME core-site)
    if [[ $coreSiteContent == *"[ERROR]"* && $coreSiteContent == *"Bad credentials"* ]]; then
        echo "[ERROR] Username and password are invalid. Exiting!"
        exit 134
    fi
}

updateAmbariConfigs() {
    updateResult=$(bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT set $ACTIVEAMBARIHOST $CLUSTERNAME core-site "fs.igfs.impl" "org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem")
    
    if [[ $updateResult != *"Tag:version"* ]] && [[ $updateResult == *"[ERROR]"* ]]; then
        echo "[ERROR] Failed to update core-site for property: 'fs.igfs.impl', Exiting!"
        echo $updateResult
        exit 135
    fi
    
    echo "Updated core-site.xml with fs.igfs.impl = org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem"
    
    updateResult=$(bash $AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT set $ACTIVEAMBARIHOST $CLUSTERNAME core-site "fs.AbstractFileSystem.igfs.impl" "org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem")
    
    if [[ $updateResult != *"Tag:version"* ]] && [[ $updateResult == *"[ERROR]"* ]]; then
        echo "[ERROR] Failed to update core-site for property: 'fs.AbstractFileSystem.igfs.impl', Exiting!"
        echo $updateResult
        exit 135
    fi
    
    echo "Updated core-site.xml with fs.AbstractFileSystem.igfs.impl = org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem"
}

stopServiceViaRest() {
    if [ -z "$1" ]; then
        echo "Need service name to stop service"
        exit 136
    fi
    SERVICENAME=$1
    echo "Stopping $SERVICENAME"
    curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop Service for Apache Ignite installation"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/$SERVICENAME
    sleep 2
}

startServiceViaRest() {
    if [ -z "$1" ]; then
        echo "Need service name to start service"
        exit 136
    fi
    sleep 2
    SERVICENAME=$1
    echo "Starting $SERVICENAME"
    startResult=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start Service for Apache Ignite installation"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/$SERVICENAME)
    if [[ $startResult == *"500 Server Error"* || $startResult == *"internal system exception occurred"* ]]; then
        sleep 60
        echo "Retry starting $SERVICENAME"
        startResult=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start Service for Apache Ignite installation"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/$SERVICENAME)
    fi
    echo $startResult
}

downloadAndUnzipApacheIgnite() {
    #KILL APACHE IGNITE IF RUNNING
	ignitepid=`ps -ef | grep ignite | grep default-config.xml | awk '{print $2}'`
	if [ ! -z "$ignitepid" ]; then
		echo "killing running Apache Ignite instance"
		kill -9 $ignitepid
	fi
	
	#DELETE APACHE IGNITE BEFORE INSTALLING
	if [ -d "$IGNITE_HOME" ]; then 
		echo "Removing existing Ignite binaries: $IGNITE_HOME_DIR/$IGNITE_BINARY"
		rm -r $IGNITE_HOME_DIR/ 
	fi
	
	echo "Downloading Apache Ignite"
	mkdir -p $IGNITE_HOME_DIR
	wget -P $IGNITE_HOME_DIR $IGNITE_BINARY_URI;
	
	echo "Unzipping Apache Ignite"
	unzip $IGNITE_HOME_DIR/$IGNITE_BINARY.zip -d $IGNITE_HOME_DIR;
	
	echo "Remove Apache Ignite zip file"
	rm $IGNITE_HOME_DIR/$IGNITE_BINARY.zip;
}

updateApacheSparkConfig(){
	echo "backing up spark-env.sh to $IGNITE_HOME"
	cp $SPARK_HOME/conf/spark-env.sh $IGNITE_HOME/config/spark-env.sh.backup;
	
	# save the whole spark json config 
	$AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT get $ACTIVEAMBARIHOST $CLUSTERNAME spark-env | sed '/properties/,$!d' > original_spark.json
	
	# get spark-env.sh contents
	sparkenv_original="`$AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT get $ACTIVEAMBARIHOST $CLUSTERNAME spark-env | grep -o '"content.*"' | sed 's/"content"[ ^I]:[ ^I]//g'| sed 's/"//g'`"
	# replace \n with \\n
	sparkenv_original=$(echo $sparkenv_original | sed -r 's/\\n+/\\\\n/g')
	
	# Ignite updates into spark-env.sj
	sparkenv_ignite="IGNITE_HOME=/hadoop/ignite/apache-ignite-hadoop-1.7.0-bin\\\\n IGNITE_LIBS=\${IGNITE_HOME}/libs/*\\\\n for file in \${IGNITE_LIBS}\\\\n do\\\\n IGNITE_LIBS=\${IGNITE_LIBS}:\${file}/*\\\\n done\\\\n export SPARK_CLASSPATH=\$SPARK_CLASSPATH:\$IGNITE_LIBS"
	
	# original spark + Ignite updates
	sparkenv_updated="$sparkenv_original\\\\n$sparkenv_ignite"
	
	# replace in the original json config using different delimiter in sed
	sed -i "s~^\(\"content\"*:*\).*$~\1:\"$sparkenv_updated\",~" original_spark.json
	
	# upload the spark-env.sh to Ambari
	$AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT set $ACTIVEAMBARIHOST $CLUSTERNAME spark-env original_spark.json

	# remove the json file
	rm original_spark.json
	echo "Spark spark-env.sh is updated.."
}

updateApacheIgniteConfig(){

	# extract default file system from core-site.xml
	FS_DEFAULT_DFS=`$AMBARICONFIGS_SH -u $USERID -p $PASSWD -port $PORT get $ACTIVEAMBARIHOST $CLUSTERNAME core-site | grep -o '"wasb:.*"' | sed 's/"//g'`
    	echo "fs.defaultFS=$FS_DEFAULT_DFS"
	
	# extract worker nodes from ambari hosts
	WORKER_NODES=(`curl -k -s -u $USERID:$PASSWD "http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/hosts" | grep -o '"[hw]n.*"' | sed 's/"//g'`)
	echo "worker nodes = ${WORKER_NODES}"

	#append and change ignite default config xml
	cd $IGNITE_HOME/config;
	echo "uncommenting the secondaryFileSystem lines"
	sed '/^\s*<!--/!b;N;/name="secondaryFileSystem"/s/.*\n//;T;:a;n;/^\s*-->/!ba;d' default-config.xml > sdfs-default-config.xml;
	
	#enable discovery services
	echo "uncommenting the discoverySpi lines"
	sed '/^\s*<!--/!b;N;/name="discoverySpi"/s/.*\n//;T;:a;n;/^\s*-->/!ba;d' sdfs-default-config.xml > sdfs-dspi-default-config.xml;
	
	#replace hdfs path
	echo "change default dfs to wasb"
	xmlstarlet ed -N x="http://www.springframework.org/schema/beans" -u "//x:property[@value='hdfs://your_hdfs_host:9000']/@value" -v "$FS_DEFAULT_DFS" sdfs-dspi-default-config.xml > ignite-default-config-wasb.xml;
	
	#add new property element
	echo "adding new empty property element"
	xmlstarlet ed -N x="http://www.springframework.org/schema/beans" -s "//x:bean[@class='org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory']" -t elem -n property -v "" ignite-default-config-wasb.xml > ignite-default-config-emptyprop.xml
	
	#add configPaths attribute to the empty property element
	echo "adding configPaths attribute name to the empty property element"
	xmlstarlet ed -N x="http://www.springframework.org/schema/beans" -a "//x:bean[@class='org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory']/x:property[not(@value='$FS_DEFAULT_DFS')]" -t attr -n name -v "configPaths" ignite-default-config-emptyprop.xml > ignite-default-config-prop.xml;
	
	#add list to configPaths property
	echo "adding empty list element to the configPaths prop"
	xmlstarlet ed -N x="http://www.springframework.org/schema/beans" -s "//x:property[@name='configPaths']" -t elem -n list -v "" ignite-default-config-prop.xml > ignite-default-config-list.xml;
	
	#add value element to list
	echo "add a value element inside the list"
	xmlstarlet ed -N x="http://www.springframework.org/schema/beans" -s "//x:property[@name='configPaths']/x:list" -t elem -n value -v "/usr/hdp/current/hadoop-client/conf/core-site.xml" ignite-default-config-list.xml > default-config-sdfs.xml;
	
	#remove default discoverySpi entry
	echo "remove default discoverySpi entry"
	xmlstarlet ed -N x="http://www.springframework.org/schema/beans" -d "//x:property[@name='addresses']/x:list/x:value" default-config-sdfs.xml > default-config.xml
	
	echo "add worker nodes ip addresses to discoverySpi"
	for node in "${WORKER_NODES[@]}"
	do
		#add worker nodes entries
		xmlstarlet ed --inplace -N x="http://www.springframework.org/schema/beans" -s "//x:property[@name='addresses']/x:list" -t elem -n value -v "$node:47500..47509" default-config.xml
	done
	
	rm sdfs-default-config.xml;
	rm sdfs-dspi-default-config.xml;
	rm ignite-default-config-wasb.xml;
	rm ignite-default-config-emptyprop;
	rm ignite-default-config-prop.xml;
	rm ignite-default-config-list.xml;
	rm default-config-sdfs.xml;
	
	echo "Updated Ignite default-config.xml"
}

setupApacheIgniteService(){
	echo "Remove Ignite ignite-spark 2.11 scala folder"
	rm -R $IGNITE_HOME/libs/ignite-spark;
	
	echo "change Ignite HOME files and folders permissions"
	find $IGNITE_HOME -type d -exec chmod 755 {} \;
	find $IGNITE_HOME -type f -exec chmod 755 {} \;
	
	echo "Creating Ignite Symlinks into Hadoop Libs"
	cd $HADOOP_HOME/lib;
	ln -sf $IGNITE_HOME/libs/ignite-core-1.7.0.jar;
	ln -sf $IGNITE_HOME/libs/ignite-shmem-1.0.0.jar;
	ln -sf $IGNITE_HOME/libs/ignite-hadoop/ignite-hadoop-1.7.0.jar;
	
	echo "Creating Hadoop Azure Symlinks into Ignite Libs"
	cd $IGNITE_HOME/libs;
	ln -sf /usr/hdp/current/hadoop-client/hadoop-azure.jar;
	ln -sf /usr/hdp/current/hadoop-client/lib/azure-storage-4.2.0.jar;
	ln -sf /usr/hdp/current/hadoop-client/lib/azure-keyvault-core-0.8.0.jar;
	
	echo "create a symlink for HADOOP_COMMON needed by Ignite"
	mkdir -p $HADOOP_HOME/share/hadoop/common/;
	ln -sf $HADOOP_HOME/lib $HADOOP_HOME/share/hadoop/common/;
	echo "created symlink from $HADOOP_HOME/share/hadoop/common/lib; to $HADOOP_HOME/lib"
	
	echo "make sure Ignite bin scripts are executable"
	cd $IGNITE_HOME;
	chmod 777 bin/*.sh;
	
	echo "make sure Ignite config are writeable"
	chmod -R +w config/;
	
	echo "make sure any user can write to Ignite work directory"
	mkdir -p $IGNITE_HOME/work/;
	#chown -R $SSH_USER. $IGNITE_HOME/work/;
	chmod -R 777 $IGNITE_HOME/work/
}

startApacheIgnite(){
	cd $IGNITE_HOME/;
	echo "starting Apache Ignite in background"
	export HADOOP_HOME="/usr/hdp/current/hadoop-client"
	nohup bin/ignite.sh &
	sleep 2
	ignitepid=`ps -ef | grep ignite | grep default-config.xml | awk '{print $2}'`
	if [ ! -z "$ignitepid" ]; then
		echo "Apache Ignite instance started successfully: $ignitepid"
	fi
}
####################################################################

## begin script main ##
#echo "begin checkHostNameAndSetClusterName"
checkHostNameAndSetClusterName
#echo "end checkHostNameAndSetClusterName"

#echo "begin validateUsernameAndPassword"
validateUsernameAndPassword
#echo "end validateUsernameAndPassword"

#echo "begin stopServiceViaRest"
stopServiceViaRest SPARK
#echo "end stopServiceViaRest"

#echo "begin stopServiceViaRest"
stopServiceViaRest MAPREDUCE2
#echo "end stopServiceViaRest"

#echo "begin stopServiceViaRest"
stopServiceViaRest YARN
#echo "end stopServiceViaRest"

#echo "begin stopServiceViaRest"
stopServiceViaRest HDFS
#echo "end stopServiceViaRest"

#echo "begin downloadAndUnzipApacheIgnite"
downloadAndUnzipApacheIgnite
#echo "end downloadAndUnzipApacheIgnite"

#echo "begin setupApacheIgniteService"
setupApacheIgniteService
#echo "end setupApacheIgniteService"

#echo "begin updateApacheIgniteConfig"
updateApacheIgniteConfig;
#echo "end updateApacheIgniteConfig"

#echo "beging updateAmbariConfigs"
updateAmbariConfigs;		
#echo "end updateAmbariConfigs"

#echo "begin updateApacheSparkConfig"
updateApacheSparkConfig;
#echo "end updateApacheSparkConfig"


#echo "begin startApacheIgnite"
startApacheIgnite
#echo "end startApacheIgnite"

#echo "start service rest"
startServiceViaRest HDFS
startServiceViaRest YARN
startServiceViaRest MAPREDUCE2
startServiceViaRest SPARK
#echo "completed"

# restart ambari agent and server
# service ambari-server restart
# service ambari-agent restart

echo "Apache Ignite installation completed"
