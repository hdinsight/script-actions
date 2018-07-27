#!/usr/bin/env bash

# Copyright (C) Microsoft Corporation. All rights reserved.

# Arguments:
# $1 Enhancement jar path

if [ "$#" -ne 1 ]; then
    >&2 echo "Please provide the upgrade jar path."
    exit 1
fi

install_jar() {
    tmp_jar_path="/tmp/spark-enhancement-hotfix-$( date +%s )"

    if wget -O "$tmp_jar_path" "$2"; then
        for FILE in "$1"/spark-enhancement*.jar
        do
            back_up_path="$FILE.original.$( date +%s )"
            echo "Back up $FILE to $back_up_path"
            if mv "$FILE" "$back_up_path"; then
                echo "Copy the hotfix jar file from $tmp_jar_path to $FILE"
                cp "$tmp_jar_path" "$FILE"
            else
                >&2 echo "Back up $FILE failed"
                exit 1
            fi

            echo "Copy completed. Going to restart Spark service."
            break
        done
    else    
        >&2 echo "Download jar file failed."
        exit 1
    fi
}

restart_spark_service() {
    USERID=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)
    echo "USERID=$USERID"

    PASSWD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python)

    CLUSTERNAME=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)
    echo "Cluster Name=$CLUSTERNAME"

    #stop spark service, retry 3 times if fails
    n=0
    STATUSCODE=400
    until [ $STATUSCODE -le 202 ] || [ $n -gt 3 ]
    do
        STATUSCODE=$(sudo curl -u $USERID:$PASSWD -H "X-Requested-By: ambari" -i -X PUT -d '{"RequestInfo": {"context": "Stop Spark2"}, "ServiceInfo": {"state": "INSTALLED"}}' --silent --write-out %{http_code} --output /dev/restartshslog.txt https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/SPARK2)
        if test $STATUSCODE -le 202; then
            break
        else
            n=$[$n+1]
        fi
        sleep 5
    done
    
    if test $STATUSCODE -gt 202; then
        echo "Stopping Spark service failed for $CLUSTERNAME with $(cat /dev/restartshslog.txt)" | logger
        exit 1
    fi
    
    #start spark service, retry 3 times if fails
    n=0
    STATUSCODE=400
    until [ $STATUSCODE -le 202 ] || [ $n -gt 3 ]
    do
        STATUSCODE=$(sudo curl -u $USERID:$PASSWD -H "X-Requested-By: ambari" -i -X PUT -d '{"RequestInfo": {"context": "Start Spark2"}, "ServiceInfo": {"state": "STARTED"}}' --silent --write-out %{http_code} --output /dev/restartshslog.txt https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/SPARK2)
        if test $STATUSCODE -le 202; then
            break
        else
            n=$[$n+1]
        fi
        sleep 5
    done
    
    if test $STATUSCODE -gt 202; then
        echo "Restarting Spark service failed for $CLUSTERNAME with $STATUSCODE - $(cat /dev/restartshslog.txt)" | logger
        exit 1
    fi
}

jars_folder="/usr/hdp/current/spark2-client/jars"
jar_path=$1

if ls ${jars_folder}/spark-enhancement*.jar 1>/dev/null 2>&1; then
    install_jar "$jars_folder" "$jar_path"
    restart_spark_service
else
    >&2 echo "There is no target jar on this node. Exit with no action."
    exit 0
fi
