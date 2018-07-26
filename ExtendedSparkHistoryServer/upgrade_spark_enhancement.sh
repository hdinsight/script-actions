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
            mv "$FILE" "$back_up_path"
            echo "Copy the hotfix jar file from $tmp_jar_path to $FILE"
            cp "$tmp_jar_path" "$FILE"

            "Hotfix done."
            break
        done
    else    
        >&2 echo "Download jar file failed."
        exit 1
    fi
}

jars_folder="/usr/hdp/current/spark2-client/jars"
jar_path=$1

if ls ${jars_folder}/spark-enhancement*.jar 1>/dev/null 2>&1; then
    install_jar "$jars_folder" "$jar_path"
else
    >&2 echo "There is no target jar on this node. Exit with no action."
    exit 0
fi
