#!/usr/bin/env bash

# Copyright (C) Microsoft Corporation. All rights reserved.

# Arguments:
# $1 Blob link with SAS token query string 
# $2 Log path
# $3 Max log file size in MB

if [ "$#" -ne 3 ]; then
    >&2 echo "$@"
    >&2 echo "Please provide Azure Storage link, log path and max log size."
    exit 1
fi

blob_link=$1
log_path=$2
max_log_size=$3

if ! [ -e "$log_path" ]; then
    >&2 echo "There is no log path $log_path on this node"
    exit 0 
fi

tail -c $((1024*1024*max_log_size)) \
    "$log_path" > /tmp/shs_log_for_trouble_shooting.log

curl -T /tmp/shs_log_for_trouble_shooting.log \
    -X PUT \
    -H "x-ms-date: $(date -u)" \
    -H "x-ms-blob-type: BlockBlob" \
    "$blob_link"