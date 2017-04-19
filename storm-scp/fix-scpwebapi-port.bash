#!/bin/bash
cp /usr/hdp/current/storm-client/scpwebapi/resources/scpwebapi-humboldt.yaml /usr/hdp/current/storm-client/scpwebapi/resources/scpwebapi-humboldt.yaml.$(date +"%s")
sed -e "s/scp.webapi.ui.port: 80/scp.webapi.ui.port: 8888/g" /usr/hdp/current/storm-client/scpwebapi/resources/scpwebapi-humboldt.yaml > /tmp/scpwebapi-humboldt.yaml.new
if [ $? == 0 ]; then
    cp -f /tmp/scpwebapi-humboldt.yaml.new /usr/hdp/current/storm-client/scpwebapi/resources/scpwebapi-humboldt.yaml
fi