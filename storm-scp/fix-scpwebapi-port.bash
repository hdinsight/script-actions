#!/bin/bash
cp /usr/hdp/current/storm-client/scpwebapi/resources/scpwebapi-humboldt.yaml /usr/hdp/current/storm-client/resources/scpwebapi-humboldt.yaml.bak
sed -e "s/scp.webapi.ui.port: 80/scp.webapi.ui.port: 8888/g" /usr/hdp/current/storm-client/resources/scpwebapi-humboldt.yaml >/usr/hdp/current/storm-client/resources/scpwebapi-humboldt.yaml