#!/bin/bash

sudo ln -s /usr/hdp/current/hadoop-client/*.jar /usr/hdp/current/storm-client/extlib/
sudo ln -s /usr/hdp/current/hadoop-client/lib/*.jar /usr/hdp/current/storm-client/extlib/
sudo ln -s /usr/hdp/current/hadoop-hdfs-client/*.jar /usr/hdp/current/storm-client/extlib/
sudo ln -s /usr/hdp/current/hadoop-hdfs-client/lib/*.jar /usr/hdp/current/storm-client/extlib/