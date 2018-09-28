
### Instructions to install Data Analytics Studio on HDI  4.0

#### Prerequisites
1. A precreated HDI 4 cluster containing hive component.
2. Make sure the tez configs(tez-site) have been modified to include ```tez.history.logging.proto-base-dir=/warehouse/tablespace/external/hive/sys.db```
Restart Tez after modifying the configuration.
3. Make sure the hive configs(hive-site) have been modified to include 
```hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook``` ```hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook``` ```hive.exec.pre.hooks=org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook```
Restart Hive after modifying the configurations.

#### Installation Instructions
Execute [install-data-analytics-studio.sh](install-data-analytics-studio.sh) as [custom script action](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux) only on the head nodes.

After the script succeeds, head to ambari view and refresh. On the left panel observe a new service: ```Data Analytics Studio```.
Use the Quick Links to navigate to the UI, or append /das/ at the end of the cluster name, e.g https://clustername.azurehdinsight.net/das/
