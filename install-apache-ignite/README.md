# azure-scripts
This repo contains Azure management scripts for certain purposes.

# Using install-apache-ignite.sh
This script installs [Apache Ignite](www.ignite.apache.org) on an HDInsight cluster, regardless how many your HDInsight cluster has.

The cluster is designed to run as a [ScriptAction](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-script-actions) **AFTER provisioning the cluster**; as it needs information about the name & worker nodes.

## Example of using the _install-apache-ignite.sh_ script
The following snippet shows how to pass the arguments for a cluster with a name: myHDICluster. The cluster consists of 2 Head nodes and 2 Worker nodes.
```bash
./install-apache-ignite.sh wasb://mycontainer@myblob.blob.core.windows.net admin AmbariPwd_01 100.8.17.254 myHDICluster adminssh 10.0.0.1 10.0.0.2 10.0.0.4 10.0.0.9
```
Running the script as a ScriptAction or manually is simple, all you need to do is submit the correct arguments separated by a space

1. The wasb storage URL of which you want Apache Ignite to interface with 
  - The URL should be as follows, you can find it in your **`HDFS core-site`** configuration: 
  ```
  wasb://container@account.blob.core.windows.net
  ```
2. The Ambari Admin username 
3. The Ambari Admin password
  - The Ambari Admin name & password are needed to automatically push Ignite's configuration into HDFS **`core-site.xml`** via Ambari's **`config.sh`** command.
4. The IP address of your namenode where Ambari server is running
  - This could be the IP address of the headnode0 or headnode1
  - I haven't tested it with the node's _FQDN_, but you can try; the worst case scenario is to push the correct configuration again. 
5. The Ambari cluster name
  - This is the name you see on the top left after you login to Ambari web console
6. The SSh username of your account
  - _Why is this needed?_ because we need to give a read/write/execute permission for you on **`&IGNITE_HOME/work`** directory; otherwise the Ignite process will fail during initiation.
7. The IP addresses of **ALL** your headnodes & worker nodes **separated by SPACE**
  - _why is this needed?_ The script configures the Apache Ignite **`default-config.xml`** and enables cluster discovery
  - _What is cluster discovery?_ Cluster discovery enables all of the Ignite processes running on your nodes to sync with each other

## How to test if Apache Ignite Works?
1. check the Ignite process is running on your nodes, for example using:
  ```
  ps -aef | grep default-config.xml
  ```
2. Check the Ambari HDFS configuration by searching for `igfs`
3. Using HDFS commands:
  1. Browse your blob storage:
    ```
    hdfs dfs -ls wasb://container@account.blob.core.windows.net/HdiNotebooks
    ```
  2. Browse Ignite:
    ```
    hdfs dfs -ls igfs:///HdiNotebooks
    ```
  The bove commands should return the same results
4. Using Spark-Shell, open `spark-shell` and run an example as follows:
  ```scala
  val textdata = sc.textFile("wasb://container@account.blob.core.windows.net/Folder/textFile.ext")
  val count = textdata.count
  val first = textdata.first
  val dataWithoutHeader = textdata.filter(line => line != first)
  val datacount = dataWithoutHeader.count
  
  val igtextdata = sc.textFile("igfs:///Folder/textFile.ext")
  val igcount = igtextdata.count
  val igfirst = igtextdata.first
  val igdataWithoutHeader = igtextdata.filter(line => line != first)
  val igdatacount = igdataWithoutHeader.count
  ```
If the above expirements above work, then **Congratulations**, Apache Ignite is acting as a secondary in-memory file system for your blob. You can start testing its performance against pulling directly from your blob storage. 
