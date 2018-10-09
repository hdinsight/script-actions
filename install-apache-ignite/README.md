# Using install-apache-ignite shellscript
This script installs [Apache Ignite](www.ignite.apache.org) on an HDInsight cluster, regardless how many nodes in your HDInsight cluster.

The cluster is designed to run as a [ScriptAction](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-script-actions).

Running the script as a ScriptAction or is simple, all you need to do is passing its URL to the script action, no need to submit any arguments. 

Note: **THE SCRIPT MUST BE RUN ON HEAD AND WORKER NODES**

# What does it do in background?
The script retrieves the following information:

1. The Ambari Admin username 
2. The Ambari Admin password
  - The Ambari Admin name & password are needed to automatically push Ignite's configuration into HDFS **`core-site.xml`** via Ambari's **`config.sh`** command.
3. The wasb storage URL of which Apache Ignite will interface with 
  - The URL should be as follows, it is found in **`HDFS core-site`** configuration: 
  ```
  wasb://container@account.blob.core.windows.net
  ```
4. The FQDN/IP addresses of your namenode where Ambari server is running
  - This could be the IP address of the headnode0 or headnode1 
5. The Ambari cluster name
  - This is the name you see on the top left after you login to Ambari web console
6. The FQDN/IP addresses of **ALL** your headnodes & worker nodes **separated by SPACE**
  - _why is this needed?_ The script configures the Apache Ignite **`default-config.xml`** and enables cluster discovery
  - _What is cluster discovery?_ Cluster discovery enables all of the Ignite processes running on your nodes to sync with each other

## How to test if Apache Ignite Works?
1. check the Ignite process is running on your nodes, for example using:
  ```
  ps -aef | grep default-config.xml
  ```
  1. This should list the Ignite process taht's using the updated default configurstion. 
2. Check the Ambari HDFS configuration by searching for `igfs`
  1. This should list **2 properties added to _HDFS core-site.xml_** 
3. Using HDFS commands:
  1. Browse your blob storage using prefix:
    ```
    hdfs dfs -ls wasb://container@account.blob.core.windows.net/HdiNotebooks
    ```
  2. Browse your blob storage using Ignite's _igfs_ prefix:
    ```
    hdfs dfs -ls igfs:///HdiNotebooks
    ```
  The bove commands should return the same results
4. Check the contents of ```/hadoop/ignite/apache-ignite-xxx/config/default-config.xml```
  1. check that your ```wasb://container@account.blob.core.windows.net``` is added to the file
  2. check that your headnodes & worker-nodes FQDN/IP addresses are added to the Spi Discovery section. 
5. Using Spark-Shell, open `spark-shell` and run an example as follows:
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
