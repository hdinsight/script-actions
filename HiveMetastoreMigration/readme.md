# Metastore Migration Shell Script
A shell script for bulk-editing Azure Storage URIs inside a Hive metastore

## Overview

The metastore migration script is a tool for migrating URIs from one or more sources to a fixed destination. This script eliminates the need to perform manual migrations (such as `update table set location` statements) against the metastore. 

The purpose of this script is to allow for bulk-editing of Azure Storage URIs inside Hive metastores. Actions including but not limited to those described below will necessitate the use of this script. This script is also a prerequisite of sorts to running data migrations between storage accounts. 

### Use Cases and Motivation

Some sample use cases for the script are as follows. These use cases give context as to why this script is important:

1. Suppose WASB secure transfer has recently been enabled or disabled for a given storage account. Therefore, URIs that Hive queries search for will begin with wasb:// or wasbs:// as needed. However, URIs in the Hive Metastore will not undergo an automatic schema update. This update must be done explicitly. In this case, the Migration script can help by doing the following:

"Move my WASB accounts Andy, Bob and Charles to WASBS"

```
> ./MigrateMetastore.sh 
--metastoreserver myserver
--metastoredatabase mydb
--metastoreuser myuser
--metastorepassword mypw
--typesrc wasb
--accountsrc Andy,Bob,Charles
--containersrc '\*'
--pathsrc '*'
--target SDS
--queryclient beeline
--typedest wasbs
```


2. In the not-too-distant future, new types of storage accounts will be available across different clouds. For example, ADLS gen1 is expected for release in the Azure US Govcloud next year. ADLs gen2 is also expected to GA across all clouds. The migration script can also help with tasks similar to the following:

"I am a customer in the Azure US Govcloud and now that ADLS gen1 is available, I need to make some changes. I have two WASB accounts was1 and was2, each with containers Echo, Charlie and Zebra. I want to move all the tables under these accounts and containers to my new ADLS account 'fastnewaccount', without changing container names":

```
> ./MigrateMetastore.sh 
--metastoreserver myserver
--metastoredatabase mydb
--metastoreuser myuser
--metastorepassword mypw
--typesrc wasb
--accountsrc was1,was2
--containersrc Echo,Carlie,Delta
--pathsrc '*'
--target SDS
--queryclient beeline
--environment usgov
--typedest adl
--accountdest fastnewaccount
```

"I am a customer in public cloud and now that ADLS gen2 is available, I need to make some changes. I have two ADL accounts adl1 and adl2, each with containers Echo, Charlie and Zebra. I want to move all the tables under these accounts and containers to my new ADLS account 'fastnewaccount', but I want them to be under the container 'migration'":

```
> ./MigrateMetastore.sh 
--metastoreserver myserver 
--metastoredatabase mydb 
--metastoreuser myuser 
--metastorepassword mypw 
--typesrc adl 
--adlaccounts adl1,adl2 
--containersrc Echo,Carlie,Delta 
--pathsrc '*' 
--target SDS 
--queryclient beeline 
--typedest abfs 
--accountdest fastnewaccount 
--containerdest migration
```

3. Another upcoming GA feature in Azure is HDInsight 4.0. Some defaults in HDI4 will be changing with respect to where Hive data is stored. These new defaults will result in changes to the Hive metastore. For example, the old default path for managed (internal) tables is /warehouse/tablespace/, but in HDI4 the default will be /warehouse/managed/hive. Knowing this, the metastore migration script can do the following:

"Move my tables stored in /warehouse/tablespace/ to /warehouse/managed/hive/ without squashing any subdirectories. However, make sure this is only done for containers named Test, Dev or Build":

```
> ./MigrateMetastore.sh 
--metastoreserver myserver 
--metastoredatabase mydb 
--metastoreuser myuser 
--metastorepassword mypw 
--typesrc '*' 
--adlaccounts '*' 
--accountsrc '*' 
--containersrc test,dev,build 
--pathsrc warehouse/tablespace 
--target SDS 
--queryclient beeline 
--pathdest warehouse/managed/hive
```

## Execution instructions

The migration script takes a fairly large number of arguments, some of which are optional. It is important to note that it is _not_ necessary to execute this script from an HDInsight cluster: so far this script can be executed using `beeline` or `sqlcmd`. The script requires the username, password, databasename and servername of the Hive metastore in which URIs will be migrated. If the metastore to be edited is an internal metastore, its information is available via Ambari. To access the password for an internal Hive metastore, run this command while `ssh`'d into the cluster headnode:

```
sudo java -cp "/var/lib/ambari-agent/cred/lib/*" org.apache.ambari.server.credentialapi.CredentialUtil get javax.jdo.option.connectionpassword -provider jceks://file/etc/hive/conf/conf.server/hive-site.jceks
```

1. Make sure one of the supported query commandline tools is installed. If the script is being run from an HDInsight cluster, use `beeline`. Alternatively, the `sqlcmd` tool can be installed here: https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility?view=sql-server-2017

2. Download and execute ./MigrateMetastore.sh without any arguments and read through the doc string for detailed instructions

3. Execute ./MigrateMetastore.sh with arguments in the style shown above (without linebreaks). The examples serve as a good start point. 
* **Note**: the script does not perform any action unles the flag `--liverun` is also used. If this flag is omitted, the migration result will instead be written to stdout (`--liverun` also writes to stdout, but it writes to your database too!)

## Testing instructions

The migration script is also accompanied by a suite of tests. These tests make sure the script behaves as expected, and they also provide further examples. The tests themselves have a few dependencies that must be accounted for:

1. The tests must run on Python **3.6**. run `python3 -V` to check python version. If the version is not 3.6, Python 3.6 can be installed at: https://www.python.org/downloads/release/python-360/.

2. The tests validate the migration script results by using `pyodbc`. Use `pip` to install pyodbc with `pip3 install pyodbc`. If Python 3.6 is not the default python installation, you may need to install pip for python 3.6 with `python3.6 get-pip.py` before installing odbc.

3. The tests require a SQL server and database instance, where a mockup metastore will be created from sample data. An actual Hive metastore is **not** to be used as the input for the tests.

4. With all dependencies and inputs ready, execute the tests as follows:
```
> Python3.6 MigrateMetastoreTests.py
./MigrateMetastoresh
--server myserver
--database mydatabase
--username user
--password mypw
--driver 'ODBC Driver 17 for SQL Server'
--testSuites All
--cleanup
```