# Metastore Migration Shell Script
Shell script to preview or execute a migration of Azure Storage URIs within various tables of the Hive metastore. 

## Execution instructions

1. Create HDInsight cluster of any cluster version. The cluster does not need to be connected to the metastore of interest. You will however need to provide access credentials.

2. Copy this code with `git clone` or download the `.sh` file onto your cluster directly, and execute via the command line. The script itself displays detailed usage instruction using the flag `--help`

Note: this script has *not* yet undergone formal testing. Use at own risk.
