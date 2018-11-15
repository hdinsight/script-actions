# Metastore Migration Script Action
Script action to preview or execute a migration of Azure Storage URIs within various tables of the Hive metastore. 

## Execution instructions

1. Create HDInsight cluster of any cluster version. The cluster does not need to be connected to the metastore of interest. You will however need to provide access credentials.
2. Run script action: `MigrateMetastore.sh` on this cluster. 

    Go to Azure portal > open cluster blade > open Script Actions tile > click Submit new and follow instructions. The script action is provided in this repository.
    
    When you are on the "Submit script action" blade, you will see "Bash script URI" field. You need to make sure that the `MigrateMetastore.sh` is stored in an Azure Storage Blob, and make the link is public.
    
    OR
    
    You can just `git clone` or download the `.sh` file onto your cluster directly, and execute via the command line. The script itself displays detailed usage instruction using the flag `--help`

Note: As of **11/14/2018** this script has *not* undergone formal testing. Use at own risk.
