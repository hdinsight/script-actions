# hdinsight  Install-Mono script-action
Script action to install Mono (https://www.mono-project.com) on a HDInsight cluster node.

This script can be used to install the default latest stable Mono version (4.8.1) or a choice of Mono version
passed in as an argument to the script.

The script will uninstall any existing versions of Mono, if different from the one specified.

## Installed Packages
The following packages are installed:
1. mono-complete
2. ca-certificates-mono

## Installation instructions

1. Create HDInsight cluster (>3.4 version)

2. If running a Storm cluster, Stop SCP.Net services from Ambari on all nodes.

3. Run script action: `install-mono.sh` on this cluster. 

    Go to Azure portal > open cluster blade > open Script Actions tile > click Submit new and follow instructions. The script action is provided in this repository.
    
    When you are on the "Submit script action" blade, you will see "Bash script URI" field. You need to make sure that the `install-mono.sh` is stored in an Azure Storage Blob, and make the link public.
    
    OR
    
    You can just add https://raw.githubusercontent.com/hdinsight/script-actions/master/install-mono/install-mono.sh to "Bash script URI".

    The script installs version 4.8.1 by default.
    If a custom Mono version is required, users can pass in the version number as the first argument to the script.
    Example: 
	install-mono.sh 4.8.0
	install-mono.sh 5.0.1

   The version passed in must be present on the following page, as an available version. Else the script will fail.
   https://download.mono-project.com/repo/debian/dists/wheezy/snapshots/
          
4. Mono is now installed and ready for use.
