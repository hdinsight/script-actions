#! /bin/bash
# Migrate locations captured in a Hive Metastore from A to B

InfoString=$(cat <<-EOF

`basename "$0"`: A script to edit the contents of a Hive metastore. Use this tool to bulk edit
the storage type, account name, container name, or directory of metastore entries. Multiple
attributes and multiple entries may be changed at one time.

This script is able to accomplish tasks such as:

"Move all wasb location to wasbs",
"Move the contents of storage accounts a, b, and c into container X of storage account Y"
"In containers a or b found in storage accounts x, y or z, move the path of all tables found
in /hive/warehouse to /warehouse/managed"

While this command must be executed against an HDInsight cluster, the cluster need not be
connected to the metastore in order to make changes to it.

Usage: sudo -E bash `basename "$0"`


Mandatory Arguments. Each one of these arguments must be provided:

-u|--metastoreuser                  The username credential used to access the Hive metastore. 

-p|--metastorepassword              Hive metastore password credential.

-d|--metastoredatabase              The name of the metastore database itself. 

-s|--metastoreserver                The name of the SQL server that contains the Hive metastore. 
                                    Provide only the name of the server: there is no need to 
                                    provide the complete SQL database endpoint.

-t|--target                         The target table of the metastore to be migrated. This
                                    flag decides which metastore table will be
                                    affected. Valid entries are: DBS, SDS, FUNC_RU, or
                                    SKEWED_COL_VALUE_LOC_MAP.

-ts|--typesrc                       A comma-separated list of the storage types correpsonding to 
                                    the entries to be migrated. Example: abfs,wasb. Valid entries 
                                    are: wasb, wasbs, abfs, abfss, adl. Use the character '%' 
                                    to select all storage types.

-cs|--containersrc                  A comma-separated list of the containers corresponding to the
                                    entries to be migrated. Example: c1,c2,c3. Use the character 
                                    '%' to select all containers. No more than 10 containers may
                                    be selected in one execution.

-as|--accountsrc                    A comma-separated list of the account names corresponding to 
                                    the entries to be migrated. Example: a1,a2,a3. Provide only 
                                    the names of the accounts: there is no need to provide the
                                    complete account endpoint. Use the character '%' to select 
                                    all account names. No more than 10 accounts may be selected
                                    in one execution.

-ps|--pathsrc                       A comma-separated list of the paths corresponding to the 
                                    entries to be migrated. Example: 
                                    warehouse,hive/tables,ext/tables/hive. Provide only the 
                                    names of the paths: there is no need to provide the '/' 
                                    character before and after the path. Use the character '%' 
                                    to select all paths. No more than 10 paths may be selected
                                    in one execution.


Optional Arguments. Any combination of these arguments is a valid input. 
Note: leaving all inputs blank will result in no change:

-td|--typedest                      A string corresponding to the storage type that all matches
                                    will be moved to. Valid entries are: wasb, wasbs, abfs, abfss,
                                    adl. If this value is left blank or omitted, no change will 
                                    be made to the storage type.

-ad|--accountdest                   A string corresponding to the account that all matches will
                                    be moved to. If this value is left blank or omitted, no 
                                    change will be made to the account. If --accountdest is set, 
                                    --typedest must also be set.

-cd|--containerdest                 A string corresponding to the container that all matches will
                                    be moved to. If this value is left blank or omitted, no change 
                                    will be made to the container.

-pd|--pathdest                      A string corresponding to the path that all matches will be 
                                    moved to. If this value is left blank or omitted, no change
                                    will be made to the path.

-l|--liverun                        This argument is a flag, not a paremeter. If --liverun is used,
                                    the flag is not to be accompanied by a value. --liverun executes
                                    this script 'live', meaning that the specified metastore will be
                                    written to as specified by the other parameters passed. Omit this
                                    flag to launch a dry run of the script (default).

-h|--help                           Display this message.

The 'source' flags work together such that a regex-like table location is built. Example: 
[abfs,wasb]://[c1,c2,c3]@[a1,a2,a3]/[p1,p2,p3]/. Every location that matches the pattern 
formed by the source flags will be converted into the location constructed by the destination 
flags. The destination flags specify which location attributes (type, account, container, path) 
to change, and what to change those attributes to. This script does NOT require Ambari credentials.

Note: It is strongly recommended to redirect stdout to a file when executing this script, as there
is a large amount of text that will be logged (especially in cases where the target metastore is at-scale)

EOF
)

EXIT_BEELINE_FAIL=25
EXIT_BAD_ARGS=50
EXIT_NO_CHANGE=75
EXIT_DRY_RUN=100

MAX_ARG_COUNT=10

WhereClauseString='*://*@*/*/%' # Type://Container@Account/Directory/Table
UpdateTemplate="update LocationUpdate set * = ('#'); "
UpdateCommands=''

declare -A EndpointMap
EndpointMap[wasb]=blob.core.windows
EndpointMap[wasbs]=blob.core.windows
EndpointMap[abfs]=dfs.core.windows
EndpointMap[abfss]=dfs.core.windows
EndpointMap[adl]=azuredatalakestore

SDSColumns=(SDS location sd_id)
DBSColumns=(DBS db_location_uri db_id)
FUNCRUColumns=(FUNC_RU resource_uri func_id)
SKEWEDCOLVALUELOCMAPColumns=(SKEWED_COL_VALUE_LOC_MAP location sd_id)

POSITIONAL=()
TargetAttrs=()
WhereClauseStringsTypeSet=()
WhereClauseStringsTypeContainerSet=()
WhereClauseStringsTypeContainerAccountSet=()
WhereClauseStringsTypeContainerAccountPathSet=()

launchBeelineCommand() 
{
    beelineCmd="beeline --outputformat=csv2 -u '$1' -n '$2' -p '$3' -e \"${4}\""
    eval "${beelineCmd}"
    if [ ! $? -eq 0 ]
    then
        code=$?
        echo "Beeline command failed with exit code ${?}"
        echo "Command executed was ${beelineCmd}"
        echo "Closing."
        exit ${EXIT_BEELINE_FAIL}
    fi
}

usage() 
{
    echo "$InfoString"
    echo "$1"
    exit ${EXIT_BAD_ARGS}
}

while [ $# -gt 0 ]
do
    key="$1"

    case $key in
        -u|--metastoreuser)
        Username="$2"
        shift
        shift
        ;;
        -p|--metastorepassword)
        Password="$2"
        shift
        shift
        ;;
        -s|--metastoreserver)
        Server="$2"
        shift
        shift
        ;;
        -d|--metastoredatabase)
        Database="$2"
        shift
        shift
        ;;
        -t|--target)
        Target="$2"
        shift
        shift
        ;;
        # These have to be parsed for the separating comma
        -ts|--typesrc)
        TypeSrc="$2"
        shift
        shift
        ;;
        -as|--accountsrc)
        AccountSrc="$2"
        shift
        shift
        ;;
        -cs|--containersrc)
        ContainerSrc="$2"
        shift
        shift
        ;;
        -ps|--pathsrc)
        RootpathSrc="$2"
        shift
        shift
        ;;
        -td|--typedest)
        TypeDest="$2"
        shift
        shift
        ;;
        -ad|--accountdest)
        AccountDest="$2"
        shift
        shift
        ;;
        -cd|--containerdest)
        ContainerDest="$2"
        shift
        shift
        ;;
        -pd|--pathdest)
        RootpathDest="$2"
        shift
        shift
        ;;
        -l|--liverun)
        Liverun=true
        shift
        ;;
        -h|--help)
        usage "Help flag entered. Closing."
        ;;

        *)
        usage "Unsupported flag entered. Closing."
        ;;
esac
done
set -- "${POSITIONAL[@]}"
# Now we have all the inputs. 

# Check Mandatory Flags Present
if [ -z "${Username}" ] || [ -z "${Password}" ] || [ -z "${Server}" ] || [ -z "${Database}" ] || [ -z "${Target}" ] || \
[ -z "${TypeSrc}" ] || [ -z "${AccountSrc}" ] || [ -z "${ContainerSrc}" ] || [ -z "${RootpathSrc}" ] || \
[ -z "${TypeDest}" -a ! -z "${AccountDest}" ]
then
    usage "At least one mandatory flag missing. Closing."
fi

# If all dests are empty, exit
if [ -z "${TypeDest}" -a -z "${AccountDest}" -a -z "${ContainerDest}" -a -z "${RootpathDest}" ]
then
    echo "No destination attributes set. Nothing to do."
    exit ${EXIT_NO_CHANGE}
fi

# Parse Inputs

IFS=',' read -r -a TypeSrcVals <<< "$TypeSrc"
IFS=',' read -r -a AccountSrcVals <<< "$AccountSrc"
IFS=',' read -r -a ContainerSrcVals <<< "$ContainerSrc"
IFS=',' read -r -a RootpathSrcVals <<< "$RootpathSrc"

# Make sure that there are not too many input values
if [ "${#AccountSrcVals[@]}" -gt "$MAX_ARG_COUNT" ] || [ "${#ContainerSrcVals[@]}" -gt "$MAX_ARG_COUNT" ] || [ "${#RootpathSrcVals[@]}" -gt "$MAX_ARG_COUNT" ]
then
    usage "Too many entries specified. Closing."
fi 

# Make sure types and target are valid
for item in "${TypeSrcVals[@]}"
do
    if [ -z "${EndpointMap[${item}]}" -a ! "${item}" = "%" ]
    then
        usage "Invalid account type specified. Closing."
    fi
done

if [ "${Target}" = "DBS" ]
then
    TargetAttrs=("${DBSColumns[@]}")
elif [ "${Target}" = "SDS" ]
then
    TargetAttrs=("${SDSColumns[@]}")
elif [ "${Target}" = "FUNC_RU" ]
then
    TargetAttrs=("${FUNCRUColumns[@]}")
elif [ "${Target}" = "SKEWED_COL_VALUE_LOC_MAP" ]
then
    TargetAttrs=("${SKEWEDCOLVALUELOCMAPColumns[@]}")
else
    usage "Invalid target specified. Closing."
fi

if [ ! -z "${TypeDest}" ]
then
    # %:// -> dest_type://
    UpdateCommands="$UpdateCommands $(sed "s/*/SCHEMATYPE/g; s/#/${TypeDest}/g"  <<< $UpdateTemplate)"
    # .%.net -> .dest_type.net
    UpdateCommands="$UpdateCommands $(sed "s/*/ENPT/g; s/#/${EndpointMap[${TypeDest}]}/g;"  <<< $UpdateTemplate)"
fi

for item in "${TypeSrcVals[@]}"
do
    WhereClauseStringsTypeSet+=("$(sed "s/*/$item/1" <<< $WhereClauseString)")
done

if [ ! -z "${ContainerDest}" ]
then
    # ://%@ -> ://dest_container@
    UpdateCommands="$UpdateCommands $(sed "s/*/CONTAINER/g; s/#/${ContainerDest}/g"  <<< $UpdateTemplate)"
fi

for TypeSetTemplate in "${WhereClauseStringsTypeSet[@]}"
do
    for item in "${ContainerSrcVals[@]}"
    do
        WhereClauseStringsTypeContainerSet+=("$(sed "s/*/$item/1" <<< $TypeSetTemplate)")
    done
done

if [ ! -z "${AccountDest}" ]
then
    # @%. -> @dest_account.
    UpdateCommands="$UpdateCommands $(sed "s/*/ACCOUNT/g; s/#/${AccountDest}/g"  <<< $UpdateTemplate)"
fi

for TypeContainerSetTemplate in "${WhereClauseStringsTypeContainerSet[@]}"
do
    for item in "${AccountSrcVals[@]}"
    do
        WhereClauseStringsTypeContainerAccountSet+=("$(sed "s/*/$item\.%\.net/1" <<< $TypeContainerSetTemplate)")
    done
done

if [ ! -z "${RootpathDest}" ]
then
    # Path update is a little different: Replace longest left-hand subsequence. Root path migration that preserves hierarchy
    if [ ! "${RootpathSrcVals[0]}" = "%" ]
    then
        SrcPathValuesStr=$( printf "('/%s/'), " "${RootpathSrcVals[@]}" | sed 's/,.$//' )
        PathReplaceCommand=$(cat <<-EOF
        drop table if exists srcpaths;
        create table srcpaths (srcpathname nvarchar(1024));
        insert into srcpaths values $SrcPathValuesStr;

        update LocationUpdate set ROOTPATH = Replace
        (
            ROOTPATH,
            (
                select ISNULL( (select top 1 srcpathname from srcpaths where ROOTPATH like (srcpathname + '%') order by srcpathname desc), '')
            ),
            '/$RootpathDest/'
        );
       drop table srcpaths;

EOF
)
        UpdateCommands="$UpdateCommands $PathReplaceCommand"
    else
        UpdateCommands="$UpdateCommands $(sed "s/*/ROOTPATH/g; s/#/\/${RootpathDest}\//g"  <<< $UpdateTemplate)"
    fi
fi

for TypeContainerAccountSetTemplate in "${WhereClauseStringsTypeContainerAccountSet[@]}"
do
    for item in "${RootpathSrcVals[@]}"
    do
        WhereClauseStringsTypeContainerAccountPathSet+=("$(sed "s:*:$item:1" <<< $TypeContainerAccountSetTemplate)")
    done
done

WhereClauseString=$(printf " OR ${TargetAttrs[1]} like ('%s')" "${WhereClauseStringsTypeContainerAccountPathSet[@]}" | cut -c 5-)

# Launch
# 1. Create workbench table for migration
# 2. Insert items into workbench table that match expanded OR clause. The insertions are parsed for their attributes and placed into the appropriate column.
# 3. Update column entries in the workbench table. 
# 3b. The path entry is updated by replacing the longest (from the root) source path found in the entry. That way the hierarchy of subfolders is preserved.
# 4. Update the original table (sds, dbs, etc) by putting the location string back together and matching the ID column

MigrationSetupString=$(cat <<-EOF

drop table if exists LocationUpdate;
create table LocationUpdate 
(
    SCHEMATYPE nvarchar(1024),
    CONTAINER nvarchar(1024),
    ACCOUNT nvarchar(1024),
    ENPT nvarchar(1024),
    ROOTPATH nvarchar(1024),
    FK_ID bigint foreign key references ${TargetAttrs[0]} (${TargetAttrs[2]})
);

insert into LocationUpdate
(
    FK_ID,
    SCHEMATYPE,
    CONTAINER,
    ACCOUNT,
    ENPT,
    ROOTPATH
)
select ${TargetAttrs[2]},
substring
(
    ${TargetAttrs[1]},
    0, 
    CHARINDEX('://', ${TargetAttrs[1]})
),
substring
(
    ${TargetAttrs[1]},
    CHARINDEX('://', ${TargetAttrs[1]}) + len('://'),
    CHARINDEX('@', ${TargetAttrs[1]}) - ( CHARINDEX('://', ${TargetAttrs[1]}) + len('://') )
),
substring
(
    ${TargetAttrs[1]},
    CHARINDEX('@', ${TargetAttrs[1]}) + len('@'),
    CHARINDEX('.', ${TargetAttrs[1]}) - ( CHARINDEX('@', ${TargetAttrs[1]}) + len('@') )
), 
substring
(
    ${TargetAttrs[1]},
    CHARINDEX('.', ${TargetAttrs[1]}) + len('.'),
    CHARINDEX('.net', ${TargetAttrs[1]}) - ( CHARINDEX('.', ${TargetAttrs[1]}) + len('.') )
),
substring
(
    ${TargetAttrs[1]},
    CHARINDEX('.net', ${TargetAttrs[1]}) + len('.net'),
    (len(${TargetAttrs[1]}) - CHARINDEX('/', reverse(${TargetAttrs[1]})) + 1) - (CHARINDEX('.net', ${TargetAttrs[1]}) + len('.net') - 1)
)

from ${TargetAttrs[0]} WHERE
$WhereClauseString;

$UpdateCommands

EOF
)

DisplayMigrationImpactString=$(cat <<-EOF

select LocationUpdate.FK_ID as ID, ${TargetAttrs[1]} as oldLocation, 
(
    SCHEMATYPE + 
    '://' + 
    CONTAINER + 
    '@' + 
    ACCOUNT + 
    '.' + 
    ENPT + 
    '.net' + 
    ROOTPATH + 
    right( ${TargetAttrs[1]}, charindex('/', reverse(${TargetAttrs[1]}) ) -1)
) as newLocation
from ${TargetAttrs[0]}, LocationUpdate
where ${TargetAttrs[2]} = LocationUpdate.FK_ID;

EOF
)

ExecuteFinalMigrationString=$(cat <<-EOF

update ${TargetAttrs[0]} set ${TargetAttrs[1]} = 
(
    SCHEMATYPE + 
    '://' + 
    CONTAINER + 
    '@' + 
    ACCOUNT + 
    '.' + 
    ENPT + 
    '.net' +
    ROOTPATH + 
    right( ${TargetAttrs[1]}, charindex('/', reverse(${TargetAttrs[1]}) ) -1)
)
from ${TargetAttrs[0]}, LocationUpdate
where ${TargetAttrs[2]} = LocationUpdate.FK_ID;

drop table LocationUpdate;

EOF
)

echo Username = "${Username}"
echo Password = "${Password}"
echo Server = "${Server}"
echo Database = "${Database}"
echo Target = "${Target}"
echo Running Live\? "${Liverun}"

echo Source Type\(s\) = "${TypeSrc}"
echo Account\(s\) = "${AccountSrc}"
echo Container\(s\) = "${ContainerSrc}"
echo Source Rootpath\(s\) = "${RootpathSrc}"

echo Final Type \(if any\) = "${TypeDest}"
echo Final Account \(if any\) = "${AccountDest}"
echo Final Container \(if any\) = "${ContainerDest}"
echo Final Rootpath \(if any\) = "${RootpathDest}"

echo "Generated Migration SQL Script:"
echo "$MigrationSetupString"
echo "$DisplayMigrationImpactString"
echo "$ExecuteFinalMigrationString"

echo "Launching Migration SQL Commands:"

ConnectionString="jdbc:sqlserver://${Server}.database.windows.net;database=${Database}"

echo "Preparing migration contents..."
launchBeelineCommand $ConnectionString $Username $Password "${MigrationSetupString}"

echo "Affected entries of ${TargetAttrs[0]} will have the following values pre and post-migration..."
launchBeelineCommand $ConnectionString $Username $Password "${DisplayMigrationImpactString}"

if [ "$Liverun" = true ]
then
    echo "Script execution type set to LIVE. Writing migration results to table: ${TargetAttrs[0]}..."
    launchBeelineCommand $ConnectionString $Username $Password "${ExecuteFinalMigrationString}"
else
    echo "LIVE flag not set. Closing"
    exit ${EXIT_DRY_RUN}
fi

echo "Migration script complete!"
