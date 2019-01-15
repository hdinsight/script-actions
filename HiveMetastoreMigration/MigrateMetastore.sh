#! /bin/bash -eux
# Migrate locations captured in a Hive Metastore from A to B

###############################################################################################################################################################################
###############################################################################################################################################################################
# Functions and globals.
###############################################################################################################################################################################
###############################################################################################################################################################################

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

-t|--target                         A comma-separated list of target metastore tables to be
                                    migrated. This flag decides which metastore table will be
                                    affected. Valid entries are: SKEWED_COL_VALUE_LOC_MAP, DBS,
                                    SDS, FUNC_RU, or ALL to select all four tables. To move table
                                    locations, use SDS. To move database locations, use DBS. If
                                    you are not sure what you are trying to do, it is not
                                    recommended to use this script.

-q|--queryclient                    The command this script will use to execute SQL queries.
                                    Currently supported clients are beeline and sqlcmd.

-ts|--typesrc                       A comma-separated list of the storage types correpsonding to
                                    the entries to be migrated. Example: abfs,wasb. Valid entries
                                    are: wasb, wasbs, abfs, abfss, adl. Use the character '*' with
                                    quotes to select all storage types. IF --typesrc includes '*' or 
                                    adl, then --adlaccounts must also be set.

-cs|--containersrc                  A comma-separated list of the containers corresponding to the
                                    entries to be migrated. Example: c1,c2,c3. Use the character '*' 
                                    with quotes to select all containers. No more than 10 containers 
                                    may be manually selected in one execution.

-as|--accountsrc                    A comma-separated list of the account names corresponding to
                                    the entries to be migrated. Example: a1,a2,a3. Provide only
                                    the names of the accounts: there is no need to provide the
                                    complete account endpoint. Use the character '*' with quotes 
                                    to select all account names. No more than 10 accounts may be 
                                    manually selected in one execution. If adl is the only entry 
                                    in --typesrc, this flag can be skipped.

-ps|--pathsrc                       A comma-separated list of the paths corresponding to the
                                    entries to be migrated. Example:
                                    warehouse,hive/tables,ext/tables/hive. Provide only the
                                    names of the paths: there is no need to provide the '/'
                                    character before and after the path. Use the character '*'
                                    with quotes to select all paths. No more than 10 paths may 
                                    be manually selected in one execution.


Optional Arguments. Any combination of these arguments is a valid input.
Note: leaving all inputs blank will result in no change:

-adls|--adlaccounts                 A comma-separated list of the Azure Data Lake Storage Gen 1
                                    account names corresponding to the entries to be migrated.
                                    This flag operates identically to --accountsrc. ADL accounts
                                    must be specified manually since ADL accounts can have the same
                                    name as other storage accounts. Use the character '*' with quotes 
                                    to select all ADL account names. No more than 10 ADL accounts may be
                                    manually selected in one execution.

-e|--environment                    The name of the Azure Environment this script is to be executed on.
                                    Options for this flag are China, Germany, USGov and Default. If this
                                    flag is omitted, the default option will be used. Note that Azure
                                    Data Lake as a source or destination type is only supported when using
                                    the Default Azure environment. If --typesrc is set to '*' and --environment
                                    is not default or blank, ADL accounts will be ignored.

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

# Exit codes for easier testing
EXIT_SQL_FAIL=25
EXIT_BAD_ARGS=50
EXIT_NO_CHANGE=75
EXIT_DRY_RUN=100

# Arg Batch Limit (Does not apply to wildcards. This is only to upper-bound the complexity of generating the WHERE clause later)
MAX_ARG_COUNT=10

# Template to generate the expanded WHERE clause
WhereClauseString='*://*@*/*/%' # Type://Container@Account/Directory/Table
# Template to execute replacements against URI attributes stored as columns in a temp table
UpdateTemplate="update LocationUpdate set * = ('#'); "
# String to sequence of filled-in update templates
UpdateCommands=''

# Map for Endpoints and Environments so different storage accounts and different azure clouds are supported
# ADL v1 is a special case: Only supported in public cloud, and its endpoint is '.azuredatalakestore.net'. There is no '.core.'
# See https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-connectivity-from-vnets

declare -A EndpointMap
EndpointMap[wasb]=blob
EndpointMap[wasbs]=blob
EndpointMap[abfs]=dfs
EndpointMap[abfss]=dfs
EndpointMap[adl]=azuredatalakestore

# Storage domain endpoints, from https://docs.microsoft.com/en-us/azure/storage/common/storage-powershell-independent-clouds
declare -A EnvironmentMap
EnvironmentMap[china]=cn
EnvironmentMap[default]=net
EnvironmentMap[germany]=de
EnvironmentMap[usgov]=net
EnvironmentMap[adl]=net

# Storage domain strings excluding final domain, from same doc
declare -A StorageDomainMap
StorageDomainMap[china]=.core.chinacloudapi
StorageDomainMap[default]=.core.windows
StorageDomainMap[germany]=.core.cloudapi
StorageDomainMap[usgov]=.core.usgovcloudapi
# ADL has no domain map entry because the domain for ADL is blank: azuredatalakestore.net

# These triples store the relevant metastore table and the columns of interest:
# (table_name, uri_column, primary_key)
SDSColumns=(SDS location sd_id)
DBSColumns=(DBS db_location_uri db_id)
FUNCRUColumns=(FUNC_RU resource_uri func_id)
SKEWEDCOLVALUELOCMAPColumns=(SKEWED_COL_VALUE_LOC_MAP location sd_id)

# Store position arguments to be fed into parameters
POSITIONAL=()
# This will be one of the triples specified above depending on the target set (SDS, DBS, FUNC_RU, SKEWED_COL_VALUE_LOC_MAP)
TargetAttrs=()
# Expanded WHERE clause string with the Type attribute filled in
WhereClauseStringsTypeSet=()
# Ditto for Container also
WhereClauseStringsTypeContainerSet=()
# Ditto for Account also
WhereClauseStringsTypeContainerAccountSet=()
# Ditto for Path also. This will include every WHERE clause URIs must match. example: WHERE location like X, Y, Z, ...
WhereClauseStringsTypeContainerAccountPathSet=()

# Supported SQL Clients and the commands to execute migration commands with them
# The templates follow the same format: <cmd> <output options> <server> <db> <user> <pass> <query file>
declare -A sqlClientMap
sqlClientMap[beeline]="beeline --outputformat=csv2 -u 'jdbc:sqlserver://%s.database.windows.net;database=%s' -n '%s' -p '%s' -f '%s'"
sqlClientMap[sqlcmd]="sqlcmd -s\",\" -W -S %s.database.windows.net -d %s -U %s -P %s -i %s"

launchSqlCommand()
{
    scriptFile=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
    scriptFile="$scriptFile-migrationcommand"
    touch ${scriptFile}
    echo ${6} >> ${scriptFile}

    command=$(printf "${sqlClientMap["$1"]}" "$2" "$3" "$4" "$5" "${scriptFile}")
    echo ${command}
    eval "${command}"
    if [ ! $? -eq 0 ]
    then
        echo "SQL command failed with exit code ${?}"
        echo "Command executed was ${command}"
        echo "Closing."
        exit ${EXIT_SQL_FAIL}
    fi
    rm -f ${scriptFile}
}

usage()
{
    echo "$InfoString"
    echo "Unexpected Argument: $1"
    exit ${EXIT_BAD_ARGS}
}

constructCloudEndpointFilter()
{
    CloudEndptWhereClause="( ${1} like ('%${3}.${4}%') "
    if [ "${2}" -eq "default" ]
    then
        CloudEndptWhereClause="$CloudEndptWhereClause or ${1} like ('%azuredatalakestore.${4}%') "
    fi
    CloudEndptWhereClause="$CloudEndptWhereClause )"
    echo "$CloudEndptWhereClause"
}

constructURIExtractString()
{
    # Take in Target Attribute keystore and string for endpoint
    AttributeStore=($1)

    EndpointValue=${2}
    ExtractionString=$(cat <<-EOF

insert into LocationUpdate
(
    FK_ID,
    SCHEMATYPE,
    CONTAINER,
    ACCOUNT,
    ENPT,
    ROOTPATH
)
select ${AttributeStore[2]},
substring
(
    ${AttributeStore[1]},
    0,
    CHARINDEX('://', ${AttributeStore[1]})
),
substring
(
    ${AttributeStore[1]},
    CHARINDEX('://', ${AttributeStore[1]}) + len('://'),
    CHARINDEX('@', ${AttributeStore[1]}) - ( CHARINDEX('://', ${AttributeStore[1]}) + len('://') )
),
substring
(
    ${AttributeStore[1]},
    CHARINDEX('@', ${AttributeStore[1]}) + len('@'),
    CHARINDEX('.', ${AttributeStore[1]}) - ( CHARINDEX('@', ${AttributeStore[1]}) + len('@') )
),
substring
(
    ${AttributeStore[1]},
    CHARINDEX('.', ${AttributeStore[1]}) + len('.'),
    CHARINDEX('.${EndpointValue}', ${AttributeStore[1]}) - ( CHARINDEX('.', ${AttributeStore[1]}) + len('.') )
),
substring
(
    ${AttributeStore[1]},
    CHARINDEX('.${EndpointValue}', ${AttributeStore[1]}) + len('.${EndpointValue}'),
    (len(${AttributeStore[1]}) - CHARINDEX('/', reverse(${AttributeStore[1]})) + 1) - (CHARINDEX('.${EndpointValue}', ${AttributeStore[1]}) + len('.${EndpointValue}') - 1)
)

from ${AttributeStore[0]} WHERE
${3} and (${4});

EOF
)

    echo "$ExtractionString"
}

constructMigrationImpactDisplayString()
{
    AttributeStore=($1)
    MigrationImpactString=$(cat <<-EOF

select LocationUpdate.FK_ID as ID, ${AttributeStore[1]} as oldLocation,
(
    SCHEMATYPE +
    '://' +
    CONTAINER +
    '@' +
    ACCOUNT +
    '.' +
    ENPT +
    '.${EnvironmentMap[${2}]}' +
    ROOTPATH +
    right( ${AttributeStore[1]}, charindex('/', reverse(${AttributeStore[1]}) ) -1)
) as newLocation
from ${AttributeStore[0]}, LocationUpdate
where ${AttributeStore[2]} = LocationUpdate.FK_ID;

EOF
)

    echo "$MigrationImpactString"
}

constructFinalMigrationString()
{
    AttributeStore=($1)
    FinalMigrationString=$(cat <<-EOF

update ${TargetAttrs[0]} set ${TargetAttrs[1]} =
(
    SCHEMATYPE +
    '://' +
    CONTAINER +
    '@' +
    ACCOUNT +
    '.' +
    ENPT +
    '.${EnvironmentMap[${2}]}' +
    ROOTPATH +
    right( ${TargetAttrs[1]}, charindex('/', reverse(${TargetAttrs[1]}) ) -1)
)
from ${TargetAttrs[0]}, LocationUpdate
where ${TargetAttrs[2]} = LocationUpdate.FK_ID;

EOF
)

    echo "$FinalMigrationString"
}

constructMigrationTableCreateString()
{
    AttributeStore=($1)
    MigrationTableCreateString=$(cat <<-EOF

drop table if exists LocationUpdate;
create table LocationUpdate
(
    SCHEMATYPE nvarchar(1024),
    CONTAINER nvarchar(1024),
    ACCOUNT nvarchar(1024),
    ENPT nvarchar(1024),
    ROOTPATH nvarchar(1024),
    FK_ID bigint
);

EOF
)

    echo "$MigrationTableCreateString"
}

constructInvalidArgumentString()
{
    echo "${1} is not a valid entry for ${2}."
}
###############################################################################################################################################################################
###############################################################################################################################################################################
# Read inputs.
###############################################################################################################################################################################
###############################################################################################################################################################################

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
        -q|--queryclient)
        Client="$2"
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
        -adls|--adlaccounts)
        ADLAccountSrc="$2"
        shift
        shift
        ;;
        -e|--environment)
        AzureEnv="$2"
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
        usage "$1 is an unsupported flag. Closing."
        ;;
esac
done
set +o nounset
set -- "${POSITIONAL[@]}"

###############################################################################################################################################################################
###############################################################################################################################################################################
# Now we have all the inputs. Run parameter validation.
###############################################################################################################################################################################
###############################################################################################################################################################################

# Translate wildcards from argument '*' to character % for SQL parity
SrcFlags=("TypeSrc" "AccountSrc" "ADLAccountSrc" "ContainerSrc" "RootpathSrc")

for i in "${!SrcFlags[@]}"; do

    if [ ! -z "${!SrcFlags[$i]}" -a "${!SrcFlags[$i]}" = "*" ]
    then
        eval "${SrcFlags[$i]}=%"
    fi
done

# Check Mandatory Flags Present
argsMissing=false
ArgumentNames=("Client" "Username" "Password" "Server" "Database" "Target" "TypeSrc" "ContainerSrc" "RootpathSrc")

for i in "${!ArgumentNames[@]}"; do

    if [ -z "${!ArgumentNames[$i]}" ]
    then
        echo "${ArgumentNames[$i]} missing from command line arguments."
        argsMissing=true
    fi
done

if [ "${argsMissing}" = true ]
then
    usage "At least one mandatory argument missing."
fi

# Check ADL list not left empty if ADL or % in typesrc
if [ -z "${ADLAccountSrc}" ] && [ "${TypeSrc}" = "%" -o ! -z "$(echo "${TypeSrc}" | grep -i adl)" ]
then
    usage "ADL cannot be specified as a source type without also specifying ADL account names to be moved. Use '%' to move all ADL accounts."
fi

# Parse Inputs
IFS=',' read -r -a TypeSrcVals <<< "$TypeSrc"
IFS=',' read -r -a AccountSrcVals <<< "$AccountSrc"
IFS=',' read -r -a ContainerSrcVals <<< "$ContainerSrc"
IFS=',' read -r -a RootpathSrcVals <<< "$RootpathSrc"
IFS=',' read -r -a ADLAccountSrcVals <<< "$ADLAccountSrc"
IFS=',' read -r -a TargetVals <<< "$Target"

# Make sure that AccountSrc is present unless the only TypeSrc is ADL
if [ "${#TypeSrcVals[@]}" -gt 1 ] || [ ! "${TypeSrcVals[0]}" = "adl" ]
then
    if [ -z "${AccountSrc}" ]
    then
        usage "Account types other than ADL set but AccountSrc missing from command line arguments."
    fi
fi

# Make sure no invalid target tables are specified
for entry in "${TargetVals[@]}"
do
    if [ ! "${entry,,}" = "dbs" ] && [ ! "${entry,,}" = "sds" ] &&  [ ! "${entry,,}" = "func_ru" ] && [ ! "${entry,,}" = "skewed_col_value_loc_map" ]
    then
        usage "$(constructInvalidArgumentString "${entry}" "--target" ) Targets must be one or more of DBS, SDS, FUNC_RU or SKEWED_COL_VALUE_LOC_MAP"
    fi
done

# Make sure that there are not too many input values
if [ "${#AccountSrcVals[@]}" -gt "$MAX_ARG_COUNT" ] || [ "${#ContainerSrcVals[@]}" -gt "$MAX_ARG_COUNT" ] || [ "${#RootpathSrcVals[@]}" -gt "$MAX_ARG_COUNT" ] || [ "${#ADLAccountSrcVals[@]}" -gt "$MAX_ARG_COUNT" ]
then
    usage "Too many entries specified. Closing."
fi

# Make sure types are valid for source
for item in "${TypeSrcVals[@]}"
do
    if [ -z "${EndpointMap[${item}]}" -a ! "${item}" = "%" ]
    then
        usage "$(constructInvalidArgumentString "${TypeSrc}" "--typesrc" ) Storage types must be one or more of $(echo ${!EndpointMap[@]})."
    fi
done

if [ -z "${AzureEnv}" ]
then
    AzureEnv="default"
fi

# Make sure environment is valid
if [ -z "${EnvironmentMap[${AzureEnv}]}" ]
then
    usage "$(constructInvalidArgumentString "${AzureEnv}" "--environment" ) Azure Environment must be one of $(echo ${!EnvironmentMap[@]})."
fi

# Make sure target type is valid
if [ ! -z "${TypeDest}" ]
then
    if [ -z "${EndpointMap[${TypeDest}]}" ]
    then
        usage "$(constructInvalidArgumentString "${TypeDest}" "--typedest" ). Storage types must be one or more of $(echo ${!EndpointMap[@]})."
    fi
fi

# Check Query Client is supported
if [ -z "${sqlClientMap[${Client}]}" ]
then
    usage "$(constructInvalidArgumentString "${Client}" "--queryclient" ) Query Client must be one of $(echo ${!sqlClientMap[@]})."
fi

# Check account name not missing type
if [ -z "${TypeDest}" -a ! -z "${AccountDest}" ]
then
    usage "Destination account cannot be specified without destination account type"
fi

# Make sure ADL not specified when using a non-default cloud
if [ ! "${AzureEnv}" = "default" ]
then
    if [ "${TypeDest}" = "adl" ]
    then
        usage "Cannot include Azure Data Lake as the target type when working in non-default cloud: ${AzureEnv}."
    fi

    # Is ADL in the array of source types?
    if [ ! -z "$(echo "${TypeSrcVals[@]}" | grep -i adl)" ] || [ "${TypeSrcVals[0]}" = "%" ]
    then
        usage "Cannot include Azure Data Lake as a source type when working in non-default cloud: ${AzureEnv}"
    fi
fi

# If all dests are empty, exit
if [ -z "${TypeDest}" -a -z "${AccountDest}" -a -z "${ContainerDest}" -a -z "${RootpathDest}" ]
then
    echo "At least one of --typedest, --accountdest, --containerdest or --pathdest must be set."
    echo "No destination attributes set. Nothing to do."
    exit ${EXIT_NO_CHANGE}
fi

###############################################################################################################################################################################
###############################################################################################################################################################################
# Parameters are now validated. Here the WHERE clause is constructed for finding URIs that will be migrated
###############################################################################################################################################################################
###############################################################################################################################################################################


if [ ! -z "${TypeDest}" ]
then
    # set the correct protocol prefix that matches the storage type
    UpdateCommands="$UpdateCommands $(sed "s/*/SCHEMATYPE/g; s/#/${TypeDest}/g" <<< $UpdateTemplate)"
    # set the correct endpoint that matches the storage type, excluding the final domain
    AzureEnvDest="${AzureEnv}"
    if [ "${TypeDest}" = "adl" ]
    then
        AzureEnvDest=adl
    fi

    UpdateCommands="$UpdateCommands $(sed "s/*/ENPT/g; s/#/${EndpointMap[${TypeDest}]}${StorageDomainMap[${AzureEnvDest}]}/g;" <<< $UpdateTemplate)"
fi

for item in "${TypeSrcVals[@]}"
do
    WhereClauseStringsTypeSet+=("$(sed "s/*/$item/1" <<< $WhereClauseString)")
done

if [ ! -z "${ContainerDest}" ]
then
    # ://%@ -> ://dest_container@
    UpdateCommands="$UpdateCommands $(sed "s/*/CONTAINER/g; s/#/${ContainerDest}/g" <<< $UpdateTemplate)"
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
    UpdateCommands="$UpdateCommands $(sed "s/*/ACCOUNT/g; s/#/${AccountDest}/g" <<< $UpdateTemplate)"
fi

for TypeContainerSetTemplate in "${WhereClauseStringsTypeContainerSet[@]}"
do
    if [ -z "$(echo "${TypeContainerSetTemplate}" | grep -i "adl://")" ]
    then # This is a non-ADL template
        for item in "${AccountSrcVals[@]}"
        do
            WhereClauseStringsTypeContainerAccountSet+=("$(sed "s/*/$item\.%\.${EnvironmentMap[${AzureEnv}]}/1" <<< $TypeContainerSetTemplate)")
        done
    else
        for item in "${ADLAccountSrcVals[@]}" # Use a different list of source entries and a different endpoint style if the template is ADL
        do
            WhereClauseStringsTypeContainerAccountSet+=("$(sed "s/*/$item\.%\.${EnvironmentMap[adl]}/1" <<< $TypeContainerSetTemplate)")
        done
    fi
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

###############################################################################################################################################################################
###############################################################################################################################################################################
# Launch SQL commands
# 1. Create workbench table for migration
# 2. Insert items into workbench table that match expanded WHERE clause. The matches are parsed for their attributes, which are inserted into the appropriate column.
# 3. Update column entries in the workbench table.
# 3b. The path entry is updated by replacing the longest (from the root) source path found in the entry. That way the hierarchy of subfolders is preserved.
# 4. Update the original table (sds, dbs, etc) by putting the location string back together and matching the ID column
###############################################################################################################################################################################
###############################################################################################################################################################################

echo Username = "${Username}"
echo Password = "${Password}"
echo Server = "${Server}"
echo Database = "${Database}"
echo Azure Environment = "${AzureEnv}"
echo Running Live\? "${Liverun}"

echo Target\(s\) = "${TargetVals}"
echo Source Type\(s\) = "${TypeSrc}"
echo Account\(s\) = "${AccountSrc}"
echo Container\(s\) = "${ContainerSrc}"
echo Source Rootpath\(s\) = "${RootpathSrc}"

echo Final Type \(if any\) = "${TypeDest}"
echo Final Account \(if any\) = "${AccountDest}"
echo Final Container \(if any\) = "${ContainerDest}"
echo Final Rootpath \(if any\) = "${RootpathDest}"

# Expand out Targets if "ALL" was used
if [ "${TargetVals[0],,}" = "all" ]
then
    TargetVals=(DBS SDS FUNC_RU SKEWED_COL_VALUE_LOC_MAP)
fi

# Launch the sequence of SQL commands via beeline for each target table specified
for entry in "${TargetVals[@]}"
do
    if [ "${entry,,}" = "dbs" ]
    then
        TargetAttrs=("${DBSColumns[@]}")
    elif [ "${entry,,}" = "sds" ]
    then
        TargetAttrs=("${SDSColumns[@]}")
    elif [ "${entry,,}" = "func_ru" ]
    then
        TargetAttrs=("${FUNCRUColumns[@]}")
    elif [ "${entry,,}" = "skewed_col_value_loc_map" ]
    then
        TargetAttrs=("${SKEWEDCOLVALUELOCMAPColumns[@]}")
    fi

    EndpointClause=$(constructCloudEndpointFilter "${TargetAttrs[1]}" "${AzureEnv}" "${StorageDomainMap[${AzureEnv}]}" "${EnvironmentMap[${AzureEnv}]}")

    WhereClauseString=$(printf " OR ${TargetAttrs[1]} like ('%s')" "${WhereClauseStringsTypeContainerAccountPathSet[@]}" | cut -c 5-)
    MigrationTableCreateString=$(constructMigrationTableCreateString "$(echo ${TargetAttrs[@]})" )
    ExtractionString=$(constructURIExtractString "$(echo ${TargetAttrs[@]})" "${EnvironmentMap[${AzureEnv}]}" "${EndpointClause}" "${WhereClauseString}" )
    DisplayMigrationImpactString=$(constructMigrationImpactDisplayString "$(echo ${TargetAttrs[@]})" "${AzureEnv}" )
    ExecuteFinalMigrationString=$(constructFinalMigrationString "$(echo ${TargetAttrs[@]})" "${AzureEnv}" )
    CleanupMigrationTableString="drop table LocationUpdate;"

    echo "Generated Migration SQL Script:"

    echo "$MigrationTableCreateString"
    echo "$ExtractionString"
    echo "$UpdateCommands"
    echo "$DisplayMigrationImpactString"
    echo "$ExecuteFinalMigrationString"
    echo "$CleanupMigrationTableString"

    echo "Launching Migration SQL Commands:"

    echo "Creating temporary migration table..."
    launchSqlCommand $Client $Server $Database $Username $Password "${MigrationTableCreateString}"

    echo "Extracting matching URIs to temporary table..."
    launchSqlCommand $Client $Server $Database $Username $Password "${ExtractionString}"

    echo "Modiying temporary table per migration paremeters..."
    launchSqlCommand $Client $Server $Database $Username $Password "${UpdateCommands}"

    echo "Affected entries of ${TargetAttrs[0]} will have the following values pre and post-migration..."
    launchSqlCommand $Client $Server $Database $Username $Password "${DisplayMigrationImpactString}"
    echo ""

    if [ "$Liverun" = true ]
    then
        echo "Script execution type set to LIVE. Writing migration results to table: ${TargetAttrs[0]}..."
        launchSqlCommand $Client $Server $Database $Username $Password "${ExecuteFinalMigrationString}"
    else
        echo "LIVE flag not set. Migration execution skipped."
    fi

    echo "Deleting temporary migration table..."
    launchSqlCommand $Client $Server $Database $Username $Password "${CleanupMigrationTableString}"
done

echo "Migration script complete!"
if [ ! "$Liverun" = true ]
then
    echo "Closing with dry run exit code"
    exit ${EXIT_DRY_RUN}
fi
