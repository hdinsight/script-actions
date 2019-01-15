#!/usr/bin/env python3
# A python script that tests MigrateMetastore.sh

"""
This python script is meant to be used alongside MigrateMetastore.sh for validation and development. Therefore, this script should be executed from the headnode of an HDInsight cluster.
The script takes in a SQL DB to run the tests against. Some tables will be created and deleted on the database, and some sample data will be read from and written to those tables.

The test suite uses pyodbc, a setup guide for which can be found here: https://www.microsoft.com/en-us/sql-server/developer-get-started/python/ubuntu/
The test suite also uses python 3.6, which can be installed if needed: https://askubuntu.com/questions/865554/how-do-i-install-python-3-6-using-apt-get

Arguments:
- Relative path to migration script
- SQL server
- SQL database
- SQL username
- SQL password
"""

###############################################################################################################################################################################
###############################################################################################################################################################################
# Imports, Variables, Classes, Functions
###############################################################################################################################################################################
###############################################################################################################################################################################

from collections import namedtuple
from pyodbc import connect
from os import path
from optparse import OptionParser
from re import escape
from re import search
from re import sub
from select import poll
from subprocess import PIPE
from subprocess import Popen
from sys import argv
from time import sleep

TOTAL_REQ_ARGS = 7

# Exit codes for testing taken from script source
EXIT_SUCCESS = 0
EXIT_BEELINE_FAIL = 25
EXIT_BAD_ARGS = 50
EXIT_NO_CHANGE = 75
EXIT_DRY_RUN = 100

MAX_SRC_ARGS = 10

ASSETS_DIR = "test-resources"
ARG_FILE = "mockup-mandatory-arguments"
URI_FILE = "mockup-metastore-uris"

STR_TYPE = "varchar(4000)"
BIGINT_TYPE = "bigint"
SET_PRMK = "primary key"

INPUT_TEST_STR = "InputTests"
EXEC_TEST_STR = "ExecTests"

ARG_WILDCARD = "*"

MOCKUP_FUNC_RU = "FUNC_RU" 
MOCKUP_SDS = "SDS" # This value must correspond to the entry in resources/mockup-mandatory-arguments
MOCKUP_DBS = "DBS"
MOCKUP_SKEWED_COL_VALUE_LOC_MAP = "SKEWED_COL_VALUE_LOC_MAP"

FlagsMap = {}
FlagsMap["--metastoreserver"] = "Server"
FlagsMap["--metastoredatabase"] = "Database"
FlagsMap["--metastoreuser"] = "Username"
FlagsMap["--metastorepassword"] = "Password"
FlagsMap["--typesrc"] = "TypeSrc"
FlagsMap["--containersrc"] = "ContainerSrc"
FlagsMap["--accountsrc"] = "AccountSrc"
FlagsMap["--pathsrc"] = "RootpathSrc"
FlagsMap["--target"] = "Target"
FlagsMap["--queryclient"] = "Client"

Match = namedtuple('Match', ['index', 'entry', 'transformedentry'])

class MigrationScriptCommandFailedException(Exception):
    pass

def dctToList(dct):
    dctlist = []
    for key in dct:
        temp = [key,dct[key]]
        dctlist.extend(temp)
    return dctlist

def getArgs():
    parser = OptionParser()

    parser.add_option("-m", "--migrationScriptPath", dest = "scriptPath", help = "Relative path to the metastore migration script")

    parser.add_option("-t", "--testSuites", dest = "suites", help = "A comma-separated list of the test suites to run: {0}, {1}, or All".format(INPUT_TEST_STR, EXEC_TEST_STR))

    parser.add_option("-s", "--server", dest = "server", help = "Address of the SQL server to be used for tests. Not used if only testing behavior of inputs.")

    parser.add_option("-d", "--database", dest = "database", help = "Name of the database within the server to use for tests. Not used if only testing behavior of inputs.")

    parser.add_option("-u", "--username", dest = "username", help = "Username to log into the test server. Not used if only testing behavior of inputs.")

    parser.add_option("-p", "--password", dest = "password", help = "Password to log into the test server. Not used if only testing behavior of inputs.")

    parser.add_option("-r", "--driver", dest = "driver", help = "The name of the ODBC driver to use to connect to the server. Not used if only testing behavior of inputs.")

    parser.add_option("-c", "--cleanup", action = "store_true", dest = "cleanupOnExit", default = False, 
        help = "Set this flag to make sure tables created by the tests are dropped. Tables will also be dropped in case of test failure.")
    
    (options, args) = parser.parse_args()

    # Validate
    assert(options.scriptPath is not None), "--migrationScriptPath must be specified. --help for more instructions."
    assert(options.suites is not None), "--testSuites must be specified. --help for more instructions."

    if options.suites.lower() == 'all':
        options.suites = "{0},{1}".format(INPUT_TEST_STR, EXEC_TEST_STR)

    dbParams = ['server', 'database', 'username', 'password', 'driver' ]
    assert( not( EXEC_TEST_STR in options.suites and any( options.__dict__[item] is None for item in dbParams ) ) ), \
        "The following arguments are missing since {0} is included in tests to run: {1}".format(EXEC_TEST_STR, ["--"+item for item in dbParams if options.__dict__[item] is None])

    return options

def listEqUnordered(l1, l2):
    sameLen = (len(l1) == len(l2) )
    sameItems = (sorted(l1) == sorted(l2))
    return sameLen and sameItems

def rootpathInURI(path, uri):
    entryAfterScheme = uri[uri.find("://") + len("://") : len(uri)]
    uriPathComponent = entryAfterScheme[entryAfterScheme.find("/") : len(entryAfterScheme)] 
    return uriPathComponent.startswith("/"+path+"/")

def runCommand(cmd, args=None, switches=None):
    fault = False
    stdout = ""
    stderr = ""
    exitCode = None
    execution = [cmd]
    if args:
        # Args is a keystore of {argname: value}
        execution.extend(dctToList(args))
    if switches:
        # Switches is an array of flags that do not require correspoding values
        execution.extend(switches)
    try:
        process = Popen(
            execution,
            stderr = PIPE,
            stdout = PIPE,
            encoding = 'utf8' # Communicating with the script requires utf-8, which requires python 3.6
        )

        stdout, stderr = process.communicate()
        exitCode = process.returncode
    except Exception as exc:
        print("Caught exception when executing {0}: {1}".format(cmd, exc))
        fault = True

    if fault:
        raise MigrationScriptCommandFailedException(
            "\n\nError executing {0} with arguments: '{1}'.\nStdout:\n{2}Stderr:\n{3}\nCommand gave exit code {4}. Closing.".format(cmd, args, stdout, stderr, exitCode)
        )

    return exitCode, stdout

###############################################################################################################################################################################
###############################################################################################################################################################################
# Shared test code
###############################################################################################################################################################################
###############################################################################################################################################################################

class MigrationScriptTestSuite:

    def __init__(
        self,
        Name,
        ScriptPath,
        Server = None,
        DB = None,
        User = None,
        Pass = None,
        Driver = None,
        CleanupOnExit = None
    ):
        self.TablesCreated = False
        for entry, value in locals().items():
            setattr(self, entry, value)

        self.BaseArguments = dict(item.split() for item in open(path.join(ASSETS_DIR, ARG_FILE), "r").read().splitlines())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        exitMsg="Succeeded" if exc_type == None else "Failed"
        print("Migration Script test suite: {0}. result: {1}!".format(self.Name, exitMsg))

    def getScriptArgumentsWithOverrides(self, argOverrides=None, argDeletions=None):
        # Use BaseArguments as start point and add/edit/delete as needed. Both args are arrays of 2-tuples
        cmdArgs = dict(self.BaseArguments)

        if argOverrides:
            for override in argOverrides:
                flag, newValue = override
                cmdArgs[flag] = newValue

        if argDeletions:
            for deletion in argDeletions:
                if deletion in cmdArgs:
                    del cmdArgs[deletion]

        return cmdArgs

    def checkExitCodeEqual(self, args, expectedCode, actualCode, output):
        assert(expectedCode == actualCode), "{0} with arguments(s) {1} expected exit code {2} but gave exit code {3}. Output was '{4}'.".format(self.ScriptPath, args, expectedCode, actualCode, output)

    def checkOutputContains(self, args, targetOutput, actualOutput):
        assert(targetOutput in actualOutput), "{0} with arguments(s) {1} did not contain '{2}'' in the stdout. Output was {3}.".format(self.ScriptPath, args, targetOutput, actualOutput)

    def checkOutputNotContains(self, args, targetOutput, actualOutput):
        assert(targetOutput not in actualOutput), "{0} with arguments(s) {1} unexpectedly contained '{2}' in the stdout. Output was {3}.".format(self.ScriptPath, args, targetOutput, actualOutput)

    def checkOutputEqual(self, args, targetOutput, actualOutput):
        assert(targetOutput == actualOutput), "{0} with arguments(s) {1} did not produce output equal to '{2}'. Output was {3}.".format(self.ScriptPath, args, targetOutput, actualOutput)

###############################################################################################################################################################################
###############################################################################################################################################################################
# Test code for input validation
###############################################################################################################################################################################
###############################################################################################################################################################################

class MigrationScriptInputTestSuite(MigrationScriptTestSuite):

    def testMissingMandatoryArgsCausesExit(self):
        print("Testing script behavior against missing arguments...")

        genericArgMissingStr = "At least one mandatory argument missing"
        specificArgMissingStr = "{0} missing from command line arguments."

        # No args
        print("Testing script behavior with no arguments...")
        scriptNoArgsExitCode, scriptNoArgsStdout = runCommand(self.ScriptPath)
        self.checkExitCodeEqual(None, EXIT_BAD_ARGS, scriptNoArgsExitCode, scriptNoArgsStdout)
        self.checkOutputContains(None, genericArgMissingStr, scriptNoArgsStdout)

        # Test one missing arg at a time
        for entry in self.BaseArguments:
            missingStr = FlagsMap[entry]
            print("Testing script behavior when missing argument {0}...".format(missingStr))

            testArgs = self.getScriptArgumentsWithOverrides(None, [entry])

            scriptMissingArgExitCode, scriptMissingArgStdout = runCommand(self.ScriptPath, testArgs)

            self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, scriptMissingArgExitCode, scriptMissingArgStdout)
            self.checkOutputContains(testArgs, specificArgMissingStr.format(missingStr), scriptMissingArgStdout)

            for entry in FlagsMap:
                if FlagsMap[entry] != missingStr:
                    self.checkOutputNotContains(testArgs, specificArgMissingStr.format(FlagsMap[entry]), scriptMissingArgStdout)

    def testInvalidArgsCausesExit(self):
        print("Testing script behavior against invalid arguments...")

        invalidArgStr = "{0} is not a valid entry for {1}"

        invalidTargetArg = ("--target", "invalidtarget")
        invalidEnvArg = ("--environment", "invalidenv")
        invalidSrcStorageTypeArg = ("--typesrc", "invalidstoragetypesrc")
        invalidDestStorageTypeArg = ("--typedest", "invalidstoragetypedest")
        invalidClientArg = ("--queryclient", "invalidqueryclient")

        # Check behavior with wrong arguments
        for entry in [invalidTargetArg, invalidEnvArg, invalidSrcStorageTypeArg, invalidDestStorageTypeArg, invalidClientArg]:
            invalidatedArgs = self.getScriptArgumentsWithOverrides([entry])
            print("Testing script behavior when argument {0} is invalid...".format(entry[0]))

            scriptInvalidArgExitCode, scriptInvalidArgStdout = runCommand(self.ScriptPath, invalidatedArgs)

            self.checkExitCodeEqual(invalidatedArgs, EXIT_BAD_ARGS, scriptInvalidArgExitCode, scriptInvalidArgStdout)
            self.checkOutputContains(invalidatedArgs, invalidArgStr.format(entry[1], entry[0]), scriptInvalidArgStdout)

    def testInvalidCombinationsCausesExit(self):
        print("Testing script behavior against invalid argument combinations...")

        # Non adl missing accountsrc
        print("Testing script behavior when type source includes non-adl but no non-adl accounts are specified...")
        wasbSrcTypeArg = ("--typesrc", "wasb")
        removeAccountList = "--accountsrc"

        testArgs = self.getScriptArgumentsWithOverrides([wasbSrcTypeArg], [removeAccountList])
        nonAdlMissingAcctSrcExitCode, nonAdlMissingAcctSrcStdout = runCommand(self.ScriptPath, testArgs)

        self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, nonAdlMissingAcctSrcExitCode, nonAdlMissingAcctSrcStdout)
        self.checkOutputContains(testArgs, "Account types other than ADL set but AccountSrc missing from command line arguments.", nonAdlMissingAcctSrcStdout)

        # adl missing adl accounts
        print("Testing script behavior when type source includes adl but no adl accounts are specified...")
        adlSrcTypeArg = ("--typesrc", "adl")
        removeAccountList = "--adlaccounts"

        testArgs = self.getScriptArgumentsWithOverrides([adlSrcTypeArg], [removeAccountList])
        adlMissingAdlAcctSrcExitCode, AdlMissingAdlAcctSrcStdout = runCommand(self.ScriptPath, testArgs)

        self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, adlMissingAdlAcctSrcExitCode, AdlMissingAdlAcctSrcStdout)
        self.checkOutputContains(testArgs, "ADL cannot be specified as a source type without also specifying ADL account names to be moved", AdlMissingAdlAcctSrcStdout)

        # adl and non default cloud
        cloudOptions = ["china", "germany", "usgov"]

        # adl as src
        print("Testing script behavior when type source includes adl but non default Azure Cloud is specified...")
        adlSrcTypeArg = ("--typesrc", "adl")
        adlAccountList = ("--adlaccounts", "foo")
        for option in cloudOptions:
            environmentOverrideArg = ("--environment", option)
            testArgs = self.getScriptArgumentsWithOverrides([adlSrcTypeArg, adlAccountList, environmentOverrideArg])
            adlNotSupportedExitCode, adlNotSupportedStdout = runCommand(self.ScriptPath, testArgs)

            self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, adlNotSupportedExitCode, adlNotSupportedStdout)
            self.checkOutputContains(testArgs, "Cannot include Azure Data Lake as a source type when working in non-default cloud: {0}".format(option), adlNotSupportedStdout)

        # adl as target
        print("Testing script behavior when type target is adl but non default Azure Cloud is specified...")
        adlDestTypeArg = ("--typedest", "adl")
        for option in cloudOptions:
            environmentOverrideArg = ("--environment", option)
            testArgs = self.getScriptArgumentsWithOverrides([adlDestTypeArg, environmentOverrideArg])
            adlNotSupportedExitCode, adlNotSupportedStdout = runCommand(self.ScriptPath, testArgs)

            self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, adlNotSupportedExitCode, adlNotSupportedStdout)
            self.checkOutputContains(testArgs, "Cannot include Azure Data Lake as the target type when working in non-default cloud: {0}".format(option), adlNotSupportedStdout)

        # account dest without account type
        print("Testing script behavior when destination account is set but account type is missing...")
        accountDestArg = ("--accountdest", "foo")
        removeAccountTypeDest = ("--typedest")

        testArgs = self.getScriptArgumentsWithOverrides([accountDestArg], [removeAccountTypeDest])
        accountTypeMissingExitCode, accountTypeMissingAcctSrcStdout = runCommand(self.ScriptPath, testArgs)

        self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, accountTypeMissingExitCode, accountTypeMissingAcctSrcStdout)
        self.checkOutputContains(testArgs, "Destination account cannot be specified without destination account type", accountTypeMissingAcctSrcStdout)

    def testTooManyArgsCausesExit(self):
        print("Testing script behavior when too many source values are set...")

        tooManyArgsStr = "Too many entries specified"
        typeSrcArg = ("--typesrc", "wasb,adl")

        accountSrcArg = ("--accountsrc")
        adlAccountSrcArg = ("--adlaccounts")
        containerSrcArg = ("--containersrc")
        pathSrcArg = ("--pathsrc")

        srcArgs = [accountSrcArg, adlAccountSrcArg, containerSrcArg, pathSrcArg]
        for optionStr in srcArgs:
            print("Testing script behavior when too many source values are set for {0}...".format(optionStr))
            option = [optionStr, (",".join([str(i) for i in range(MAX_SRC_ARGS+1)]))]
            overrides = [typeSrcArg, option]

            for value in [item for item in srcArgs if item != optionStr]:
                overrides.append([value, ARG_WILDCARD])

            testArgs = self.getScriptArgumentsWithOverrides(overrides)

            tooManyArgsExitCode, tooManyArgsStdout = runCommand(self.ScriptPath, testArgs)
            self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, tooManyArgsExitCode, tooManyArgsStdout)
            self.checkOutputContains(testArgs, tooManyArgsStr, tooManyArgsStdout)

    def testUnsupportedArgsCausesExit(self):
        print("Testing script behavior when an unsupported argument is specified...")

        testArgs = self.getScriptArgumentsWithOverrides([("--unsupportedTest", "argument")])

        unsupportedArgExitCode, unsupportedArgStdout = runCommand(self.ScriptPath, testArgs)
        self.checkExitCodeEqual(testArgs, EXIT_BAD_ARGS, unsupportedArgExitCode, unsupportedArgStdout)
        self.checkOutputContains(testArgs, "unsupportedTest is an unsupported flag.", unsupportedArgStdout)

    def testNoDestinationArgsCausesNoAction(self):
        print("Testing script behavior when there is no migration to do...")

        testArgs = self.getScriptArgumentsWithOverrides()
        noMigrationExitCode, noMigrationStdout = runCommand(self.ScriptPath, testArgs)
        self.checkExitCodeEqual(testArgs, EXIT_NO_CHANGE, noMigrationExitCode, noMigrationStdout)
        self.checkOutputContains(testArgs, "No destination attributes set. Nothing to do.", noMigrationStdout)

###############################################################################################################################################################################
###############################################################################################################################################################################
# Tests for query validation
###############################################################################################################################################################################
###############################################################################################################################################################################

class MigrationScriptExecutionTestSuite(MigrationScriptTestSuite):
    def __init__(
        self,
        Name,
        ScriptPath,
        Server,
        DB,
        User,
        Pass,
        Driver,
        CleanupOnExit
    ):
        super().__init__(Name, ScriptPath, Server, DB, User, Pass, Driver, CleanupOnExit)

        self.TableSchema = {}
        self.TableSchema[MOCKUP_FUNC_RU] = [("RESOURCE_URI", STR_TYPE), ("FUNC_ID", BIGINT_TYPE + ' ' + SET_PRMK)]
        self.TableSchema[MOCKUP_DBS] = [("DB_LOCATION_URI", STR_TYPE), ("DB_ID", BIGINT_TYPE + ' ' + SET_PRMK)]
        self.TableSchema[MOCKUP_SDS] = [("LOCATION", STR_TYPE), ("SD_ID", BIGINT_TYPE + ' ' + SET_PRMK)]
        self.TableSchema[MOCKUP_SKEWED_COL_VALUE_LOC_MAP] = [("LOCATION", STR_TYPE), ("SD_ID", BIGINT_TYPE + ' ' + SET_PRMK)]

        # Make sure we are not about to test on a database with the tables already present
        with self.getODBCConnection().cursor() as crsr:
            for table in self.TableSchema:
                self.checkTableNotExists(table, crsr)

        liveServer = ("--metastoreserver", self.Server)
        liveDB = ("--metastoredatabase", self.DB)
        liveUser = ("--metastoreuser", self.User)
        livePassword = ("--metastorepassword", self.Pass)
        # Override the default arguments so the queries run live
        self.BaseArguments = self.getScriptArgumentsWithOverrides([liveServer, liveDB, liveUser, livePassword])
        self.SampleOverrides = [
            ("--accountsrc", "gopher"),
            ("--adlaccounts", "gopher"),
            ("--containersrc", "bravo"),
            ("--pathsrc", ARG_WILDCARD),
            ("--typesrc", "abfs,abfss,wasb,wasbs"),
            ("--accountdest", "echo"),
            ("--typedest", "wasbs"),
        ]

        self.SampleData = open(path.join(ASSETS_DIR, URI_FILE), "r").read().splitlines()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.TablesCreated and self.CleanupOnExit: # Don't clean up the tables if we didn't make them
            with self.getODBCConnection().cursor() as crsr:
                self.dropURITables(crsr)

        super().__exit__(exc_type, exc_value, traceback)

    def checkMigrationScriptResult(self, testArgs, testExitCode, testStdout, expectedExitCode, expectedOutput, resultBefore, resultAfter):
        self.checkExitCodeEqual(testArgs, expectedExitCode, testExitCode, testStdout)
        self.checkOutputContains(testArgs, expectedOutput, testStdout)
        self.checkURIResultMatch(testArgs, resultBefore, resultAfter, testStdout)

    def checkTableNotExists(self, table, crsr):
        errorMsg = "Table {0} already exists in database {1} on server {2}. Tests will be run against this table so use a database where {0} does not exist.".format(table, self.DB, self.Server)
        assert(not self.tableExistsQuery(table, crsr)), errorMsg

    def checkTableUnchanged(self, tableRows):
        errorMsg = "Query result was unexpectely different from the original sample data. Query result was: {0}.".format(tableRows)
        assert(listEqUnordered(tableRows, self.SampleData)), errorMsg

    def checkURIResultMatch(self, args, resultBefore, resultAfter, stdout):
        errorMsg = "{0} with arguments: {1} unexpectely altered the did not produce the correct migration result. \
                    URIs matching post-migration expression before execution were: {2} and after execution were: {3}. \
                    Stdout was: {4}.".format(
            self.ScriptPath,
            args,
            resultBefore,
            resultAfter,
            stdout
        )
        assert(listEqUnordered(resultBefore, resultAfter)), errorMsg

    def createTable(self, table, columns, crsr):
        queryStr = "create table {0} ({1});".format(table, ', '.join(colname + ' ' + coltype for colname, coltype in columns))
        print("Creating table {0} with query: {1}".format(table, queryStr));
        crsr.execute(queryStr)

    def dropTable(self, tableName, crsr):
        print("Dropping table {0}.".format(tableName))
        queryStr = "drop table if exists {0}".format(tableName)
        crsr.execute(queryStr)

    def dropURITables(self, crsr):
        for tableName in self.TableSchema:
            self.dropTable(tableName, crsr)
        self.TablesCreated = False

    def getODBCConnection(self):
        return connect('DRIVER={' + self.Driver + '};SERVER=' + self.Server + '.database.windows.net;PORT=1433;DATABASE=' + self.DB + ';UID=' + self.User + ';PWD=' + self.Pass)

    def getAllURIsFromTable(self, tableName=MOCKUP_SDS):
        with self.getODBCConnection().cursor() as crsr:
            queryStr = "select {0} as uri from {1}".format(
                self.TableSchema[tableName][0][0],
                tableName
            )
            print("Gathering all URIs in {0}...".format(tableName))
            queryResult = crsr.execute(queryStr)
            finalResult = ([] if queryResult.rowcount == 0 else [entry.uri for entry in queryResult.fetchall()])

        return finalResult

    def getParameterizedURI(self, replacementKeys, args=None, cloudOverride=None):
        if args is None:
            args = dict(self.BaseArguments)

        genericURI = "{0}://{1}@{2}.%.{4}/{3}/%".format(
            *[args[item] if item in args else '%' for item in replacementKeys ], cloudOverride or "%"
        )
        return genericURI

    def getMatchingURIsFromTableWithID(self, tableName, genericURI):
        with self.getODBCConnection().cursor() as crsr:
            queryStr = "select {0}, {1} from {2} where {0} like ?".format(
                self.TableSchema[tableName][0][0],
                self.TableSchema[tableName][1][0],
                tableName
            )
            print("Gathering all entries in {0} that match query: {1}".format(tableName, queryStr).replace('?', genericURI))
            queryResult = crsr.execute(queryStr, genericURI)
            finalResult = ([] if queryResult.rowcount == 0 else [tuple(entry) for entry in queryResult.fetchall()])

        return finalResult

    def getTransformedURI(self, uri, args, cloudEndpoint):
        newuri = uri

        if "--typedest" in args:
            newuri = sub("(" + args["--typesrc"].replace(",", "|").replace(ARG_WILDCARD, ".*") + ")://", escape(args["--typedest"])+"://", newuri)

            if args["--typedest"] in "wasbs":
                newuri = sub( escape(".") + "[a-z0-9.]+" + escape("/"), ".blob." + cloudEndpoint + "/", newuri)
            elif args["--typedest"] in "abfss":
                newuri = sub( escape(".") + "[a-z0-9.]+" + escape("/"), ".dfs." + cloudEndpoint + "/", newuri)
            else: # adl
                newuri = sub( escape(".") + "[a-z0-9.]+" + escape("/"), ".azuredatalakestore.net/", newuri)
 
        if "--containerdest" in args:
            newuri = sub("://(" + args["--containersrc"].replace(",", "|").replace(ARG_WILDCARD, "[a-z0-9]+") + ")@", "://" + escape(args["--containerdest"]) + "@", newuri)

        if "--accountdest" in args:
            newuri = sub("@(" + args["--accountsrc"].replace(",", "|").replace(ARG_WILDCARD, "[a-z0-9]+") + ")" + escape("."), "@" + escape(args["--accountdest"]) + ".", newuri)

        if "--pathdest" in args:
            pathsubstr = newuri.split(".")
            pathRegex = "/(" + args["--pathsrc"].replace(",", "|").replace(ARG_WILDCARD, ".*") + ")/"

            # Make sure the match is a prefix
            if search(pathRegex, pathsubstr[-1][pathsubstr[-1].find('/'):len(pathsubstr[-1])]).start() == 0:
                pathsubstr[-1] = sub( "/(" + args["--pathsrc"].replace(",", "|").replace(ARG_WILDCARD, ".*") + ")/", "/" + escape(args["--pathdest"]) + "/", pathsubstr[-1])

            newuri = ".".join(pathsubstr)

        return newuri

    def getURITransformationOutput(self, args, cloudEndpoint):
        matches = []
        srcFlags = ["--typesrc", "--containersrc", "--accountsrc", "--pathsrc"]

        # get all entries in sample data that match source uri
        srcMatchURI = (self.getParameterizedURI(["--typesrc", "--containersrc", "--accountsrc", "--pathsrc"], args, cloudEndpoint)).replace("%", "*")

        for index, entry in enumerate(self.SampleData):
            if(
                (args["--typesrc"] == ARG_WILDCARD or any(acctype+"://" in entry for acctype in args["--typesrc"].split(','))) and
                (args["--containersrc"] == ARG_WILDCARD or any("://"+container+"@" in entry for container in args["--containersrc"].split(','))) and
                (args["--accountsrc"] == ARG_WILDCARD or any("@"+acc+"." in entry for acc in args["--accountsrc"].split(','))) and
                (args["--pathsrc"] == ARG_WILDCARD or any(rootpathInURI(path, entry) for path in args["--pathsrc"].split(','))) and
                cloudEndpoint in entry
            ):
                # Match found
                matches.append( Match(index+1, entry, self.getTransformedURI(entry, args, cloudEndpoint)) )

        return matches

    def insertIntoURITableWithIDs(self, table, uricol, idcol, data, crsr):
        insertionString = ', '.join('('+str(index+1)+', \''+item+'\')' for index, item in enumerate(data))
        queryStr = "insert into {0} ({1}, {2}) values {3};".format(table, idcol, uricol, insertionString)
        crsr.execute(queryStr)

    def loadDataIntoTables(self, crsr):
        for table in self.TableSchema:
            self.checkTableNotExists(table, crsr)
            self.createTable(table, self.TableSchema[table], crsr)
            self.insertIntoURITableWithIDs(table, self.TableSchema[table][0][0], self.TableSchema[table][1][0], self.SampleData, crsr)
        self.TablesCreated = True

    def loadTables(self):
        print("Dropping test tables and re-loading...")
        with self.getODBCConnection().cursor() as crsr:
            self.dropURITables(crsr)
            self.loadDataIntoTables(crsr)

    def runMigrationTest(self, testArgs, tableName=MOCKUP_SDS, azureEnv="default", cloudEndpoint="core.windows.net"):
        migrationResultURI = self.getParameterizedURI(["--typedest", "--containerdest", "--accountdest", "--pathdest"], testArgs, cloudEndpoint)

        # URIs that already matched the migration result before the migration runs
        matchingURIsBeforeDryExec = self.getMatchingURIsFromTableWithID(tableName, migrationResultURI)
        # A list of triples for URIs that match the src flags: index, uri, transformeduri
        expectedURITransformationOutput = self.getURITransformationOutput(testArgs, cloudEndpoint)
        # Migration source flag matches in the form of the output the script produces
        expectedOutputResult = "\n".join([",".join([str(x) for x in item]) for item in expectedURITransformationOutput])

        # Dry run: Check exit code, output match and no db change
        print("Executing dry run...")
        testExitCode, testStdout = runCommand(self.ScriptPath, testArgs)
        # Since this was a dry run, table should not have any new URIs matching the migration result
        matchingURIsAfterDryRunExec = self.getMatchingURIsFromTableWithID(tableName, migrationResultURI)
        self.checkMigrationScriptResult(testArgs, testExitCode, testStdout, EXIT_DRY_RUN, expectedOutputResult, matchingURIsBeforeDryExec, matchingURIsAfterDryRunExec)

        # Live run: Check exit code, output match and exists db change
        print("Executing live...")
        testExitCode, testStdout = runCommand(self.ScriptPath, testArgs, ["--liverun"])
        matchingURIsAfterLiveRunExec = self.getMatchingURIsFromTableWithID(tableName, migrationResultURI)
        # Since this was a live run, table should have the matching URIs from before exec, as well as some new ones as predicted by getURITransformationOutput()
        matchingURIsBeforeLiveExec = matchingURIsBeforeDryExec + [(match.transformedentry, match.index) for match in expectedURITransformationOutput]
        self.checkMigrationScriptResult(testArgs, testExitCode, testStdout, EXIT_SUCCESS, expectedOutputResult, matchingURIsBeforeLiveExec, matchingURIsAfterLiveRunExec)

    def tableExistsQuery(self, table, crsr):
        existsQueryResult = crsr.execute("select OBJECT_ID('{0}') as result".format(table));
        return existsQueryResult.fetchone().result != None

    def testSampleMigrationInAllClouds(self):
        self.loadTables()
        cloudOptions = {}
        cloudOptions["default"] = "core.windows.net"
        cloudOptions["china"] = "core.chinacloudapi.cn"
        cloudOptions["germany"] = "core.cloudapi.de"
        cloudOptions["usgov"] = "core.usgovcloudapi.net"

        for option in cloudOptions:
            print("Testing script behavior when executing a standard migration in Azure environment: {0}...".format(option))
            testArgs = self.getScriptArgumentsWithOverrides([("--environment", option)] + self.SampleOverrides)
            self.runMigrationTest(testArgs, MOCKUP_SDS, option, cloudOptions[option])

    def testSampleMigrationInAllTables(self):
        self.loadTables()
        tables = [MOCKUP_SDS, MOCKUP_DBS, MOCKUP_FUNC_RU, MOCKUP_SKEWED_COL_VALUE_LOC_MAP]

        for table in tables:
            print("Testing script behavior when executing a standard migration against metastore table: {0}...".format(table))
            testArgs = self.getScriptArgumentsWithOverrides([("--target", table)] + self.SampleOverrides)
            self.runMigrationTest(testArgs, table, "default", "core.windows.net")

    def testSampleMigrationInAllSupportedClients(self):
        clientArgs = ["beeline", "sqlcmd"]

        for client in clientArgs:
            print("Testing script behavior when executing a standard migration using query client: {0}...".format(client))
            self.loadTables()
            testArgs = self.getScriptArgumentsWithOverrides([("--queryclient", client)] + self.SampleOverrides)
            self.runMigrationTest(testArgs, MOCKUP_SDS, "default", "core.windows.net")

    def testAdlMigrations(self):
        adlAccounts = ["gopher", "echo"]
        adlAccountsAsStr = ",".join(adlAccounts)

        argOverrides = [
            ("--containersrc", ARG_WILDCARD),
            ("--pathsrc", ARG_WILDCARD),
        ]

        adlSrcArgs = [
            ("--typesrc", "adl"),
            ("--adlaccounts", adlAccountsAsStr)
        ]

        adlDestArgs = [
            ("--accountdest", "newadlacct"),
            ("--typedest", "adl")
        ]

        nonAdlSrcArgs = [
            ("--typesrc", "wasb"),
            ("--accountsrc", "echo")
        ]

        nonAdlDestArgs = [
            ("--accountdest", "newwasbacct"),
            ("--typedest", "wasb")
        ]

        print("Testing script behavior when executing migrations involving Azure Data Lake accounts...")

        print("Testing script behavior for ADL to ADL migration...")
        self.loadTables()
        testArgs = self.getScriptArgumentsWithOverrides(argOverrides + adlSrcArgs + adlDestArgs)
        self.runMigrationTest(testArgs)

        print("Testing script behavior for ADL to non ADL migration...")
        self.loadTables()
        testArgs = self.getScriptArgumentsWithOverrides(argOverrides + adlSrcArgs + nonAdlDestArgs)
        self.runMigrationTest(testArgs)

        print("Testing script behavior for non ADL to ADL migration...")
        self.loadTables()
        testArgs = self.getScriptArgumentsWithOverrides(argOverrides + nonAdlSrcArgs + adlDestArgs)
        self.runMigrationTest(testArgs, cloudEndpoint=".azuredatalakestore.net")

    def testContainerPathPatternMatching(self):
        argOverrides = [
            ("--containersrc", ARG_WILDCARD),
            ("--typesrc", ARG_WILDCARD),
            ("--accountsrc", ARG_WILDCARD),
            ("--adlaccounts", ARG_WILDCARD),
            ("--accountdest", "newwasbacct"),
            ("--typedest", "wasb")
        ]

        """
        warehouse/hivetables should move
        warehouse/hive should not move
        hive should not move
        managed/table should stil end with hive
        warehouse,managed/tables/hive should do a partial replace and a full replace
        """
        print("Testing script behavior when executing migrations involving various possible container paths...")
        pathOptions = ["warehouse/hivetables", "warehouse/hive", "hive", "managed/tables", "warehouse,managed/tables/hive", "managed/tables/hive,managed/tables"]
        pathResult = "resultpath"
        for option in pathOptions:
            self.loadTables()
            print("Testing script behavior when replacing path(s) {0} with path {1}".format(option, pathResult))
            pathArgs = [("--pathsrc", option), ("--pathdest", pathResult)]
            testArgs = self.getScriptArgumentsWithOverrides(argOverrides + pathArgs)
            self.runMigrationTest(testArgs)

    def testNonMatchingURIsUnchanged(self):
        self.loadTables()
        argOverrides = [
            ("--containersrc", "water"),
            ("--typesrc", "abfss"),
            ("--accountsrc", "xylophone"),
            ("--adlaccounts", "yellow"),
            ("--accountdest", "zebra"),
            ("--typedest", "wasb")
        ]

        print("Testing script behavior when no migration matches are expected...")
        testArgs = self.getScriptArgumentsWithOverrides(argOverrides)
        self.runMigrationTest(testArgs)

    def testAllMigrationAspects(self):
        self.loadTables()
        argOverrides = self.SampleOverrides + [
            ("--containerdest", "newctr"),
            ("--pathdest", "newpath")
        ]

        print("Testing script behavior when all possible migration parameters are specified...")
        testArgs = self.getScriptArgumentsWithOverrides(argOverrides)
        self.runMigrationTest(testArgs)

    def testSQLFailureCausesNoChange(self):
        self.loadTables()
        print("Testing script behavior when there is an unexpected failure during sql execution.")

        execution = [self.ScriptPath] + dctToList(self.getScriptArgumentsWithOverrides(self.SampleOverrides)) + ["--liverun"]
        migrationProcess = Popen(
            execution,
            stderr = PIPE,
            stdout = PIPE,
            encoding = 'utf8' # Communicating with the script requires utf-8, which requires python 3.6
        )

        migrationScriptPoll = poll()
        migrationScriptPoll.register(migrationProcess.stdout)
        while migrationProcess.returncode is None:
            if migrationScriptPoll.poll(0.5):
                stdout = migrationProcess.stdout.readline()
                if "Writing migration results to table" in stdout:
                    migrationProcess.kill() # Kill the process during the write
                    migrationProcess.communicate()

        with self.getODBCConnection().cursor() as crsr:
            crsr.execute("drop table if exists locationupdate;")

        # Select the URIs from MOCKUP_SDS to make sure no changes took effect despite killing the process in the middle of writing to MOCKUP_SDS
        uriResult = self.getAllURIsFromTable()
        self.checkTableUnchanged(uriResult)

    def testMigrationCommandIdempotent(self):
        print("Testing migration script idempotency by executing same migration twice.")
        self.loadTables()
        testArgs = self.getScriptArgumentsWithOverrides(self.SampleOverrides)

        print("Running first execution...")
        self.runMigrationTest(testArgs)

        # Set the sample data to reflect the change made to the database
        print("Updating sample data to reflect migration result...")
        self.SampleData = self.getAllURIsFromTable()

        print("Running second execution...")
        self.runMigrationTest(testArgs)

        # This test alters the sample data to test how the data is impacted by repeated migrations
        # So the sample data must be replaced on completion
        self.SampleData = open(path.join(ASSETS_DIR, URI_FILE), "r").read().splitlines()

    def testMaximumSizeWhereClause(self):
        self.loadTables()
        longArgStr = "alpha,bravo,charlie,delta,echo,foxtrot,gopher,hedgehog,igloo,jupiter"
        argOverrides = [
            ("--containersrc", longArgStr),
            ("--pathsrc", longArgStr),
            ("--accountsrc", longArgStr),
            ("--adlaccounts", longArgStr),
            ("--typesrc", "wasb,adl"),
            ("--accountdest", "newacct"),
            ("--typedest", "wasb")
        ]

        print("Testing script behavior when the maximum number of arguments is passed to the script...")
        testArgs = self.getScriptArgumentsWithOverrides(argOverrides)
        self.runMigrationTest(testArgs)
        # Need to check exit code 25

###############################################################################################################################################################################
###############################################################################################################################################################################
# Execution
###############################################################################################################################################################################
###############################################################################################################################################################################

def main():
    arguments = getArgs()

    if INPUT_TEST_STR in arguments.suites:
        with MigrationScriptInputTestSuite("InputTests", arguments.scriptPath) as testSuite:

            # Tests for argument syntax
            testSuite.testMissingMandatoryArgsCausesExit()
            testSuite.testInvalidArgsCausesExit()
            testSuite.testInvalidCombinationsCausesExit()
            testSuite.testUnsupportedArgsCausesExit()

            # Tests for argument semantics
            testSuite.testTooManyArgsCausesExit()
            testSuite.testNoDestinationArgsCausesNoAction()

    if EXEC_TEST_STR in arguments.suites:
        with MigrationScriptExecutionTestSuite(
            "ExecTests",
            arguments.scriptPath,
            arguments.server,
            arguments.database,
            arguments.username,
            arguments.password,
            arguments.driver,
            arguments.cleanupOnExit
        ) as testSuite:

            # Tests for alternative clouds, metastore tables and query clients
            testSuite.testSampleMigrationInAllClouds()
            testSuite.testSampleMigrationInAllTables()
            testSuite.testSampleMigrationInAllSupportedClients()

            # Tests for special cases
            testSuite.testAdlMigrations()
            testSuite.testContainerPathPatternMatching()
            testSuite.testNonMatchingURIsUnchanged()

            # Tests for script design
            testSuite.testAllMigrationAspects()
            testSuite.testSQLFailureCausesNoChange()
            testSuite.testMigrationCommandIdempotent()
            testSuite.testMaximumSizeWhereClause()


if __name__ == "__main__":
    main()
