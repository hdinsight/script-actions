#!/bin/bash

##################################################################
# Custom action script to install/upgrade Mono (
#  a. Remove any existing version of Mono installed on a node
#  b. Install specified versioni of Mono (http://www.mono-project.com) on a HDInsight cluster's Node.
##################################################################

set -e

log(){
    local message="${1}"
    local current_time=$(date "+%Y.%m.%d-%H.%M.%S")
    echo "Install-Mono: ${current_time}: ${message}"
}

validate_installation(){
    log "Validating Mono installation for target version $1"
    local MATCH_STR="Mono JIT compiler version $1"
    log "Validating Mono Version $TARGET_VERSION is installed properly"
    local INSTALLED_VERSION_STR=$(mono --version 2>&1 | grep -w "Mono JIT compiler version")
    if [[ $INSTALLED_VERSION_STR != $MATCH_STR* ]] ; then
        log "Failed to detect mono version $1 on machine."
        exit 1
    fi
    
    if [ -f /tmp/tlstest.cs ]; then
        rm -f /tmp/tlstest.cs
    fi
    wget https://raw.github.com/mono/mono/master/mcs/class/Mono.Security/Test/tools/tlstest/tlstest.cs -O /tmp/tlstest.cs
    
    local CMD="mono /tmp/tlstest.exe https://www.nuget.org"
    mcs /tmp/tlstest.cs /r:System.dll /r:Mono.Security.dll
    local RET_VAL=$($CMD 2>&1 | sed '/^\s*$/d')
    if [[ "$RET_VAL" != "https://www.nuget.org" ]]; then
        log "Mono validation failed. Result from $CMD: $RET_VAL"
        exit 1
    fi
    log "Successfully validated that Mono is installed correctly"
}

if [ "$(id -u)" != "0" ]; then
    log  "This script is supposed to be invoked as root."
    exit 1
fi

# Note: Mono version will default to 4.8
declare MONO_VERSION="4.8.0"

# Note: It is expected that user's supplying Mono versions validated the version
# exists under http://download.mono-project.com/repo/debian/wheezy/snapshots folder.
if [ "$1" != "" ]; then
    MONO_VERSION="$1"
fi

log "Target Mono version to install: $MONO_VERSION"

REPO_CHECK_URL="https://download.mono-project.com/repo/debian/dists/wheezy/snapshots/$MONO_VERSION/"
if  ! curl -s --head  "$REPO_CHECK_URL" | head -n 1 | grep 200 >/dev/null; then
    log "Mono Version $MONO_VERSION not found at $REPO_CHECK_URL"
    exit 1
fi
log  "Checking for existing Mono version"
if type "mono" > /dev/null; then
    INSTALLED_VERSION_STR=$(mono --version 2>&1 | grep -w "Mono JIT compiler version")
    log  "Installed Version of Mono: ${INSTALLED_VERSION_STR}"
    
    MATCH_STR="Mono JIT compiler version $MONO_VERSION "
    if [[ $INSTALLED_VERSION_STR != $MATCH_STR* ]] ; then
        log  "Uninstalling current version, before installing target version"
        apt-get -y remove --purge --auto-remove mono-runtime
        apt-get -y remove --purge --auto-remove mono-complete
        apt-get -y remove --purge --auto-remove ca-certificates-mono
    else
        log  "Installed version of mono $INSTALLED_VERSION_STR matches target version $MONO_VERSION."
        validate_installation $MONO_VERSION
        
        exit 0
    fi
else
    log  "Mono not found on the system"
fi

log  "Looking for existing xamarian repo sources"
if [ -f /etc/apt/sources.list.d/mono-xamarin.list ]; then
    log  "Removing xamarian repo sources"
    rm /etc/apt/sources.list.d/mono-xamarin.list
fi
log  "Removed existing xamarian repo sources"

log  "Adding Mono GPG keys"
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF

MONO_RESOURCE_LIST_URL="http://download.mono-project.com/repo/debian wheezy/snapshots/$MONO_VERSION main"
log  "Adding Mono package resource list $MONO_RESOURCE_LIST_URL"
echo  "deb $MONO_RESOURCE_LIST_URL" | tee /etc/apt/sources.list.d/mono-xamarin.list

log  "Updating package list"
apt-get update

log  "Installing mono-complete package"
apt-get install -y mono-complete
log  "Installed mono-complete package"

log  "Installing ca-certificates-mono package"
apt-get install -y ca-certificates-mono
log  "Installed ca-certificates-mono package"

validate_installation $MONO_VERSION

log  "Finished installing Mono version $MONO_VERSION"