#!/bin/bash

set -e -x

IRODS_VERSION=${IRODS_VERSION:=3.3.1}

before_script_common() {
    return
}

before_script_3_3_1() {
    source $TRAVIS_BUILD_DIR/travis_linux_env.sh
    export IRODS_HOME=$TRAVIS_BUILD_DIR/iRODS
    export PATH=$PATH:$IRODS_HOME/clients/icommands/bin
    export IRODS_TEST_VAULT=/usr/local/var/lib/irods/Test

    sudo -E -u postgres $TRAVIS_BUILD_DIR/setup_pgusers.sh
    sudo -E -u postgres $TRAVIS_BUILD_DIR/irodscontrol psetup
    $TRAVIS_BUILD_DIR/irodscontrol istart ; sleep 10

    echo irods | script -q -c "iinit"
    iadmin mkresc testResc 'unix file system' cache `hostname --fqdn` $IRODS_TEST_VAULT

    iadmin asq 'select alias,sqlStr from R_SPECIFIC_QUERY where alias = ?' findQueryByAlias

    export WTSI_NPG_iRODS_Test_irodsEnvFile=$TRAVIS_BUILD_DIR/irods_env.conf
    export WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE=DUMMY_VALUE
}

before_script_4_1_x() {
    sudo -E -u postgres createuser -D -R -S irods
    sudo -E -u postgres createdb -O irods ICAT
    sudo -E -u postgres sh -c "echo \"ALTER USER irods WITH PASSWORD 'irods'\" | psql"
    sudo /var/lib/irods/packaging/setup_irods.sh < .travis.setup_irods
    sudo jq -f .travis.server_config /etc/irods/server_config.json > server_config.tmp
    sudo mv server_config.tmp /etc/irods/server_config.json
    ls -l /etc/irods
    sudo /etc/init.d/irods restart
    sudo -E su irods -c "iadmin mkuser $USER rodsadmin ; iadmin moduser $USER password testuser"
    sudo -E su irods -c "iadmin lu $USER"
    sudo -E su irods -c "mkdir -p /var/lib/irods/iRODS/Test"
    sudo -E su irods -c "iadmin mkresc testResc unixfilesystem `hostname --fqdn`:/var/lib/irods/iRODS/Test"
    mkdir $HOME/.irods
    sed -e "s#__USER__#$USER#" -e "s#__HOME__#$HOME#" < .travis.irodsenv.json > $HOME/.irods/irods_environment.json
    cat $HOME/.irods/irods_environment.json
    ls -la $HOME/.irods/
    echo testuser | script -q -c "iinit"
    ls -la $HOME/.irods/

    export WTSI_NPG_iRODS_Test_irodsEnvFile=DUMMY_VALUE
    export WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE=$HOME/.irods/irods_environment.json
}

case $IRODS_VERSION in

    3.3.1)
        before_script_common
        before_script_3_3_1
        ;;

    4.1.9)
        before_script_common
        before_script_4_1_x
        ;;

    *)
        echo Unknown iRODS version $IRODS_VERSION
        exit 1
esac
