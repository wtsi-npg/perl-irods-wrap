#!/bin/bash

# set -e -x

IRODS_VERSION=${IRODS_VERSION:=3.3.1}

install_common() {
    sudo apt-get update -qq
    sudo apt-get install -qq odbc-postgresql unixodbc-dev

    tar xfz /tmp/jansson-${JANSSON_VERSION}.tar.gz -C /tmp
    cd /tmp/jansson-${JANSSON_VERSION}
    autoreconf -fi
    ./configure ; make ; sudo make install

    cd $TRAVIS_BUILD_DIR
    sudo ldconfig

    cpanm --no-lwp --notest https://github.com/wtsi-npg/perl-dnap-utilities/releases/download/${DNAP_UTILITIES_VERSION}/WTSI-DNAP-Utilities-${DNAP_UTILITIES_VERSION}.tar.gz
    cpanm --installdeps --notest .
}

install_3_3_1() {
    cd $TRAVIS_BUILD_DIR
    tar xfz /tmp/irods.tar.gz

    source $TRAVIS_BUILD_DIR/travis_linux_env.sh
    export IRODS_HOME=$TRAVIS_BUILD_DIR/iRODS
    export PATH=$PATH:$IRODS_HOME/clients/icommands/bin
    export IRODS_VAULT=/usr/local/var/lib/irods/Vault
    export IRODS_TEST_VAULT=/usr/local/var/lib/irods/Test

    sudo mkdir -p $IRODS_VAULT
    sudo chown $USER:$USER $IRODS_VAULT

    sudo mkdir -p $IRODS_TEST_VAULT
    sudo chown $USER:$USER $IRODS_TEST_VAULT

    tar xfz /tmp/baton-${BATON_VERSION}.tar.gz -C /tmp
    cd /tmp/baton-${BATON_VERSION}
    ./configure --with-irods=$IRODS_HOME ; make ; sudo make install

    cd $TRAVIS_BUILD_DIR
    sudo ldconfig
}

install_4_1_x() {
    sudo apt-get install -qq python-psutil python-requests
    sudo apt-get install super libjson-perl jq
    sudo -H pip install jsonschema

    sudo dpkg -i irods-icat-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb irods-database-plugin-postgres-${PG_PLUGIN_VERSION}-${PLATFORM}-${ARCH}.deb
    sudo dpkg -i irods-runtime-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb irods-dev-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb

    ls -l /usr/include/irods/rodsVersion.h
    ls -l /usr/lib/libRodsAPIs.a

    tar xfz /tmp/baton-${BATON_VERSION}.tar.gz -C /tmp
    cd /tmp/baton-${BATON_VERSION}
    ./configure --with-irods ; cat config.log ; make ; sudo make install

    cd $TRAVIS_BUILD_DIR
    sudo ldconfig
}

case $IRODS_VERSION in

    3.3.1)
        install_common
        install_3_3_1
        ;;

    4.1.8)
        install_common
        install_4_1_x
        ;;

    *)
        echo Unknown iRODS version $IRODS_VERSION
        exit 1
esac
