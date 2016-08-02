#!/bin/bash

set -e -x

IRODS_VERSION=${IRODS_VERSION:=3.3.1}

before_install_common() {
    sudo apt-get update -qq

    wget -q https://github.com/akheron/jansson/archive/v2.7.tar.gz -O /tmp/jansson-2.7.tar.gz -O /tmp/jansson-${JANSSON_VERSION}.tar.gz
    wget -q https://github.com/wtsi-npg/baton/releases/download/${BATON_VERSION}/baton-${BATON_VERSION}.tar.gz -O /tmp/baton-${BATON_VERSION}.tar.gz
}

before_install_3_3_1() {
    wget https://github.com/wtsi-npg/irods-legacy/releases/download/3.3.1-travis-bc85aa/irods.tar.gz -O /tmp/irods.tar.gz
}

before_install_4_1_x() {
    wget ${RENCI_URL}/pub/irods/releases/${IRODS_VERSION}/${PLATFORM}/irods-icat-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb
    wget ${RENCI_URL}/pub/irods/releases/${IRODS_VERSION}/${PLATFORM}/irods-database-plugin-postgres-${PG_PLUGIN_VERSION}-${PLATFORM}-${ARCH}.deb
    wget ${RENCI_URL}/pub/irods/releases/${IRODS_VERSION}/${PLATFORM}/irods-runtime-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb
    wget ${RENCI_URL}/pub/irods/releases/${IRODS_VERSION}/${PLATFORM}/irods-dev-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb
}

case $IRODS_VERSION in

    3.3.1)
        before_install_common
        before_install_3_3_1
        ;;

    4.1.9)
        before_install_common
        before_install_4_1_x
        ;;

    *)
        echo Unknown iRODS version $IRODS_VERSION
        exit 1
esac
