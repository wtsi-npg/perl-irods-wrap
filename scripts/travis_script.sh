#!/bin/bash

set -e -u -x

export TEST_AUTHOR=1
export TEST_RABBITMQ=1
export WTSI_NPG_iRODS_Test_irodsEnvFile=$HOME/.irods/.irodsEnv
export WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE=$HOME/.irods/irods_environment.json
export WTSI_NPG_iRODS_Test_Resource=testResc

perl Build.PL
./Build clean
./Build test
