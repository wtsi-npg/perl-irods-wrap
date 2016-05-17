#!/bin/bash

set -e -x

export TEST_AUTHOR=1
perl Build.PL
./Build clean
./Build test

if [ $? -ne 0 ]; then
    echo ===============================================================================
    cat tests.log
    echo ===============================================================================
fi
