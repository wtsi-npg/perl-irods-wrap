#!/bin/bash

set -ex

PERLBREW_ROOT=${PERLBREW_ROOT:-"/app/perlbrew"}
export PERLBREW_ROOT

PERLBREW_SHA256="8f254651d2eee188199b3355228eb67166974716081b794ca93b69c8f949c38d"
curl -sSL https://install.perlbrew.pl -o ./perlbrew.sh
sha256sum ./perlbrew.sh | grep "$PERLBREW_SHA256"
/bin/bash ./perlbrew.sh
rm ./perlbrew.sh
