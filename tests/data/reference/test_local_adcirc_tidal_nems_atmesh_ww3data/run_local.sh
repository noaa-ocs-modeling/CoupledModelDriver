#!/bin/bash
echo deleting previous ADCIRC output
sh cleanup.sh
echo deleting previous ADCIRC logs
rm spinup/*.log runs/*/*.log
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run spinup
pushd ${DIRECTORY}/spinup >/dev/null 2>&1
sh setup.job
sh adcirc.job
popd >/dev/null 2>&1

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    sh setup.job
    sh adcirc.job
    popd >/dev/null 2>&1
done
