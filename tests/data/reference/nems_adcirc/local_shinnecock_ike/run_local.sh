echo deleting previous ADCIRC output
sh C:\Repositories\CoupledModelDriver\tests\data\output\nems_adcirc\local_shinnecock_ike\cleanup.sh
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run single coldstart configuration
pushd ${DIRECTORY}/coldstart >/dev/null 2>&1
sh adcprep.job
sh adcirc.job
popd >/dev/null 2>&1

# run every hotstart configuration
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    sh adcprep.job
    sh adcirc.job
    popd >/dev/null 2>&1
done
