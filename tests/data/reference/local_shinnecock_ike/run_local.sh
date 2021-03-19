sh setup_local.sh

DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# run single coldstart configuration
pushd ${DIRECTORY}/coldstart >/dev/null 2>&1
sh setup.sh
sh adcprep.job
sh nems_adcirc.job
popd >/dev/null 2>&1

# run every hotstart configuration
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    sh setup.sh
    sh adcprep.job
    sh nems_adcirc.job
    popd >/dev/null 2>&1
done
