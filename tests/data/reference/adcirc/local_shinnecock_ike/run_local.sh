echo deleting previous ADCIRC output
sh cleanup.sh
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    sh adcprep.job
    sh adcirc.job
    popd >/dev/null 2>&1
done
