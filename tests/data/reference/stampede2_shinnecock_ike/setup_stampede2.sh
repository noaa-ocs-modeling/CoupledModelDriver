DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# prepare single coldstart directory
pushd ${DIRECTORY}/coldstart >/dev/null 2>&1
ln -sf ../setup.sh.coldstart setup.sh
ln -sf ../job_adcprep_stampede2.job adcprep.job
ln -sf ../job_nems_adcirc_stampede2.job.coldstart nems_adcirc.job
popd >/dev/null 2>&1

# prepare every hotstart directory
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    ln -sf ../../setup.sh.hotstart setup.sh
    ln -sf ../../job_adcprep_stampede2.job adcprep.job
    ln -sf ../../job_nems_adcirc_stampede2.job.hotstart nems_adcirc.job
    popd >/dev/null 2>&1
done
