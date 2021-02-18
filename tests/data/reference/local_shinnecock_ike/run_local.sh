DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# prepare single coldstart directory
cd $DIRECTORY/coldstart
ln -sf ../job_adcprep_local.job adcprep.job
ln -sf ../job_nems_adcirc_local.job.coldstart nems_adcirc.job
cd $DIRECTORY

# prepare every hotstart directory
for hotstart in $DIRECTORY//runs/*/; do
    cd "$hotstart"
    ln -sf ../../job_adcprep_local.job adcprep.job
    ln -sf ../../job_nems_adcirc_local.job.hotstart nems_adcirc.job
    cd $DIRECTORY/
done

# run single coldstart configuration
cd $DIRECTORY/coldstart
sh adcprep.job
sh nems_adcirc.job
cd $DIRECTORY

# run every hotstart configuration
for hotstart in $DIRECTORY/runs/*/; do
    cd "$hotstart"
    sh adcprep.job
    sh nems_adcirc.job
    cd $DIRECTORY
done
