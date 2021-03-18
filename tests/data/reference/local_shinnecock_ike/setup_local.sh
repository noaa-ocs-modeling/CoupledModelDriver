DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# prepare single coldstart directory
cd $DIRECTORY/coldstart
sh ../setup_coldstart.sh
ln -sf ../job_adcprep_local.job adcprep.job
ln -sf ../job_nems_adcirc_local.job.coldstart nems_adcirc.job
cd $DIRECTORY

# prepare every hotstart directory
for hotstart in $DIRECTORY//runs/*/; do
    cd "$hotstart"
    sh ../../setup_hotstart.sh.hotstart
    ln -sf ../../job_adcprep_local.job adcprep.job
    ln -sf ../../job_nems_adcirc_local.job.hotstart nems_adcirc.job
    cd $DIRECTORY/
done
