DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# prepare single coldstart directory
cd $DIRECTORY/coldstart
ln -sf ../hera_adcprep.job adcprep.job
ln -sf ../hera_nems_adcirc.job.coldstart nems_adcirc.job
cd $DIRECTORY

# prepare every hotstart directory
for hotstart in $DIRECTORY//runs/*/; do
    cd "$hotstart"
    ln -sf ../../hera_adcprep.job adcprep.job
    ln -sf ../../hera_nems_adcirc.job.hotstart nems_adcirc.job
    cd $DIRECTORY/
done

# run single coldstart configuration
cd $DIRECTORY/coldstart
coldstart_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')
coldstart_jobid=$(sbatch --dependency=afterany:$coldstart_adcprep_jobid nems_adcirc.job | awk '{print $NF}')
cd $DIRECTORY

# run every hotstart configuration
for hotstart in $DIRECTORY/runs/*/; do
    cd "$hotstart"
    hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job | awk '{print $NF}')
    sbatch --dependency=afterany:$hotstart_adcprep_jobid nems_adcirc.job
    cd $DIRECTORY
done
echo squeue -u $USER -o \"%.8F %.21j %.4C %.4D %.31E %.7a %.9P %.20V %.20S %.20e\"
squeue -u $USER -o "%.8F %.21j %.4C %.4D %.31E %.7a %.9P %.20V %.20S %.20e"
