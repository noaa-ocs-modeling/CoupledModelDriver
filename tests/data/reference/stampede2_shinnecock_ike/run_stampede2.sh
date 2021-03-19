sh setup_stampede2.sh

DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# run single coldstart configuration
pushd ${DIRECTORY}/coldstart >/dev/null 2>&1
sh setup.sh
coldstart_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')
coldstart_jobid=$(sbatch --dependency=afterany:$coldstart_adcprep_jobid nems_adcirc.job | awk '{print $NF}')
popd >/dev/null 2>&1

# run every hotstart configuration
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    sh setup.sh
    hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job | awk '{print $NF}')
    sbatch --dependency=afterany:$hotstart_adcprep_jobid nems_adcirc.job
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %.21j %.4C %.4D %.31E %.20V %.20S %.20e"
echo squeue -u $USER -o \"%.8i %.21j %.4C %.4D %.31E %.20V %.20S %.20e\"
