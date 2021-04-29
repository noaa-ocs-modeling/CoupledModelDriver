echo deleting previous ADCIRC output
sh cleanup.sh
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run spinup
pushd ${DIRECTORY}/spinup >/dev/null 2>&1
spinup_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')
spinup_jobid=$(sbatch --dependency=afterany:$spinup_adcprep_jobid adcirc.job | awk '{print $NF}')
popd >/dev/null 2>&1

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$spinup_jobid adcprep.job | awk '{print $NF}')
    sbatch --dependency=afterany:$hotstart_adcprep_jobid adcirc.job
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %3C %4D %97Z %15j" --sort i
echo squeue -u $USER -o \"%.8i %3C %4D %97Z %15j\" --sort i
