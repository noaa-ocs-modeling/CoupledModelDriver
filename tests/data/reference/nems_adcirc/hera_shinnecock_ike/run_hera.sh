echo deleting previous ADCIRC output
sh cleanup.sh
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    hotstart_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')
    sbatch --dependency=afterany:$hotstart_adcprep_jobid adcirc.job
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %3C %4D %97Z %15j" --sort i
echo squeue -u $USER -o \"%.8i %3C %4D %97Z %15j\" --sort i
