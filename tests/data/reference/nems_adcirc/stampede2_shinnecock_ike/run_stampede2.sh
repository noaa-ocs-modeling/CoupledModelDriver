echo deleting previous ADCIRC output
sh cleanup.sh
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run spinup
pushd ${DIRECTORY}/spinup >/dev/null 2>&1
setup_jobid=$(sbatch setup.job | awk '{print $NF}')
spinup_jobid=$(sbatch adcirc.job --dependency=afterok:$setup_jobid | awk '{print $NF}')
popd >/dev/null 2>&1

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    setup_jobid=$(sbatch setup.job | awk '{print $NF}')
    sbatch adcirc.job --dependency=afterok:$setup_jobid,afterok:$spinup_jobid
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %3C %4D %97Z %15j" --sort i
echo squeue -u $USER -o \"%.8i %3C %4D %97Z %15j\" --sort i
