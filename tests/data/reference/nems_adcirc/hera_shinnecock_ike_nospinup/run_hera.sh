echo deleting previous ADCIRC output
sh cleanup.sh
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    setup_jobid=$(sbatch setup.job | awk '{print $NF}')
    sbatch --dependency=afterok:$setup_jobid adcirc.job
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %3C %4D %15j %16E %Z" --sort i
echo squeue -u $USER -o \"%.8i %3C %4D %15j %16E %Z\" --sort i
