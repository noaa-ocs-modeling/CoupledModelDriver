#!/bin/bash
echo deleting previous ADCIRC output
sh cleanup.sh
echo deleting previous ADCIRC logs
rm spinup/*.log runs/*/*.log
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    setup_jobid=$(sbatch setup.job | awk '{print $NF}')
    sbatch --dependency=afterok:$setup_jobid adcirc.job
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %4C %4D %16E %12R %8M %j" --sort i
echo squeue -u $USER -o \"%.8i %4C %4D %16E %12R %8M %j\" --sort i
