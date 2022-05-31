#!/bin/bash
echo deleting previous SCHISM output
sh cleanup.sh
echo deleting previous SCHISM logs
rm spinup/*.log runs/*/*.log
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# run spinup
pushd ${DIRECTORY}/spinup >/dev/null 2>&1
spinup_jobid=$(sbatch  schism.job | awk '{print $NF}')
combine_hotstart_jobid=$(sbatch --dependency=afterok:$spinup_jobid combine_hotstart.job | awk '{print $NF}')
popd >/dev/null 2>&1

# run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    run_jobid=$(sbatch --dependency=afterok:$combine_hotstart_jobid schism.job | awk '{print $NF}')
    if [ -f combine_hotstart.job ];then _=$(sbatch --dependency=afterok:$run_jobid combine_hotstart.job | awk '{print $NF}');fi
    if [ -f combine_output.job ];then _=$(sbatch --dependency=afterok:$run_jobid combine_output.job | awk '{print $NF}');fi
    popd >/dev/null 2>&1
done

# display job queue with dependencies
squeue -u $USER -o "%.8i %4C %4D %16E %12R %8M %j" --sort i
echo squeue -u $USER -o \"%.8i %4C %4D %16E %12R %8M %j\" --sort i
