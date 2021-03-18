sh setup_hera

DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# run single coldstart configuration
cd $DIRECTORY/coldstart
sh setup.sh
coldstart_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')
coldstart_jobid=$(sbatch --dependency=afterany:$coldstart_adcprep_jobid nems_adcirc.job | awk '{print $NF}')
cd $DIRECTORY

# run every hotstart configuration
for hotstart in $DIRECTORY/runs/*/; do
    cd "$hotstart"
    sh setup.sh
    hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job | awk '{print $NF}')
    sbatch --dependency=afterany:$hotstart_adcprep_jobid nems_adcirc.job
    cd $DIRECTORY
done

# display job queue with dependencies
echo squeue -u $USER -o \"%.8i %.21j %.4C %.4D %.31E %.7a %.9P %.20V %.20S %.20e\"
squeue -u $USER -o "%.8i %.21j %.4C %.4D %.31E %.7a %.9P %.20V %.20S %.20e"
