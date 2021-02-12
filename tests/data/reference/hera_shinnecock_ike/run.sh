cd coldstart
ln -sf ../hera_adcprep.job adcprep.job
ln -sf ../hera_nems_adcirc.job.coldstart nems_adcirc.job
coldstart_adcprep_jobid=$(sbatch adcprep.job)
coldstart_jobid=$(sbatch --dependency=afterany:$adcprep_jobid nems_adcirc.job)
cd ..
for directory in ./runs/*/; do
    cd "$directory"
    ln -sf ../../hera_adcprep.job adcprep.job
    ln -sf ../../hera_nems_adcirc.job.hotstart nems_adcirc.job
    hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job)
    sbatch --dependency=afterany:$hotstart_adcprep_jobid nems_adcirc.job
done
squeue -u $USER -o "%.8A %.4C %.10m %.20E"
