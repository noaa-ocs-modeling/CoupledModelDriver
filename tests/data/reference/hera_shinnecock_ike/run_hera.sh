# prepare single coldstart directory
cd coldstart
ln -sf ../hera_adcprep.job adcprep.job
ln -sf ../hera_nems_adcirc.job.coldstart nems_adcirc.job
cd ..

# prepare every hotstart directory
for hotstart in ./runs/*/; do
    cd "$hotstart"
    ln -sf ../../hera_adcprep.job adcprep.job
    ln -sf ../../hera_nems_adcirc.job.hotstart nems_adcirc.job
    cd ../..
done

# run single coldstart configuration
cd coldstart
coldstart_adcprep_jobid=$(sbatch adcprep.job)
coldstart_jobid=$(sbatch --dependency=afterany:$coldstart_adcprep_jobid nems_adcirc.job)
cd ..

# run every hotstart configuration
for hotstart in ./runs/*/; do
    cd "$hotstart"
    hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job)
    sbatch --dependency=afterany:$hotstart_adcprep_jobid nems_adcirc.job
    cd ../..
done
squeue -u $USER -o "%.8A %.4C %.10m %.20E"
