# prepare single coldstart directory
cd coldstart
ln -sf ../local_adcprep.job adcprep.job
ln -sf ../local_nems_adcirc.job.coldstart nems_adcirc.job
cd ..

# prepare every hotstart directory
for hotstart in ./runs/*/; do
    cd "$hotstart"
    ln -sf ../../local_adcprep.job adcprep.job
    ln -sf ../../local_nems_adcirc.job.hotstart nems_adcirc.job
    cd ../..
done

# run single coldstart configuration
cd coldstart
sh adcprep.job
sh nems_adcirc.job
cd ..

# run every hotstart configuration
for hotstart in ./runs/*/; do
    cd "$hotstart"
    sh adcprep.job
    sh nems_adcirc.job
    cd ../..
done
