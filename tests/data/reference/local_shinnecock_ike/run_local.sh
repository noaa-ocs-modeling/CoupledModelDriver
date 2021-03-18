sh setup_local

DIRECTORY="$(
    cd "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# run single coldstart configuration
cd $DIRECTORY/coldstart
sh setup.sh
sh adcprep.job
sh nems_adcirc.job
cd $DIRECTORY

# run every hotstart configuration
for hotstart in $DIRECTORY/runs/*/; do
    cd "$hotstart"
    sh setup.sh
    sh adcprep.job
    sh nems_adcirc.job
    cd $DIRECTORY
done
