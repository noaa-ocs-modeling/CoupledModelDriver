#!/bin/bash --login

main() {
  run_coldstart_phase
  for directory in ./runs/*/; do
    echo "Starting configuration $directory..."
    cd "$directory"
    SECONDS=0
    if grep -Rq "ERROR: Elevation.gt.ErrorElev, ADCIRC stopping." test_case_1.out.log; then
      duration=$SECONDS
      echo "ERROR: Elevation.gt.ErrorElev, ADCIRC stopping."
      echo "Wallclock time: $($duration / 60) minutes and $($duration % 60) seconds."
      exit -1
    else
      run_hotstart_phase
      duration=$SECONDS
      if grep -Rq "ERROR: Elevation.gt.ErrorElev, ADCIRC stopping." test_case_1.out.log; then
        echo "ERROR: Elevation.gt.ErrorElev, ADCIRC stopping."
        echo "Wallclock time: $($duration / 60) minutes and $($duration % 60) seconds."
        exit -1
      fi
    fi
    echo "Wallclock time: $($duration / 60) minutes and $($duration % 60) seconds."
    cd ..
  done
}

run_coldstart_phase() {
  rm -rf ./coldstart/*
  cd ./coldstart
  ln -sf ../fort.13 ./fort.13
  ln -sf ../fort.14 ./fort.14
  ln -sf ../fort.15.coldstart ./fort.15
  ln -sf ../../nems.configure.coldstart ./nems.configure
  ln -sf ../../model_configure.coldstart ./model_configure
  ln -sf ../../atm_namelist.rc.coldstart ./atm_namelist.rc
  ln -sf ../../config.rc.coldstart ./config.rc
  adcprep --np $SLURM_NTASKS --partmesh
  adcprep --np $SLURM_NTASKS --prepall
   NEMS.x
  clean_directory
  cd ..
}

run_hotstart_phase() {
  rm -rf ./hotstart/*
  cd ./hotstart
  ln -sf ../fort.13 ./fort.13
  ln -sf ../fort.14 ./fort.14
  ln -sf ../fort.15.hotstart ./fort.15
  ln -sf ../../nems.configure.hotstart ./nems.configure
  ln -sf ../../nems.configure.hotstart ./nems.configure
  ln -sf ../../model_configure.hotstart ./model_configure
  ln -sf ../../atm_namelist.rc.hotstart ./atm_namelist.rc
  ln -sf ../../config.rc.hotstart ./config.rc
  ln -sf ../../../coldstart/fort.67.nc ./fort.67.nc
  adcprep --np $SLURM_NTASKS --partmesh
  adcprep --np $SLURM_NTASKS --prepall
   NEMS.x
  clean_directory
  cd ..
}

clean_directory() {
  rm -rf PE*
  rm -rf partmesh.txt
  rm -rf metis_graph.txt
  rm -rf fort.13
  rm -rf fort.14
  rm -rf fort.15
  rm -rf fort.16
  rm -rf fort.80
  rm -rf fort.68.nc
  rm -rf nems.configure
  rm -rf model_configure
  rm -rf atm_namelist.rc
  rm -rf config.rc
}

main
