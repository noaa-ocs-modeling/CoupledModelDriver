# CoupledModelDriver

[![tests](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/tests/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Atests)
[![codecov](https://codecov.io/gh/noaa-ocs-modeling/coupledmodeldriver/branch/main/graph/badge.svg?token=4DwZePHp18)](https://codecov.io/gh/noaa-ocs-modeling/coupledmodeldriver)
[![build](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/build/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/CoupledModelDriver)](https://pypi.org/project/CoupledModelDriver)
[![license](https://img.shields.io/github/license/noaa-ocs-modeling/CoupledModelDriver)](https://creativecommons.org/share-your-work/public-domain/cc0)
[![style](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://sourceforge.net/p/oitnb/code)

CoupledModelDriver generates an overlying job submission framework and configuration directories for NEMS-coupled coastal ocean
model ensembles.

```shell
pip install coupledmodeldriver
```

It utilizes [NEMSpy](https://pypi.org/project/nemspy) to generate NEMS configuration files, shares common configurations
between runs, and organizes spinup and mesh partition into separate jobs for dependant submission.

## supported models and platforms

- **models**
    - circulation models
        - ADCIRC (uses [ADCIRCpy](https://pypi.org/project/adcircpy))
    - forcings
        - ATMESH
        - WW3DATA
        - HURDAT best track
        - OWI
- **platforms**
    - local
    - Slurm
        - Hera
        - Stampede2
        - Orion

## organization / responsibility

- Zachary Burnett (**lead**) - zachary.burnett@noaa.gov
- William Pringle - wpringle@anl.gov
- Saeed Moghimi - saeed.moghimi@noaa.gov

## usage example

### 1. generate JSON configuration files

`initialize_adcirc` creates JSON configuration files according to the given parameters. ADCIRC run options that are not exposed
by this command, such as `runs` or `gwce_solution_scheme`, can be specified by directly modifying the JSON files. The following
creates JSON files for coupling `(ATMESH + WW3DATA) -> ADCIRC` over a small Shinnecock Inlet mesh:

```shell
initialize_adcirc \
    --platform HERA \
    --mesh-directory /scratch2/COASTAL/coastal/save/shared/models/meshes/shinnecock/v1.0 \
    --output-directory hera_shinnecock_ike_spinup_tidal_atmesh_ww3data \
    --modeled-start-time 20080823 \
    --modeled-duration 14:06:00:00 \
    --modeled-timestep 00:00:02 \
    --nems-interval 01:00:00 \
    --adcirc-executable /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/NEMS-adcirc-atmesh-ww3data.x \
    --adcirc-processors 40
    --adcprep-executable /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ADCIRC/ALLBIN_INSTALL/adcprep \
    --modulefile /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/modulefiles/envmodules_intel.hera \
    --forcings tidal,atmesh,ww3data \
    --tidal-source TPXO \
    --tidal-path /scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc \
    --tidal-spinup-duration 12:06:00:00 \
    --atmesh-path /scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/wind_atm_fin_ch_time_vec.nc \
    --ww3data-path /scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/ww3.Constant.20151214_sxy_ike_date.nc
```

This will create the directory `hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/` with the following JSON configuration files:

```
📦 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
┣ 📜 configure_adcirc.json
┣ 📜 configure_atmesh.json
┣ 📜 configure_modeldriver.json
┣ 📜 configure_nems.json
┣ 📜 configure_slurm.json
┣ 📜 configure_tidal_forcing.json
┗ 📜 configure_ww3data.json
```

These files contain relevant configuration values for an ADCIRC run. You will likely wish to change these values to alter the
resulting run, before generating the actual model configuration. For instance, NEMS connections and the run sequence need to be
manually specified in `configure_nems.json`.

### 2. generate model configuration files

`generate_adcirc` generates an ADCIRC run configuration (`fort.14`, `fort.15`, etc.) using options read from the JSON
configuration files (generated in the previous step).

```shell
cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
generate_adcirc
```

The resulting configuration will look like this:

```
📦 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
┣ 📜 configure_adcirc.json
┣ 📜 configure_atmesh.json
┣ 📜 configure_modeldriver.json
┣ 📜 configure_nems.json
┣ 📜 configure_slurm.json
┣ 📜 configure_tidal_forcing.json
┣ 📜 configure_ww3data.json
┣ 📂 spinup/
┃  ┣ 📜 fort.13
┃  ┣ 🔗 fort.14 -> ../fort.14
┃  ┣ 📜 fort.15
┃  ┣ 📜 nems.configure
┃  ┣ 📜 model_configure
┃  ┣ 🔗 atm_namelist.rc -> ./model_configure
┃  ┣ 📜 config.rc
┃  ┣ 📜 setup.job
┃  ┗ 📜 adcirc.job
┣ 📂 runs/
┃  ┗ 📂 unperturbed/
┃    ┣ 📜 fort.13
┃    ┣ 🔗 fort.14 -> ../../fort.14
┃    ┣ 📜 fort.15
┃    ┣ 🔗 fort.67.nc -> ../../spinup/fort.67.nc
┃    ┣ 🔗 fort.68.nc -> ../../spinup/fort.68.nc
┃    ┣ 📜 nems.configure
┃    ┣ 📜 model_configure
┃    ┣ 🔗 atm_namelist.rc -> ./model_configure
┃    ┣ 📜 config.rc
┃    ┣ 📜 setup.job
┃    ┗ 📜 adcirc.job
┣ 📜 fort.14
┣ 📜 cleanup.sh
┗ 📜 run_hera.sh
```

### 3. run the model

The previous step will also have generated a script called `./run_hera.sh`. You can run it to submit the model run to the Slurm
job queue:

```shell
./run_hera.sh
``` 

The queue will have the following jobs added:

```
   JOBID CPU NODE DEPENDENCY       NODELIST(REA NAME
20967647 1   1    (null)           (None)       ADCIRC_SETUP_SPINUP
20967648 40  1    afterok:20967647 (Dependency) ADCIRC_COLDSTART_SPINUP
20967649 1   1    (null)           (None)       ADCIRC_SETUP_unperturbed
20967650 42  2    afterok:20967649 (Dependency) ADCIRC_HOTSTART_unperturbed
```

### 4. track model progress

`check_completion` checks the completion status of a running model directory.

```shell
check_completion hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
```

