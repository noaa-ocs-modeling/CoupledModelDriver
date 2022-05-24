# CoupledModelDriver

[![tests](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/tests/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Atests)
[![codecov](https://codecov.io/gh/noaa-ocs-modeling/coupledmodeldriver/branch/main/graph/badge.svg?token=4DwZePHp18)](https://codecov.io/gh/noaa-ocs-modeling/coupledmodeldriver)
[![build](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/build/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/CoupledModelDriver)](https://pypi.org/project/CoupledModelDriver)
[![license](https://img.shields.io/github/license/noaa-ocs-modeling/CoupledModelDriver)](https://creativecommons.org/share-your-work/public-domain/cc0)
[![style](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://sourceforge.net/p/oitnb/code)
[![documentation](https://readthedocs.org/projects/coupledmodeldriver/badge/?version=latest)](https://coupledmodeldriver.readthedocs.io/en/latest/?badge=latest)

CoupledModelDriver generates an overlying job submission framework and configuration directories for NEMS-coupled coastal ocean
model ensembles.

```shell
pip install coupledmodeldriver
```

It utilizes [NEMSpy](https://nemspy.readthedocs.io) to generate NEMS configuration files, shares common configurations between
runs, and organizes spinup and mesh partition into separate jobs for dependant submission.

Documentation can be found at https://coupledmodeldriver.readthedocs.io

## supported models and platforms

- **models**
    - circulation models
        - ADCIRC (uses [ADCIRCpy](https://pypi.org/project/adcircpy))
        - SCHISM (uses [PySCHISM](https://github.com/schism-dev/pyschism))
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

CoupledModelDriver is developed for the COASTAL Act project by the [Coastal Marine Modeling Branch (CMMB)](https://coastaloceanmodels.noaa.gov) of the Office of Coast Survey (OCS), a part of the [National Oceanic and Atmospheric Administration (NOAA)](https://www.noaa.gov), an agency of the United States federal government.

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
    --adcprep-executable /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/adcprep \
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
📂 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
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
📂 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
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
cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
check_completion
```

```json
{
    "hera_shinnecock_ike_spinup_tidal_atmesh_ww3data": {
        "spinup": "running - 15%",
        "runs": "not_started - 0%"
    }
}
```

you can also pass a specific directory (or several directories):

```shell
check_completion spinup
```

```json
{
    "spinup": "running - 27%"
}
```

```shell
cd run_20211027_florence_besttrack_250msubset_quadrature
check_completion runs/*_13
```

```json
{
    "vortex_4_variable_perturbation_13": "completed - 100.0%",
    "vortex_4_variable_quadrature_13": "not_started - 0%"
}
```

if a run has an error, you can pass `--verbose` to see detailed logs:

```shell
check_completion spinup
```

```json
{
    "spinup": "error - 0%"
}
```

```shell
check_completion spinup --verbose
```

```json
{
    "spinup": {
        "status": "error",
        "progress": "0%",
        "error": {
            "ADCIRC_SETUP_SPINUP.err.log": [
                "forrtl: severe (24): end-of-file during read, unit -4, file /proc/92195/fd/0\n",
                "Image              PC                Routine            Line        Source             \n",
                "adcprep            000000000069A72E  Unknown               Unknown  Unknown\n",
                "adcprep            00000000006CBAAF  Unknown               Unknown  Unknown\n",
                "adcprep            000000000050A5CB  openprepfiles_           6996  prep.F\n",
                "adcprep            0000000000507F22  prep13_                   753  prep.F\n",
                "adcprep            000000000042E2E9  prepinput_                717  adcprep.F\n",
                "adcprep            000000000042BCDB  MAIN__                    239  adcprep.F\n",
                "adcprep            000000000040B65E  Unknown               Unknown  Unknown\n",
                "libc-2.17.so       00002AAEC02EB555  __libc_start_main     Unknown  Unknown\n",
                "adcprep            000000000040B569  Unknown               Unknown  Unknown\n",
                "srun: error: h24c51: task 0: Exited with exit code 24\n",
                "srun: launch/slurm: _step_signal: Terminating StepId=25366266.1\n"
            ]
        }
    }
}
```

```shell
check_completion runs
```

```json
{
    "spinup": "failed - 0%"
}
```

```shell
check_completion runs --verbose
```

```json
{
    "runs": {
        "status": "failed",
        "progress": "0%",
        "failed": {
            "fort.16": "ADCIRC output file `fort.16` not found"
        },
        "error": {
            "ADCIRC_SETUP_unperturbed.err.log": [
                "slurmstepd: error: execve(): /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ADCIRC/ALLBIN_INSTALL/adcprep: No such file or directory\n",
                "srun: error: h18c49: task 0: Exited with exit code 2\n",
                "srun: launch/slurm: _step_signal: Terminating StepId=25366268.0\n"
            ]
        }
    }
}
```
