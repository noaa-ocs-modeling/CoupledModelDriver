# CoupledModelDriver

[![tests](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/tests/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Atests)
[![codecov](https://codecov.io/gh/noaa-ocs-modeling/coupledmodeldriver/branch/main/graph/badge.svg?token=4DwZePHp18)](https://codecov.io/gh/noaa-ocs-modeling/coupledmodeldriver)
[![build](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/build/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/CoupledModelDriver)](https://pypi.org/project/CoupledModelDriver)
[![license](https://img.shields.io/github/license/noaa-ocs-modeling/CoupledModelDriver)](https://creativecommons.org/share-your-work/public-domain/cc0)
[![style](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://sourceforge.net/p/oitnb/code)

`coupledmodeldriver` generates an overlying job submission framework and configuration directories for NEMS-coupled coastal
ocean model ensembles.

It utilizes [`nemspy`](https://pypi.org/project/nemspy) to generate NEMS configuration files, shares common configurations
between runs, and organizes spinup and mesh partition into separate jobs for dependant submission.

## Supported models and platforms

- **models**
    - circulation models
        - ADCIRC (uses [`adcircpy`](https://pypi.org/project/adcircpy))
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

## Organization / Responsibility
- Zachary Burnett (**lead**) - zachary.burnett@noaa.gov
- William Pringle - wpringle@anl.gov
- Saeed Moghimi - saeed.moghimi@noaa.gov

## Usage

### 1. generate JSON configuration files

`initialize_adcirc` creates JSON configuration files according to the given parameters:

```
usage: initialize_adcirc [-h] --platform PLATFORM --mesh-directory MESH_DIRECTORY --modeled-start-time MODELED_START_TIME
                         --modeled-duration MODELED_DURATION --modeled-timestep MODELED_TIMESTEP
                         [--nems-interval NEMS_INTERVAL] [--modulefile MODULEFILE] [--forcings FORCINGS]
                         [--adcirc-executable ADCIRC_EXECUTABLE] [--adcprep-executable ADCPREP_EXECUTABLE]
                         [--aswip-executable ASWIP_EXECUTABLE] [--adcirc-processors ADCIRC_PROCESSORS]
                         [--job-duration JOB_DURATION] [--output-directory OUTPUT_DIRECTORY] [--skip-existing]
                         [--absolute-paths] [--verbose]

optional arguments:
  -h, --help            show this help message and exit
  --platform PLATFORM   HPC platform for which to configure
  --mesh-directory MESH_DIRECTORY
                        path to input mesh (`fort.13`, `fort.14`)
  --modeled-start-time MODELED_START_TIME
                        start time within the modeled system
  --modeled-duration MODELED_DURATION
                        end time within the modeled system
  --modeled-timestep MODELED_TIMESTEP
                        time interval within the modeled system
  --nems-interval NEMS_INTERVAL
                        main loop interval of NEMS run
  --modulefile MODULEFILE
                        path to module file to `source`
  --forcings FORCINGS   comma-separated list of forcings to configure, from ['tidal', 'atmesh', 'besttrack', 'owi',
                        'ww3data']
  --adcirc-executable ADCIRC_EXECUTABLE
                        filename of compiled `adcirc` or `NEMS.x`
  --adcprep-executable ADCPREP_EXECUTABLE
                        filename of compiled `adcprep`
  --aswip-executable ASWIP_EXECUTABLE
                        filename of compiled `aswip`
  --adcirc-processors ADCIRC_PROCESSORS
                        numbers of processors to assign for ADCIRC
  --job-duration JOB_DURATION
                        wall clock time for job
  --output-directory OUTPUT_DIRECTORY
                        directory to which to write configuration files (defaults to `.`)
  --skip-existing       skip existing files
  --absolute-paths      write paths as absolute in configuration
  --verbose             show more verbose log messages
```

ADCIRC run options that are not exposed by this command, such as `runs` or `gwce_solution_scheme`, can be specified by directly
modifying the JSON files.

The following command creates JSON files for coupling `(ATMESH + WW3DATA) -> ADCIRC` over a small Shinnecock Inlet mesh:

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

Alternatively, the following Python code creates the same configuration:

```python
from datetime import datetime, timedelta
from pathlib import Path

from adcircpy.forcing.tides import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from coupledmodeldriver import Platform
from coupledmodeldriver.generate import NEMSADCIRCRunConfiguration

# directory to which to write configuration
OUTPUT_DIRECTORY = 'hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/'

# start and end times for model
MODELED_START_TIME = datetime(year=2008, month=8, day=23)
MODELED_DURATION = timedelta(days=14.5)
MODELED_TIMESTEP = timedelta(seconds=2)
TIDAL_SPINUP_DURATION = timedelta(days=12.5)
NEMS_INTERVAL = timedelta(hours=1)

# directories containing forcings and mesh
MESH_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/meshes/shinnecock/v1.0'
FORCINGS_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike'
HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'
TPXO_FILENAME = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc'

# connections between coupled components
NEMS_CONNECTIONS = ['ATM -> OCN', 'WAV -> OCN']
NEMS_SEQUENCE = [
    'ATM -> OCN',
    'WAV -> OCN',
    'ATM',
    'WAV',
    'OCN',
]

# platform-specific parameters
PLATFORM = Platform.HERA
ADCIRC_PROCESSORS = 1 * PLATFORM.value['processors_per_node']
NEMS_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/NEMS-adcirc-atmesh-ww3data.x'
ADCPREP_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/adcprep'
MODULEFILE = '/scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/modulefiles/envmodules_intel.hera'
SLURM_JOB_DURATION = timedelta(hours=6)

if __name__ == '__main__':
    # initialize `adcircpy` forcing objects
    FORCINGS_DIRECTORY = Path(FORCINGS_DIRECTORY)
    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=TPXO_FILENAME)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename=FORCINGS_DIRECTORY / 'wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=FORCINGS_DIRECTORY / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    # initialize configuration object
    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=MESH_DIRECTORY,
        modeled_start_time=MODELED_START_TIME,
        modeled_end_time=MODELED_START_TIME + MODELED_DURATION,
        modeled_timestep=MODELED_TIMESTEP,
        nems_interval=NEMS_INTERVAL,
        nems_connections=NEMS_CONNECTIONS,
        nems_mediations=None,
        nems_sequence=NEMS_SEQUENCE,
        tidal_spinup_duration=TIDAL_SPINUP_DURATION,
        platform=PLATFORM,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=ADCIRC_PROCESSORS,
        slurm_partition=None,
        slurm_job_duration=SLURM_JOB_DURATION,
        slurm_email_address=None,
        nems_executable=NEMS_EXECUTABLE,
        adcprep_executable=ADCPREP_EXECUTABLE,
        source_filename=MODULEFILE,
    )

    # write configuration to `*.json` files
    configuration.write_directory(OUTPUT_DIRECTORY, overwrite=False)
```

Either method will create the directory `hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/` with the following JSON
configuration files:

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
resulting run, before generating the actual model configuration.

### 2. generate model configuration files

`generate_adcirc` reads a set of JSON configuration files and generates an ADCIRC run configuration from the options read from
these files:

```
usage: generate_adcirc [-h] [--configuration-directory CONFIGURATION_DIRECTORY] [--output-directory OUTPUT_DIRECTORY] [--relative-paths] [--skip-existing] [--verbose]

optional arguments:
  -h, --help            show this help message and exit
  --configuration-directory CONFIGURATION_DIRECTORY
                        path containing JSON configuration files
  --output-directory OUTPUT_DIRECTORY
                        path to store generated configuration files
  --relative-paths      use relative paths in output configuration
  --skip-existing       skip existing files
  --verbose             show more verbose log messages
```

```shell
cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
generate_adcirc
```

The resulting configuration will have the following structure:

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

Run the following to submit the model run to the Slurm job queue:

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

```
usage: check_completion [-h] [--model MODEL] [--verbose] [directory ...]

positional arguments:
  directory      directory containing model run configuration

optional arguments:
  -h, --help     show this help message and exit
  --model MODEL  model that is running, one of: `ADCIRC`
  --verbose      list all errors and problems with runs
```

```shell
check_completion hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
```
