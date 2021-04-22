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

## Usage

Example scripts can be found at `examples/<platform>`

### 1. generate JSON configuration files

The following command creates a configuration for coupling `(ATMESH + WW3DATA) -> ADCIRC` over a small Shinnecock Inlet mesh:

```bash
initialize_adcirc \
    --platform HERA \
    --mesh-directory /scratch2/COASTAL/coastal/save/shared/models/meshes/shinnecock/v1.0 \
    --output-directory hera_shinnecock_ike_spinup_tidal_atmesh_ww3data \
    --modeled-start-time 20080823 \
    --modeled-duration 14:06:00:00 \
    --modeled-timestep 00:00:02 \
    --nems-interval 01:00:00 \
    --tidal-spinup-duration 12:06:00:00 \
    --adcirc-executable /scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/NEMS/exe/NEMS.x \
    --adcprep-executable /scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ADCIRC/work/adcprep \
    --modulefile /scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera \
    --generate-script \
    --forcings tidal,atmesh,ww3data \
    --tidal-source TPXO \
    --tidal-path /scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc \
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
from coupledmodeldriver.generate import (
    NEMSADCIRCRunConfiguration,
)

# directory to which to write configuration
OUTPUT_DIRECTORY = Path(__file__).parent / Path(__file__).stem

# start and end times for model
MODELED_START_TIME = datetime(year=2012, month=10, day=22, hour=6)
MODELED_DURATION = timedelta(days=4, hours=5)
MODELED_TIMESTEP = timedelta(seconds=2)
TIDAL_SPINUP_DURATION = timedelta(days=12.5)
NEMS_INTERVAL = timedelta(hours=1)

# directories containing forcings and mesh
MESH_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/meshes/hsofs/120m/v3.0_20210401'
FORCINGS_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/hsofs/120m/sandy'
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
ADCIRC_PROCESSORS = 15 * PLATFORM.value['processors_per_node']
NEMS_EXECUTABLE = (
    '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/NEMS/exe/NEMS.x'
)
ADCPREP_EXECUTABLE = (
    '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ADCIRC/work/adcprep'
)
MODULEFILE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera'
SLURM_JOB_DURATION = timedelta(hours=6)

if __name__ == '__main__':
    # initialize `adcircpy` forcing objects
    FORCINGS_DIRECTORY = Path(FORCINGS_DIRECTORY)
    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=TPXO_FILENAME)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename=FORCINGS_DIRECTORY / 'Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=FORCINGS_DIRECTORY / 'ww3.HWRF.NOV2018.2012_sxy.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

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
    configuration.write_directory(OUTPUT_DIRECTORY, overwrite=False)
```

Either method will create the directory `hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/` with the following JSON
configuration files:

```
📦 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
┣ ✎ configure_modeldriver.json
┣ ✎ configure_adcirc.json
┣ ✎ configure_nems.json
┣ ✎ configure_slurm.json
┣ ✎ configure_tidal_forcing.json
┣ ✎ configure_atmesh.json
┣ ✎ configure_ww3data.json
```

These files contain relevant configuration values for an ADCIRC run. You will likely wish to change these values to alter the
resulting run, before generating the actual model configuration.

### 2. generate model configuration files

Run the following command to read the JSON configuration and generate the ADCIRC run configuration:

```bash
generate_adcirc
```

The resulting configuration will have the following structure:

```
📦 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
┣ ✎ configure_modeldriver.json
┣ ✎ configure_adcirc.json
┣ ✎ configure_nems.json
┣ ✎ configure_slurm.json
┣ ✎ configure_tidal_forcing.json
┣ ✎ configure_atmesh.json
┣ ✎ configure_ww3data.json
┣ 📂 coldstart/
┃  ┣ 📜 fort.13
┃  ┣ 🔗 fort.14 -> ../fort.14
┃  ┣ 📜 fort.15
┃  ┣ 🔗 nems.configure -> ../nems.configure.coldstart
┃  ┣ 🔗 config.rc -> ../config.rc.coldstart
┃  ┣ 🔗 model_configure -> ../model_configure.coldstart
┃  ┣ 🔗 adcprep.job -> ../job_adcprep_hera.job
┃  ┣ 🔗 adcirc.job -> ../job_adcirc_hera.job.coldstart
┃  ┗ 🔗 setup.sh -> ../setup.sh.coldstart
┣ 📂 runs/
┃  ┗ 📂 run_1/
┃    ┣ 📜 fort.13
┃    ┣ 🔗 fort.14 -> ../../fort.14
┃    ┣ 📜 fort.15
┃    ┣ 🔗 fort.67.nc -> ../../coldstart/fort.67.nc
┃    ┣ 🔗 nems.configure -> ../../nems.configure.hotstart
┃    ┣ 🔗 config.rc -> ../../config.rc.hotstart
┃    ┣ 🔗 model_configure -> ../../model_configure.hotstart
┃    ┣ 🔗 adcprep.job -> ../../job_adcprep_hera.job
┃    ┣ 🔗 adcirc.job -> ../../job_adcirc_hera.job.hotstart
┃    ┗ 🔗 setup.sh -> ../../setup.sh.hotstart
┣ 📜 fort.14
┣ 📜 nems.configure.coldstart
┣ 📜 nems.configure.hotstart
┣ 📜 config.rc.coldstart
┣ 📜 config.rc.hotstart
┣ 📜 model_configure.coldstart
┣ 📜 model_configure.hotstart
┣ 📜 job_adcprep_hera.job
┣ 📜 job_adcirc_hera.job.coldstart
┣ 📜 job_adcirc_hera.job.hotstart
┣ 📜 setup.sh.coldstart
┣ 📜 setup.sh.hotstart
┣ 📜 cleanup.sh
┗  ▶ run_hera.sh
```

### 3. run the model

Run the following to submit the model run to the Slurm job queue:

```bash
sh run_hera.sh
``` 

The queue will have the following jobs added:

```
   JOBID                  NAME CPUS NODE                      DEPENDENCY          SUBMIT_TIME           START_TIME             END_TIME
16368044 ADCIRC_MESH_PARTITION    1    1                          (null)  2021-02-18T19:29:17                  N/A                  N/A
16368045      ADCIRC_COLDSTART   11    1  afterany:16368044(unfulfilled)  2021-02-18T19:29:17                  N/A                  N/A
16368046 ADCIRC_MESH_PARTITION    1    1  afterany:16368045(unfulfilled)  2021-02-18T19:29:17                  N/A                  N/A
16368047       ADCIRC_HOTSTART   13    1  afterany:16368046(unfulfilled)  2021-02-18T19:29:17                  N/A                  N/A
```

## Command-line interface

`coupledmodeldriver` exposes the following CLI commands:

- `initialize_adcirc`
- `generate_adcirc`

### Initialize ADCIRC configuration (`initialize_adcirc`)

`initialize_adcirc` creates JSON configuration files according to the given parameters.

```
usage: initialize_adcirc [-h] --platform PLATFORM --mesh-directory MESH_DIRECTORY --modeled-start-time MODELED_START_TIME
                         --modeled-duration MODELED_DURATION --modeled-timestep MODELED_TIMESTEP [--nems-interval NEMS_INTERVAL]
                         [--modulefile MODULEFILE] [--tidal-spinup-duration TIDAL_SPINUP_DURATION] [--forcings FORCINGS]
                         [--adcirc-executable ADCIRC_EXECUTABLE] [--adcprep-executable ADCPREP_EXECUTABLE]
                         [--adcirc-processors ADCIRC_PROCESSORS] [--job-duration JOB_DURATION] [--output-directory OUTPUT_DIRECTORY]
                         [--generate-script] [--skip-existing]

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
  --tidal-spinup-duration TIDAL_SPINUP_DURATION
                        spinup time for ADCIRC tidal coldstart
  --forcings FORCINGS   comma-separated list of forcings to configure, from ['tidal', 'atmesh', 'besttrack', 'owi', 'ww3data']
  --adcirc-executable ADCIRC_EXECUTABLE
                        filename of compiled `adcirc` or `NEMS.x`
  --adcprep-executable ADCPREP_EXECUTABLE
                        filename of compiled `adcprep`
  --adcirc-processors ADCIRC_PROCESSORS
                        numbers of processors to assign for ADCIRC
  --job-duration JOB_DURATION
                        wall clock time for job
  --output-directory OUTPUT_DIRECTORY
                        directory to which to write configuration files
  --generate-script     write shell script to load configuration
  --skip-existing       skip existing files
```

ADCIRC run options that are not exposed by this command, such as `runs` or `gwce_solution_scheme`, can be specified by directly
modifying the JSON files.

### Generate ADCIRC configuration (`generate_adcirc`)

`generate_adcirc` reads a set of JSON configuration files and generates an ADCIRC run configuration from the options read from
these files.

```
usage: generate_adcirc [-h] [--configuration-directory CONFIGURATION_DIRECTORY] [--output-directory OUTPUT_DIRECTORY]
                       [--skip-existing] [--verbose]

optional arguments:
  -h, --help            show this help message and exit
  --configuration-directory CONFIGURATION_DIRECTORY
                        path containing JSON configuration files
  --output-directory OUTPUT_DIRECTORY
                        path to store generated configuration files
  --skip-existing       skip existing files
  --verbose             show more verbose log messages
```

After this configuration is generated, the model can be started by executing the `run_<platform>.sh` script.
