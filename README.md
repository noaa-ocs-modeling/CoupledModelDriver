# CoupledModelDriver

[![tests](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/tests/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Atests)
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
- **platforms**
    - local
    - Slurm
        - Hera
        - Stampede2

## Usage

Example scripts can be found at `examples/<platform>`

### 1. generate configuration directory

The following code (`examples/hera/hera_shinnecock_ike.py`) creates a configuration for coupling `(ATMESH + WW3DATA) -> ADCIRC`
on Hera, over a small Shinnecock Inlet mesh:

```python
from datetime import datetime, timedelta
from pathlib import Path
import sys

from adcircpy import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, WaveMeshEntry

sys.path.append((Path(__file__).parent / '..').absolute())

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.job_script import Platform

# paths to compiled `NEMS.x` and `adcprep`
NEMS_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ALLBIN_INSTALL/NEMS-adcirc_atmesh_ww3data.x'
ADCPREP_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ALLBIN_INSTALL/adcprep'

# directory containing input ADCIRC mesh nodes (`fort.14`) and (optionally) mesh values (`fort.13`)
MESH_DIRECTORY = Path('/scratch2/COASTAL/coastal/save/shared/models') / 'meshes' / 'shinnecock' / 'grid_v1'

# directory containing input atmospheric mesh forcings (`wind_atm_fin_ch_time_vec.nc`) and WaveWatch III forcings (`ww3.Constant.20151214_sxy_ike_date.nc`)
FORCINGS_DIRECTORY = Path('/scratch2/COASTAL/coastal/save/shared/models') / 'forcings' / 'shinnecock' / 'ike'

# directory to which to write configuration
OUTPUT_DIRECTORY = Path(__file__).parent.parent / 'data' / 'configuration' / 'hera_shinnecock_ike'

HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'

# dictionary defining runs with ADCIRC value perturbations - in this case, a single run with no perturbation
runs = {f'test_case_1': (None, None)}

# initialize `nemspy` configuration object with forcing file locations, start and end times,  and processor assignment
nems = ModelingSystem(
    start_time=datetime(2008, 8, 23),
    end_time=datetime(2008, 8, 23) + timedelta(days=14.5),
    interval=timedelta(hours=1),
    atm=AtmosphericMeshEntry(filename=FORCINGS_DIRECTORY / 'wind_atm_fin_ch_time_vec.nc', processors=1),
    wav=WaveMeshEntry(filename=FORCINGS_DIRECTORY / 'ww3.Constant.20151214_sxy_ike_date.nc', processors=1),
    ocn=ADCIRCEntry(processors=11),
)

# describe connections between coupled components
nems.connect('ATM', 'OCN')
nems.connect('WAV', 'OCN')
nems.sequence = [
    'ATM -> OCN',
    'WAV -> OCN',
    'ATM',
    'WAV',
    'OCN',
]

# initialize `adcircpy` forcing objects
tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE, resource=HAMTIDE_DIRECTORY)
tidal_forcing.use_all()
wind_forcing = AtmosphericMeshForcing(nws=17, interval_seconds=3600)
wave_forcing = WaveWatch3DataForcing(nrs=5, interval_seconds=3600)

# send run information to `adcircpy` and write the resulting configuration to output directory
write_adcirc_configurations(
    nems,
    runs,
    MESH_DIRECTORY,
    OUTPUT_DIRECTORY,
    nems_executable=NEMS_EXECUTABLE,
    adcprep_executable=ADCPREP_EXECUTABLE,
    email_address='example@email.gov',
    platform=Platform.HERA,
    spinup=timedelta(days=12.5),
    forcings=[tidal_forcing, wind_forcing, wave_forcing],
    overwrite=True,
    use_original_mesh=False,
    verbose=True,
)
```

This code will generate a directory `hera_shinnecock_ike/` with the following structure:

```
📦 hera_shinnecock_ike/
┣ 📂 coldstart/
┃  ┣ 📜 fort.13
┃  ┣ 📜 fort.14
┃  ┗ 📜 fort.15
┣ 📂 runs/
┃  ┗ 📂 test_case_1/
┃    ┣ 📜 fort.13
┃    ┣ 📜 fort.14
┃    ┗ 📜 fort.15
┣ 📜 nems.configure.coldstart
┣ 📜 nems.configure.hotstart
┣ 📜 config.rc.coldstart
┣ 📜 config.rc.hotstart
┣ 📜 model_configure.coldstart
┣ 📜 model_configure.hotstart
┣ 📜 job_adcprep_hera.job
┣ 📜 job_nems_adcirc_hera.job.coldstart
┣ 📜 job_nems_adcirc_hera.job.hotstart
┣ 📜 setup.sh.coldstart
┣ 📜 setup.sh.hotstart
┣ 📜 setup_hera.sh
┗ 📜 run_hera.sh
```

_**Note:** the required NEMS configuration files (`nems.configure`, `model_configure`) do not yet exist in the run
directories (`coldstart/`, `runs/test_case_1/`). These will be populated in the next step._

### 2. run job submission script `run_<platform>.sh`

Run `run_hera.sh`:

```bash
sh run_hera.sh
``` 

This will first create symbolic links to populate configuration directories (by calling `setup_hera.sh`),

```
📦 hera_shinnecock_ike/
┣ 📂 coldstart/
┃  ┣ 📜 fort.13
┃  ┣ 📜 fort.14
┃  ┣ 📜 fort.15
┃  ┣ 🔗 nems.configure -> ../nems.configure.coldstart
┃  ┣ 🔗 config.rc -> ../config.rc.coldstart
┃  ┣ 🔗 model_configure -> ../model_configure.coldstart
┃  ┣ 🔗 setup.sh -> ../setup.sh.coldstart
┃  ┣ 🔗 hera_adcprep.job -> ../job_adcprep_hera.job
┃  ┗ 🔗 hera_nems_adcirc.job -> ../job_nems_adcirc_hera.job.coldstart
┣ 📂 runs/
┃  ┗ 📂 test_case_1/
┃    ┣ 📜 fort.13
┃    ┣ 📜 fort.14
┃    ┣ 📜 fort.15
┃    ┣ 🔗 fort.67.nc -> ../../coldstart/fort.67.nc
┃    ┣ 🔗 nems.configure -> ../../nems.configure.hotstart
┃    ┣ 🔗 config.rc -> ../../config.rc.hotstart
┃    ┣ 🔗 model_configure -> ../../model_configure.hotstart
┃    ┣ 🔗 setup.sh -> ../../setup.sh.hotstart
┃    ┣ 🔗 hera_adcprep.job -> ../../job_adcprep_hera.job
┃    ┗ 🔗 hera_nems_adcirc.job -> ../../job_nems_adcirc_hera.job.hotstart
┣ 📜 nems.configure.coldstart
┣ 📜 nems.configure.hotstart
┣ 📜 config.rc.coldstart
┣ 📜 config.rc.hotstart
┣ 📜 model_configure.coldstart
┣ 📜 model_configure.hotstart
┣ 📜 job_adcprep_hera.job
┣ 📜 job_nems_adcirc_hera.job.coldstart
┣ 📜 job_nems_adcirc_hera.job.hotstart
┣ 📜 setup.sh.coldstart
┣ 📜 setup.sh.hotstart
┣ 📜 setup_hera.sh
┗ 📜 run_hera.sh
```

and then submit the requested jobs to the queue:

```bash
squeue -u $USER -o "%.8i %.21j %.4C %.4D %.31E %.20V %.20S %.20e"
```

```
   JOBID                  NAME CPUS NODE                      DEPENDENCY          SUBMIT_TIME           START_TIME             END_TIME
16368044 ADCIRC_MESH_PARTITION    1    1                          (null)  2021-02-18T19:29:17                  N/A                  N/A
16368045      ADCIRC_COLDSTART   11    1  afterany:16368044(unfulfilled)  2021-02-18T19:29:17                  N/A                  N/A
16368046 ADCIRC_MESH_PARTITION    1    1  afterany:16368045(unfulfilled)  2021-02-18T19:29:17                  N/A                  N/A
16368047       ADCIRC_HOTSTART   13    1  afterany:16368046(unfulfilled)  2021-02-18T19:29:17                  N/A                  N/A
```
