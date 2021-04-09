from datetime import datetime, timedelta
from pathlib import Path

from adcircpy.forcing.tides import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from coupledmodeldriver import Platform
from coupledmodeldriver.generate import ADCIRCGenerationScript, \
    ADCIRCRunConfiguration

# directory to which to write configuration
OUTPUT_DIRECTORY = Path(__file__).parent / Path(__file__).stem

# start and end times for model
MODELED_START_TIME = datetime(year=2008, month=8, day=23)
MODELED_DURATION = timedelta(days=14.5)
MODELED_TIMESTEP = timedelta(seconds=2)
TIDAL_SPINUP_DURATION = timedelta(days=12.5)

# directories containing forcings and mesh
MESH_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/meshes/hsofs/120m/v3.0_20210401'
FORCINGS_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/hsofs/120m/sandy'
HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'
TPXO_FILENAME = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc'

# platform-specific parameters
PLATFORM = Platform.HERA
ADCIRC_PROCESSORS = 15 * PLATFORM.value['processors_per_node']
ADCIRC_EXECUTABLE = (
    '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ADCIRC/work/padcirc'
)
ADCPREP_EXECUTABLE = (
    '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ADCIRC/work/adcprep'
)
MODULEFILE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera'
SLURM_JOB_DURATION = timedelta(hours=6)

if __name__ == '__main__':
    # initialize `adcircpy` forcing objects
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

    configuration = ADCIRCRunConfiguration(
        mesh_directory=MESH_DIRECTORY,
        modeled_start_time=MODELED_START_TIME,
        modeled_end_time=MODELED_START_TIME + MODELED_DURATION,
        modeled_timestep=MODELED_TIMESTEP,
        tidal_spinup_duration=TIDAL_SPINUP_DURATION,
        platform=PLATFORM,
        runs=None,
        forcings=forcings,
        adcirc_processors=ADCIRC_PROCESSORS,
        slurm_partition=None,
        slurm_job_duration=SLURM_JOB_DURATION,
        slurm_email_address=None,
        adcirc_executable=ADCIRC_EXECUTABLE,
        adcprep_executable=ADCPREP_EXECUTABLE,
        source_filename=MODULEFILE,
    )

    configuration.write_directory(OUTPUT_DIRECTORY, overwrite=False)

    generation_script = ADCIRCGenerationScript()
    generation_script.write(OUTPUT_DIRECTORY / 'generate_adcirc.py', overwrite=True)
