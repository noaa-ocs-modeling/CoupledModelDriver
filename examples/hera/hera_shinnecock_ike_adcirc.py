#! /usr/bin/env python

from datetime import datetime, timedelta
from pathlib import Path

from adcircpy import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from coupledmodeldriver.adcirc.adcirc import ADCIRCRunConfiguration
from coupledmodeldriver.job_script import ADCIRCGenerationScript
from coupledmodeldriver.platforms import Platform

# paths to compiled `NEMS.x` and `adcprep`

NEMS_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ALLBIN_INSTALL/NEMS-adcirc_atmesh_ww3data.x'
ADCPREP_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ALLBIN_INSTALL/adcprep'

# directory containing input ADCIRC mesh nodes (`fort.14`) and (optionally) mesh values (`fort.13`)
MESH_DIRECTORY = (
    Path('/scratch2/COASTAL/coastal/save/shared/models') / 'meshes' / 'shinnecock' / 'grid_v1'
)

# directory containing input atmospheric mesh forcings (`wind_atm_fin_ch_time_vec.nc`) and WaveWatch III forcings (`ww3.Constant.20151214_sxy_ike_date.nc`)
FORCINGS_DIRECTORY = (
    Path('/scratch2/COASTAL/coastal/save/shared/models') / 'forcings' / 'shinnecock' / 'ike'
)

# directory to which to write configuration
OUTPUT_DIRECTORY = (
    Path(__file__).parent.parent / 'data' / 'configuration' / 'hera_shinnecock_ike_adcirc'
)

HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'
TPXO_FILENAME = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc'

if __name__ == '__main__':
    platform = Platform.HERA
    adcirc_processors = 11
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    # dictionary defining runs with ADCIRC value perturbations - in this case, a single run with no perturbation
    runs = {f'test_case_1': (None, None)}

    slurm_email_address = 'example@email.gov'

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
        fort13=MESH_DIRECTORY / 'fort.13',
        fort14=MESH_DIRECTORY / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.write_directory(OUTPUT_DIRECTORY, overwrite=False)

    generation_script = ADCIRCGenerationScript()
    generation_script.write(OUTPUT_DIRECTORY, overwrite=True)
