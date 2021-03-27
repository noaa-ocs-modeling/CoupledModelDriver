#! /usr/bin/env python

from datetime import datetime, timedelta
from pathlib import Path
import sys

from adcircpy.forcing.tides.tides import TidalSource
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, \
    WaveMeshEntry

from coupledmodeldriver.configuration import (
    ATMESHForcingConfiguration,
    NEMSConfiguration,
    TidalForcingConfiguration,
    WW3DATAForcingConfiguration,
)

sys.path.append((Path(__file__).parent / '..').absolute())

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.platforms import Platform

# paths to compiled `NEMS.x` and `adcprep`
NEMS_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ALLBIN_INSTALL/NEMS-adcirc_atmesh_ww3data.x'
ADCPREP_EXECUTABLE = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ALLBIN_INSTALL/adcprep'

# directory containing input ADCIRC mesh nodes (`fort.14`) and (optionally) mesh values (`fort.13`)
MESH_DIRECTORY = (
    Path('/scratch2/COASTAL/coastal/save/shared/models') / 'meshes' / 'hsofs' / 'grid_v1'
)

# directory containing input atmospheric mesh forcings (`wind_atm_fin_ch_time_vec.nc`) and WaveWatch III forcings (`ww3.Constant.20151214_sxy_ike_date.nc`)
FORCINGS_DIRECTORY = (
    Path('/scratch2/COASTAL/coastal/save/shared/models') / 'forcings' / 'hsofs' / 'sandy'
)

# directory to which to write configuration
OUTPUT_DIRECTORY = Path(__file__).parent.parent / 'data' / 'configuration' / 'hera_hsofs_sandy'

HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'
TPXO_FILENAME = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc'

if __name__ == '__main__':
    platform = Platform.HERA
    adcirc_processors = 15 * platform.value['processors_per_node']

    # dictionary defining runs with ADCIRC value perturbations - in this case, a single run with no perturbation
    runs = {f'test_case_1': (None, None)}

    nems_model_entries = [
        AtmosphericMeshEntry(
            FORCINGS_DIRECTORY / 'Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc'
        ),
        WaveMeshEntry(FORCINGS_DIRECTORY / 'ww3.HWRF.NOV2018.2012_sxy.nc'),
        ADCIRCEntry(adcirc_processors),
    ]

    # initialize `nemspy` configuration object with forcing file locations, start and end times, and processor assignment
    nems = NEMSConfiguration(
        executable_path=NEMS_EXECUTABLE,
        modeled_start_time=datetime(2012, 10, 22, 6),
        modeled_end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        modeled_timestep=timedelta(hours=1),
        models=nems_model_entries,
    )

    # describe connections between coupled components
    nems['connections'] = [('ATM', 'OCN'), ('WAV', 'OCN')]
    nems['sequence'] = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    # initialize forcing conf
    tidal_forcing = TidalForcingConfiguration(
        tidal_source=TidalSource.TPXO, resource=TPXO_FILENAME, constituents='all',
    )
    wind_forcing = ATMESHForcingConfiguration(nws=17, modeled_timestep=timedelta(hours=1))
    wave_forcing = WW3DATAForcingConfiguration(nrs=5, modeled_timestep=timedelta(hours=1))

    # send run information to `adcircpy` and write the resulting configuration to output directory
    write_adcirc_configurations(
        nems,
        runs,
        MESH_DIRECTORY,
        OUTPUT_DIRECTORY,
        nems_executable=NEMS_EXECUTABLE,
        adcprep_executable=ADCPREP_EXECUTABLE,
        platform=platform,
        email_address='example@email.gov',
        wall_clock_time=timedelta(hours=6),
        model_timestep=timedelta(seconds=2),
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
        overwrite=True,
        use_original_mesh=False,
        verbose=True,
    )
