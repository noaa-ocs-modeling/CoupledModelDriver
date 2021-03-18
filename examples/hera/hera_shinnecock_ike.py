#! /usr/bin/env python

from datetime import datetime, timedelta
from pathlib import Path
import sys

from adcircpy import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, \
    WaveMeshEntry

sys.path.append((Path(__file__).parent / '..').absolute())

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.job_script import Platform

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
    Path(__file__).parent.parent / 'data' / 'configuration' / 'hera_shinnecock_ike'
)

HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'

if __name__ == '__main__':
    # dictionary defining runs with ADCIRC value perturbations - in this case, a single run with no perturbation
    runs = {f'test_case_1': (None, None)}

    # initialize `nemspy` configuration object with forcing file locations, start and end times,  and processor assignment
    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        end_time=datetime(2008, 8, 23) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(
            filename=FORCINGS_DIRECTORY / 'wind_atm_fin_ch_time_vec.nc', processors=1
        ),
        wav=WaveMeshEntry(
            filename=FORCINGS_DIRECTORY / 'ww3.Constant.20151214_sxy_ike_date.nc', processors=1
        ),
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
    )
