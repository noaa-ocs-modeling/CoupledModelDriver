#! /usr/bin/env python

from datetime import datetime, timedelta
from pathlib import Path
import sys

from adcircpy import Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, WaveMeshEntry

sys.path.append((Path(__file__).parent / '..').absolute())

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.job_script import Platform

MESH_DIRECTORY = (
    (Path(__file__).parent / '../data') / 'input' / 'meshes' / 'hsofs' / 'irma' / 'grid_v1'
)
FORCINGS_DIRECTORY = (
    (Path(__file__).parent / '../data') / 'input' / 'forcings' / 'hsofs' / 'irma'
)
OUTPUT_DIRECTORY = (Path(__file__).parent / '../data') / 'configuration' / 'local_hsofs_irma'

if __name__ == '__main__':
    runs = {f'test_case_1': (None, None)}

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2017, 9, 5),
        end_time=datetime(2017, 9, 5) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(
            FORCINGS_DIRECTORY / 'Wind_HWRF_IRMA_Nov2018_ExtendedSmoothT.nc'
        ),
        wav=WaveMeshEntry(FORCINGS_DIRECTORY / 'ww3.HWRF.NOV2018.2017_sxy.nc'),
        ocn=ADCIRCEntry(382),
    )

    nems.connect('ATM', 'OCN')
    nems.connect('WAV', 'OCN')
    nems.sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    write_adcirc_configurations(
        nems,
        runs,
        MESH_DIRECTORY,
        OUTPUT_DIRECTORY,
        email_address='example@email.gov',
        platform=Platform.LOCAL,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )
