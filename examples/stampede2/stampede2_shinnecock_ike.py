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

MESH_DIRECTORY = Path('/work/07531/zrb/stampede2') / 'meshes' / 'shinnecock' / 'ike' / 'grid_v1'
FORCINGS_DIRECTORY = Path('/work/07531/zrb/stampede2') / 'forcings' / 'shinnecock' / 'ike'
OUTPUT_DIRECTORY = (Path(__file__).parent / '../data') / 'configuration' / 'stampede2_shinnecock_ike'

if __name__ == '__main__':
    runs = {f'test_case_1': (None, None)}

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        end_time=datetime(2008, 8, 23) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(FORCINGS_DIRECTORY / 'wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry(FORCINGS_DIRECTORY / 'ww3.Constant.20151214_sxy_ike_date.nc'),
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
        name='nems_test_shinnecock',
        email_address='zachary.burnett@noaa.gov',
        platform=Platform.STAMPEDE2,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )
