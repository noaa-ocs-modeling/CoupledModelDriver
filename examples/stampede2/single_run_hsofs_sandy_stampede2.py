#! /usr/bin/env python

from datetime import datetime, timedelta
from pathlib import Path
import sys

from adcircpy import Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, WaveMeshEntry

sys.path.append(Path(__file__).parent.parent.parent.absolute())

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.job_script import HPC
from coupledmodeldriver.utilities import repository_root

DATA_DIRECTORY = repository_root() / 'examples/data'
INPUT_DIRECTORY = DATA_DIRECTORY / 'input' / 'hsofs'
OUTPUT_DIRECTORY = DATA_DIRECTORY / 'configuration' / 'hsofs'

if __name__ == '__main__':
    runs = {f'nems_hsofs_test': (None, None)}

    fort14_filename = INPUT_DIRECTORY / 'fort.14'
    if not fort14_filename.exists():
        raise RuntimeError(f'file not found at {fort14_filename}')

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2012, 10, 22, 6),
        duration=timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry('/work/07531/zrb/stampede2/forcings/hsofs/SANDY_HWRF_HSOFS_Nov2018.nc'),
        wav=WaveMeshEntry('/work/07531/zrb/stampede2/forcings/hsofs/ww3.HWRF.NOV2018.2012_sxy.nc'),
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
        INPUT_DIRECTORY,
        OUTPUT_DIRECTORY,
        name='nems_hsofs_test',
        email_address='zachary.burnett@noaa.gov',
        platform=HPC.STAMPEDE2,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )
