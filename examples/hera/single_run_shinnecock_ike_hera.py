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
INPUT_DIRECTORY = DATA_DIRECTORY / 'input' / 'hera'
OUTPUT_DIRECTORY = DATA_DIRECTORY / 'configuration' / 'hera'

if __name__ == '__main__':
    runs = {f'nems_shinnecock_test': (None, None)}

    fort14_filename = INPUT_DIRECTORY / 'fort.14'
    if not fort14_filename.exists():
        raise RuntimeError(f'file not found at {fort14_filename}')

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        duration=timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry('/scratch2/COASTAL/coastal/save/Zachary.Burnett/forcings/'
                                 'shinnecock/ike/wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry('/scratch2/COASTAL/coastal/save/Zachary.Burnett/forcings/'
                          'shinnecock/ike/ww3.Constant.20151214_sxy_ike_date.nc'),
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
        name='nems_shinnecock_test',
        email_address='zachary.burnett@noaa.gov',
        platform=HPC.HERA,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )
