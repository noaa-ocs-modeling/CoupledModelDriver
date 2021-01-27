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

from coupledmodeldriver.adcirc import download_shinnecock_mesh, write_adcirc_configurations
from coupledmodeldriver.job_script import HPC
from coupledmodeldriver.utilities import repository_root


DATA_DIRECTORY = Path('/mnt/c/Users/Saeed.Moghimi/Documents/work/linux_working/00-working/05-nemspy/CoupledModelDriver/examples/data/')
INPUT_DIRECTORY = DATA_DIRECTORY / 'input' / 'wsl'
OUTPUT_DIRECTORY = DATA_DIRECTORY / 'configuration' / 'wsl'

if __name__ == '__main__':
    runs = {f'nems_shinnecock_test': (None, None)}

    if not (INPUT_DIRECTORY / 'fort.14').exists():
        download_shinnecock_mesh(INPUT_DIRECTORY)

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    dir0 = Path('/home/moghimis/linux_working/00-working/05-nemspy/hera_files')

    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        duration=timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(dir0 / 'wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry       (dir0 / 'ww3.Constant.20151214_sxy_ike_date.nc'),
        ocn=ADCIRCEntry(11),
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
