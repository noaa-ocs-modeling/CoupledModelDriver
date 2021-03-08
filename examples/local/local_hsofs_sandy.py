#! /usr/bin/env python

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

# directory containing input ADCIRC mesh nodes (`fort.14`) and (optionally) mesh values (`fort.13`)
MESH_DIRECTORY = (
    Path(__file__).parent.parent / 'data' / 'input' / 'meshes' / 'hsofs' / 'grid_v1'
)

# directory containing input atmospheric mesh forcings (`wind_atm_fin_ch_time_vec.nc`) and WaveWatch III forcings (`ww3.Constant.20151214_sxy_ike_date.nc`)
FORCINGS_DIRECTORY = (
    Path(__file__).parent.parent / 'data' / 'input' / 'forcings' / 'hsofs' / 'sandy'
)

# directory to which to write configuration
OUTPUT_DIRECTORY = (
    Path(__file__).parent.parent / 'data' / 'configuration' / 'local_hsofs_sandy'
)

if __name__ == '__main__':
    # dictionary defining runs with ADCIRC value perturbations - in this case, a single run with no perturbation
    runs = {f'test_case_1': (None, None)}

    # initialize `nemspy` configuration object with forcing file locations, start and end times,  and processor assignment
    nems = ModelingSystem(
        start_time=datetime(2012, 10, 22, 6),
        end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(FORCINGS_DIRECTORY / 'SANDY_HWRF_HSOFS_Nov2018.nc'),
        wav=WaveMeshEntry(FORCINGS_DIRECTORY / 'ww3.HWRF.NOV2018.2012_sxy.nc'),
        ocn=ADCIRCEntry(382),
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
    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE, resource=None)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(nws=17, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(nrs=5, interval_seconds=3600)

    # send run information to `adcircpy` and write the resulting configuration to output directory
    write_adcirc_configurations(
        nems,
        runs,
        MESH_DIRECTORY,
        OUTPUT_DIRECTORY,
        email_address='example@email.gov',
        platform=Platform.LOCAL,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
        overwrite=True,
    )
