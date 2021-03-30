#! /usr/bin/env python

from datetime import datetime, timedelta
from pathlib import Path

from adcircpy import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, \
    WaveMeshEntry
import numpy

from coupledmodeldriver.adcirc import (
    write_adcirc_configurations,
    write_forcings_json,
    write_required_json,
)
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
    Path(__file__).parent.parent
    / 'data'
    / 'configuration'
    / 'hera_shinnecock_ike_perturbed_mannings_n'
)

HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'
TPXO_FILENAME = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc'

if __name__ == '__main__':
    platform = Platform.HERA
    adcirc_processors = 11
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    # dictionary defining runs with ADCIRC value perturbations - in this case, a range of Manning's N values
    range = [0.016, 0.08]
    mean = numpy.mean(range)
    std = mean / 3
    values = numpy.random.normal(mean, std, 5)
    runs = {
        f'mannings_n_{mannings_n:.3}': (mannings_n, 'mannings_n_at_sea_floor')
        for mannings_n in values
    }

    # initialize `nemspy` configuration object with forcing file locations, start and end times,  and processor assignment
    nems = ModelingSystem(
        start_time=modeled_start_time,
        end_time=modeled_start_time + modeled_duration,
        interval=nems_interval,
        atm=AtmosphericMeshEntry(FORCINGS_DIRECTORY / 'wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry(FORCINGS_DIRECTORY / 'ww3.Constant.20151214_sxy_ike_date.nc'),
        ocn=ADCIRCEntry(adcirc_processors),
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
    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=TPXO_FILENAME)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(nws=17, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(nrs=5, interval_seconds=3600)
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    # generate JSON configuration files for the current run
    write_required_json(
        output_directory=OUTPUT_DIRECTORY,
        fort13_filename=MESH_DIRECTORY / 'fort.13',
        fort14_filename=MESH_DIRECTORY / 'fort.14',
        nems=nems,
        platform=platform,
        nems_executable=NEMS_EXECUTABLE,
        adcprep_executable=ADCPREP_EXECUTABLE,
        tidal_spinup_duration=tidal_spinup_duration,
        runs=runs,
        job_duration=job_duration,
        verbose=True,
    )

    # generate JSON configuration files for the forcings
    write_forcings_json(
        output_directory=OUTPUT_DIRECTORY, forcings=forcings, verbose=True,
    )

    # read JSON configuration files and write the resulting configuration to the output directory
    write_adcirc_configurations(
        output_directory=OUTPUT_DIRECTORY,
        configuration_directory=OUTPUT_DIRECTORY,
        verbose=True,
    )
