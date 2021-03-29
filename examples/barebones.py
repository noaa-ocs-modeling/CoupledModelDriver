from datetime import datetime, timedelta
from pathlib import Path

from adcircpy import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, \
    WaveMeshEntry

from coupledmodeldriver.adcirc import write_forcings_json, write_required_json
# paths to compiled `NEMS.x` and `adcprep`
from coupledmodeldriver.platforms import Platform

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
OUTPUT_DIRECTORY = Path(__file__).parent / 'data' / 'configuration' / 'barebones'

HAMTIDE_DIRECTORY = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/hamtide'
TPXO_FILENAME = '/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc'

if __name__ == '__main__':
    platform = Platform.HERA
    adcirc_processors = 15 * platform.value['processors_per_node']
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    # dictionary defining runs with ADCIRC value perturbations - in this case, a single run with no perturbation
    runs = {f'test_case_1': (None, None)}

    # initialize `nemspy` configuration object with forcing file locations, start and end times, and processor assignment
    nems = ModelingSystem(
        executable_path=NEMS_EXECUTABLE,
        start_time=datetime(2012, 10, 22, 6),
        end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(FORCINGS_DIRECTORY /
                                 'Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc'),
        wav=WaveMeshEntry(FORCINGS_DIRECTORY / 'ww3.HWRF.NOV2018.2012_sxy.nc'),
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

    # create forcing objects
    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE, resource=None)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(nws=17, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(nrs=5, interval_seconds=3600)
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    generate_barebones_configuration(
        output_directory=OUTPUT_DIRECTORY,
        fort13_filename=MESH_DIRECTORY / 'fort.13',
        fort14_filename=MESH_DIRECTORY / 'fort.14',
        nems=nems,
        platform=platform,
        nems_executable=NEMS_EXECUTABLE,
        adcprep_executable=ADCPREP_EXECUTABLE,
        tidal_spinup_duration=tidal_spinup_duration,
        forcings=forcings,
        runs=runs,
        job_duration=job_duration,
        verbose=True,
    )



    # create forcing objects
    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE, resource=None)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(nws=17, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(nrs=5, interval_seconds=3600)

    write_forcings_json([tidal_forcing, wind_forcing, wave_forcing])
