from datetime import datetime, timedelta
import os
import sys

from pyschism.forcing.bctides.tides import Tides as PySCHISMTides
from pyschism.forcing.bctides.tides import TidalDatabase as PySCHISMTidalDatabase
from pyschism.forcing.bctides.tides import TidalDatabase as PySCHISMTidalDatabase
from pyschism.forcing.nws.best_track import BestTrackForcing as PySCHISMBestTrackForcing
from pyschism.forcing import NWM as PySCHISMNWM
import pytest

from coupledmodeldriver import Platform
from coupledmodeldriver.client.initialize_schism import initialize_schism
from coupledmodeldriver.generate import generate_schism_configuration
from coupledmodeldriver._depend import optional_import

from tests import (
    check_reference_directory,
    INPUT_DIRECTORY,
    OUTPUT_DIRECTORY,
    REFERENCE_DIRECTORY,
)


test_adcirc = False
skip_adcircpy_msg = 'AdcircPy is not available!'
if (adcircpy := optional_import('adcircpy')) is not None:
    test_adcirc = True
    skip_adcircpy_msg = ""

    TidalSource = adcircpy.forcing.tides.TidalSource
    Tides = adcircpy.forcing.tides.tides.Tides
    WaveWatch3DataForcing = adcircpy.forcing.waves.ww3.WaveWatch3DataForcing
    AtmosphericMeshForcing = adcircpy.forcing.winds.atmesh.AtmosphericMeshForcing
    BestTrackForcing = adcircpy.forcing.winds.best_track.BestTrackForcing

    _adc_init = optional_import('coupledmodeldriver.client.initialize_adcirc')
    initialize_adcirc = _adc_init.initialize_adcirc
    _adc_gen = optional_import('coupledmodeldriver.generate.adcirc')
    generate_adcirc_configuration = _adc_gen.generate_adcirc_configuration


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_adcirc'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_adcirc'

    platform = Platform.HERA
    mesh = 'shinnecock'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = None
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=None,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc_nems_atmesh_ww3data():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_adcirc_nems_atmesh_ww3data'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_adcirc_nems_atmesh_ww3data'

    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = None
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    wind_forcing = AtmosphericMeshForcing(
        filename=forcings_directory / 'wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc_tidal():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_adcirc_tidal'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_adcirc_tidal'

    platform = Platform.HERA
    mesh = 'shinnecock'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc_tidal_besttrack_nems_ww3data():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_adcirc_tidal_besttrack_nems_ww3data'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_adcirc_tidal_besttrack_nems_ww3data'

    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 9, 1, 6)
    modeled_duration = timedelta(days=14)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'WAV -> OCN',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = BestTrackForcing(storm='ike2008', nws=8, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
@pytest.mark.disable_socket
def test_hera_adcirc_tidal_besttrack_nems_ww3data_nointernet():
    output_directory = (
        OUTPUT_DIRECTORY / 'test_hera_adcirc_tidal_besttrack_nems_ww3data_nointernet'
    )
    reference_directory = (
        REFERENCE_DIRECTORY / 'test_hera_adcirc_tidal_besttrack_nems_ww3data_nointernet'
    )

    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 9, 1, 6)
    modeled_duration = timedelta(days=14)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'WAV -> OCN',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = BestTrackForcing.from_fort22(
        forcings_directory / 'fort.22', nws=8, interval_seconds=3600
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )

    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc_tidal_besttrack_nems_ww3data_aswip():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_adcirc_tidal_besttrack_nems_ww3data_aswip'
    reference_directory = (
        REFERENCE_DIRECTORY / 'test_hera_adcirc_tidal_besttrack_nems_ww3data_aswip'
    )

    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 9, 1, 6)
    modeled_duration = timedelta(days=14)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'WAV -> OCN',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = BestTrackForcing(storm='ike2008', nws=20, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc_tidal_nems_atmesh_ww3data():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_adcirc_tidal_nems_atmesh_ww3data'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_adcirc_tidal_nems_atmesh_ww3data'

    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename=forcings_directory / 'wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_hera_adcirc_tidal_nems_atmesh_ww3data_perturbed():
    output_directory = (
        OUTPUT_DIRECTORY / 'test_hera_adcirc_tidal_nems_atmesh_ww3data_perturbed'
    )
    reference_directory = (
        REFERENCE_DIRECTORY / 'test_hera_adcirc_tidal_nems_atmesh_ww3data_perturbed'
    )

    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename=forcings_directory / 'wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations={
            'run_1': {'adcirc': {'ICS': 2}, 'tidalforcing': {'constituents': 'all'}},
            'run_2': {
                'adcirc': {'ICS': 22},
                'tidalforcing': {
                    'tidal_source': 'HAMTIDE',
                    'constituents': 'all',
                    'resource': None,
                },
            },
        },
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_local_adcirc_tidal():
    output_directory = OUTPUT_DIRECTORY / 'test_local_adcirc_tidal'
    reference_directory = REFERENCE_DIRECTORY / 'test_local_adcirc_tidal'

    platform = Platform.LOCAL
    mesh = 'shinnecock'
    adcirc_processors = 11
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=None,
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_local_adcirc_tidal_nems_atmesh_ww3data():
    output_directory = OUTPUT_DIRECTORY / 'test_local_adcirc_tidal_nems_atmesh_ww3data'
    reference_directory = REFERENCE_DIRECTORY / 'test_local_adcirc_tidal_nems_atmesh_ww3data'

    platform = Platform.LOCAL
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 11
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename=forcings_directory / 'wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=None,
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_stampede2_adcirc_tidal():
    output_directory = OUTPUT_DIRECTORY / 'test_stampede2_adcirc_tidal'
    reference_directory = REFERENCE_DIRECTORY / 'test_stampede2_adcirc_tidal'

    platform = Platform.STAMPEDE2
    mesh = 'shinnecock'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.stampede',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )

    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(not test_adcirc, reason=skip_adcircpy_msg)
def test_stampede2_adcirc_tidal_nems_atmesh_ww3data():
    output_directory = OUTPUT_DIRECTORY / 'test_stampede2_adcirc_tidal_nems_atmesh_ww3data'
    reference_directory = (
        REFERENCE_DIRECTORY / 'test_stampede2_adcirc_tidal_nems_atmesh_ww3data'
    )

    platform = Platform.STAMPEDE2
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    nems_interval = timedelta(hours=1)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    forcings_directory = INPUT_DIRECTORY / 'forcings' / storm

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename=forcings_directory / 'wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    initialize_adcirc(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.stampede',
        forcings=forcings,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        adcirc_processors=adcirc_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_adcirc_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'atm_namelist.rc': [0],
            'nems.configure': [0],
        },
    )


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='MacOSX issue with pickling local objects used in PySCHISM',
)
def test_hera_schism():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_schism'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_schism'

    platform = Platform.HERA
    mesh = 'shinnecock'
    schism_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = None
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    initialize_schism(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=None,
        schism_executable=INPUT_DIRECTORY / 'bin' / 'pschism-TVD_VL',
        schism_hotstart_combiner=INPUT_DIRECTORY / 'bin' / 'combine_hotstart7',
        schism_processors=schism_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_schism_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={'param.nml': [0], 'model_configure': [0], 'Makefile': [3]},
    )


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='MacOSX issue with pickling local objects used in PySCHISM',
)
def test_hera_schism_tidal():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_schism_tidal'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_schism_tidal'

    platform = Platform.HERA
    mesh = 'shinnecock'
    schism_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    # NOTE: Tide constituent orders from pyschism are not always
    # the same, so generated tide files are NOT compared
    tidal_forcing = PySCHISMTides(
        tidal_database=PySCHISMTidalDatabase.HAMTIDE, constituents=['all']
    )
    forcings = [tidal_forcing]

    initialize_schism(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        schism_executable=INPUT_DIRECTORY / 'bin' / 'pschism-TVD_VL',
        schism_hotstart_combiner=INPUT_DIRECTORY / 'bin' / 'combine_hotstart7',
        schism_processors=schism_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_schism_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={'param.nml': [0], 'model_configure': [0], 'Makefile': [3]},
    )


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='MacOSX issue with pickling local objects used in PySCHISM',
)
@pytest.mark.disable_socket
def test_hera_schism_besttrack_fromfile():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_schism_besttrack_fromfile'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_schism_besttrack_fromfile'

    platform = Platform.HERA
    mesh = 'shinnecock'
    schism_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 9, 1, 6)
    modeled_duration = timedelta(days=14)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = None
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh
    nhc_bdeck_path = INPUT_DIRECTORY / 'forcings' / 'ike' / 'fort.22'

    wind_forcing = PySCHISMBestTrackForcing.from_nhc_bdeck(nhc_bdeck=nhc_bdeck_path)
    forcings = [wind_forcing]

    initialize_schism(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        schism_executable=INPUT_DIRECTORY / 'bin' / 'pschism-TVD_VL',
        schism_hotstart_combiner=INPUT_DIRECTORY / 'bin' / 'combine_hotstart7',
        schism_processors=schism_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_schism_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={'param.nml': [0], 'model_configure': [0], 'Makefile': [3]},
    )


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='MacOSX issue with pickling local objects used in PySCHISM',
)
def test_hera_schism_besttrack_fromnhccode():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_schism_besttrack_fromnhccode'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_schism_besttrack_fromnhccode'

    platform = Platform.HERA
    mesh = 'shinnecock'
    schism_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 9, 1, 6)
    modeled_duration = timedelta(days=14)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = None
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    wind_forcing = PySCHISMBestTrackForcing(storm='IKE2008')
    forcings = [wind_forcing]

    initialize_schism(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        schism_executable=INPUT_DIRECTORY / 'bin' / 'pschism-TVD_VL',
        schism_hotstart_combiner=INPUT_DIRECTORY / 'bin' / 'combine_hotstart7',
        schism_processors=schism_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_schism_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={'param.nml': [0], 'model_configure': [0], 'Makefile': [3]},
    )


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='MacOSX issue with pickling local objects used in PySCHISM',
)
def test_hera_schism_nwm():
    output_directory = OUTPUT_DIRECTORY / 'test_hera_schism_nwm'
    reference_directory = REFERENCE_DIRECTORY / 'test_hera_schism_nwm'

    platform = Platform.HERA
    mesh = 'shinnecock_w_floodplain'
    schism_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=2)
    modeled_timestep = timedelta(seconds=150)
    tidal_spinup_duration = None
    job_duration = timedelta(hours=6)

    mesh_directory = INPUT_DIRECTORY / 'meshes' / mesh

    nwm = PySCHISMNWM()
    forcings = [nwm]

    initialize_schism(
        platform=platform,
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_duration=modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        perturbations=None,
        nems_interval=None,
        nems_connections=None,
        nems_mediations=None,
        nems_sequence=None,
        modulefile=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
        forcings=forcings,
        schism_executable=INPUT_DIRECTORY / 'bin' / 'pschism-TVD_VL',
        schism_hotstart_combiner=INPUT_DIRECTORY / 'bin' / 'combine_hotstart7',
        schism_processors=schism_processors,
        job_duration=job_duration,
        output_directory=output_directory,
        absolute_paths=False,
        overwrite=True,
        verbose=False,
    )
    generate_schism_configuration(output_directory, relative_paths=True, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={'param.nml': [0], 'model_configure': [0], 'Makefile': [3]},
    )
