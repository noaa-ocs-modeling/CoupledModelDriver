from datetime import datetime, timedelta

from adcircpy.forcing.tides.tides import TidalSource, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.forcing.winds.best_track import BestTrackForcing
import pytest

from coupledmodeldriver import Platform
from coupledmodeldriver.client.initialize_adcirc import initialize_adcirc
from coupledmodeldriver.generate import generate_adcirc_configuration
from tests import (
    check_reference_directory,
    INPUT_DIRECTORY,
    OUTPUT_DIRECTORY,
    REFERENCE_DIRECTORY,
)


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
