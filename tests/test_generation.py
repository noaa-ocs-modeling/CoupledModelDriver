from datetime import datetime, timedelta

from adcircpy.forcing.tides.tides import TidalSource, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.forcing.winds.best_track import BestTrackForcing

from coupledmodeldriver import Platform
from coupledmodeldriver.generate import (
    ADCIRCRunConfiguration,
    generate_adcirc_configuration,
    NEMSADCIRCRunConfiguration,
)
from tests import (
    check_reference_directory,
    INPUT_DIRECTORY,
    OUTPUT_DIRECTORY,
    REFERENCE_DIRECTORY,
)

NEMS_PATH = 'NEMS.x'
ADCPREP_PATH = 'adcprep'


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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    slurm_email_address = 'example@email.gov'

    configuration = ADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=None,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

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

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    configuration = ADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    configuration = ADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.stampede',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    configuration = ADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcirc_executable=INPUT_DIRECTORY / 'bin' / 'padcirc',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

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

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

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

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.stampede',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

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

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=None,
    )

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['ATM -> OCN', 'WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

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

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
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
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'WAV -> OCN',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = BestTrackForcing(storm='ike2008', nws=8, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=None,
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / storm / 'forcings'

    nems_connections = ['WAV -> OCN']
    nems_mediations = None
    nems_sequence = [
        'WAV -> OCN',
        'WAV',
        'OCN',
    ]

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    wind_forcing = BestTrackForcing(storm='ike2008', nws=20, interval_seconds=3600)
    wave_forcing = WaveWatch3DataForcing(
        filename=forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory=mesh_directory,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        nems_executable=INPUT_DIRECTORY / 'bin' / 'NEMS.x',
        adcprep_executable=INPUT_DIRECTORY / 'bin' / 'adcprep',
        aswip_executable=INPUT_DIRECTORY / 'bin' / 'aswip',
        source_filename=INPUT_DIRECTORY / 'modulefiles' / 'envmodules_intel.hera',
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
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
