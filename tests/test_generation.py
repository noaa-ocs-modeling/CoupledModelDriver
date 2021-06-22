from datetime import datetime, timedelta

from adcircpy.forcing.tides import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from coupledmodeldriver import Platform
from coupledmodeldriver.generate import (
    ADCIRCRunConfiguration,
    generate_adcirc_configuration,
    NEMSADCIRCRunConfiguration,
)

# noinspection PyUnresolvedReferences
from tests import (
    check_reference_directory,
    INPUT_DIRECTORY,
    OUTPUT_DIRECTORY,
    REFERENCE_DIRECTORY,
    tpxo_filename,
)

NEMS_PATH = 'NEMS.x'
ADCPREP_PATH = 'adcprep'


def test_nems_adcirc_local_shinnecock_ike(tpxo_filename):
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

    output_directory = (
        OUTPUT_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        REFERENCE_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

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

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        nems_executable=None,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_nems_adcirc_hera_shinnecock_ike(tpxo_filename):
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

    output_directory = (
        OUTPUT_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        REFERENCE_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

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

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        nems_executable=None,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_nems_adcirc_stampede2_shinnecock_ike(tpxo_filename):
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

    output_directory = (
        OUTPUT_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        REFERENCE_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

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

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        nems_executable=None,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_adcirc_local_shinnecock_ike(tpxo_filename):
    platform = Platform.LOCAL
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 11
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    output_directory = OUTPUT_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    reference_directory = (
        REFERENCE_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_adcirc_hera_shinnecock_ike(tpxo_filename):
    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    output_directory = OUTPUT_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    reference_directory = (
        REFERENCE_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_adcirc_stampede2_shinnecock_ike(tpxo_filename):
    platform = Platform.STAMPEDE2
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    output_directory = OUTPUT_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    reference_directory = (
        REFERENCE_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_nems_adcirc_hera_shinnecock_ike_nospinup(tpxo_filename):
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

    output_directory = (
        OUTPUT_DIRECTORY / 'nems_adcirc' / f'{platform.name.lower()}_{mesh}_{storm}_nospinup'
    )
    reference_directory = (
        REFERENCE_DIRECTORY
        / 'nems_adcirc'
        / f'{platform.name.lower()}_{mesh}_{storm}_nospinup'
    )

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

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        nems_executable=None,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )


def test_adcirc_hera_shinnecock_ike_nospinup(tpxo_filename):
    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = None
    job_duration = timedelta(hours=6)

    input_directory = INPUT_DIRECTORY / mesh
    mesh_directory = input_directory / 'mesh'

    output_directory = (
        OUTPUT_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}_nospinup'
    )
    reference_directory = (
        REFERENCE_DIRECTORY / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}_nospinup'
    )

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.TPXO, resource=tpxo_filename)
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
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.relative_to(output_directory)

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=output_directory,
        reference_directory=reference_directory,
        skip_lines={
            'fort.15': [0],
            'config.rc': [0],
            'model_configure': [0],
            'nems.configure': [0],
        },
    )
