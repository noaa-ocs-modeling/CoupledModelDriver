from datetime import datetime, timedelta
import logging
import os
from os import PathLike
from pathlib import Path
import tarfile

from adcircpy.forcing.tides import Tides
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
import pytest
import wget

from coupledmodeldriver import Platform
from coupledmodeldriver.generate import (
    ADCIRCRunConfiguration,
    NEMSADCIRCRunConfiguration,
    generate_adcirc_configuration,
    generate_nems_adcirc_configuration,
)

NEMS_PATH = 'NEMS.x'
ADCPREP_PATH = 'adcprep'

DATA_DIRECTORY = Path(__file__).parent / 'data'

INPUT_DIRECTORY = DATA_DIRECTORY / 'input'

TPXO_FILENAME = INPUT_DIRECTORY / 'h_tpxo9.v1.nc'

MESH_URLS = {
    'shinnecock': {
        'ike': 'https://www.dropbox.com/s/1wk91r67cacf132/NetCDF_shinnecock_inlet.tar.bz2?dl=1',
    },
}


def test_nems_local_shinnecock_ike():
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

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)
    forcings_directory = input_directory / 'forcings'

    output_directory = (
        Path('.') / 'output' / 'nems' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        Path('.') / 'reference' / 'nems' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    runs = {f'test_case_1': None}

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
        fort13=mesh_directory / 'fort.13',
        fort14=mesh_directory / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
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
    generate_nems_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=DATA_DIRECTORY / output_directory,
        reference_directory=DATA_DIRECTORY / reference_directory,
        skip_lines=1,
    )


def test_nems_hera_shinnecock_ike():
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

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)
    forcings_directory = input_directory / 'forcings'

    output_directory = (
        Path('.') / 'output' / 'nems' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        Path('.') / 'reference' / 'nems' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    runs = {f'test_case_1': None}

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
        fort13=mesh_directory / 'fort.13',
        fort14=mesh_directory / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
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
    generate_nems_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=DATA_DIRECTORY / output_directory,
        reference_directory=DATA_DIRECTORY / reference_directory,
        skip_lines=1,
    )


def test_nems_stampede2_shinnecock_ike():
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

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)
    forcings_directory = input_directory / 'forcings'

    output_directory = (
        Path('.') / 'output' / 'nems' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        Path('.') / 'reference' / 'nems' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    runs = {f'test_case_1': None}

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
        fort13=mesh_directory / 'fort.13',
        fort14=mesh_directory / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        nems_interval=nems_interval,
        nems_connections=nems_connections,
        nems_mediations=nems_mediations,
        nems_sequence=nems_sequence,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
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
    generate_nems_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=DATA_DIRECTORY / output_directory,
        reference_directory=DATA_DIRECTORY / reference_directory,
        skip_lines=1,
    )


def test_adcirc_local_shinnecock_ike():
    platform = Platform.LOCAL
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 11
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)

    output_directory = (
        Path('.') / 'output' / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        Path('.') / 'reference' / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    runs = {f'test_case_1': None}

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    configuration = ADCIRCRunConfiguration(
        fort13=mesh_directory / 'fort.13',
        fort14=mesh_directory / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=DATA_DIRECTORY / output_directory,
        reference_directory=DATA_DIRECTORY / reference_directory,
        skip_lines=1,
    )


def test_adcirc_hera_shinnecock_ike():
    platform = Platform.HERA
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)

    output_directory = (
        Path('.') / 'output' / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        Path('.') / 'reference' / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    runs = {f'test_case_1': None}

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    configuration = ADCIRCRunConfiguration(
        fort13=mesh_directory / 'fort.13',
        fort14=mesh_directory / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=DATA_DIRECTORY / output_directory,
        reference_directory=DATA_DIRECTORY / reference_directory,
        skip_lines=1,
    )


def test_adcirc_stampede2_shinnecock_ike():
    platform = Platform.STAMPEDE2
    mesh = 'shinnecock'
    storm = 'ike'
    adcirc_processors = 15 * platform.value['processors_per_node']
    modeled_start_time = datetime(2008, 8, 23)
    modeled_duration = timedelta(days=14.5)
    modeled_timestep = timedelta(seconds=2)
    tidal_spinup_duration = timedelta(days=12.5)
    job_duration = timedelta(hours=6)

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)

    output_directory = (
        Path('.') / 'output' / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )
    reference_directory = (
        Path('.') / 'reference' / 'adcirc' / f'{platform.name.lower()}_{mesh}_{storm}'
    )

    runs = {f'test_case_1': None}

    slurm_email_address = 'example@email.gov'

    tidal_forcing = Tides(tidal_source=TidalSource.HAMTIDE)
    tidal_forcing.use_all()
    forcings = [tidal_forcing]

    configuration = ADCIRCRunConfiguration(
        fort13=mesh_directory / 'fort.13',
        fort14=mesh_directory / 'fort.14',
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_start_time + modeled_duration,
        modeled_timestep=modeled_timestep,
        tidal_spinup_duration=tidal_spinup_duration,
        platform=platform,
        runs=runs,
        forcings=forcings,
        adcirc_processors=adcirc_processors,
        slurm_partition=None,
        slurm_job_duration=job_duration,
        slurm_email_address=slurm_email_address,
        adcprep_executable=None,
        source_filename=None,
    )

    configuration.write_directory(output_directory, overwrite=True)
    generate_adcirc_configuration(output_directory, overwrite=True)

    check_reference_directory(
        test_directory=DATA_DIRECTORY / output_directory,
        reference_directory=DATA_DIRECTORY / reference_directory,
        skip_lines=1,
    )


@pytest.fixture(scope='session', autouse=False)
def download_tpxo():
    if not TPXO_FILENAME.exists():
        url = 'https://www.dropbox.com/s/uc44cbo5s2x4n93/h_tpxo9.v1.tar.gz?dl=1'
        extract_download(url, TPXO_FILENAME.parent, ['h_tpxo9.v1.nc'])


@pytest.fixture(scope='session', autouse=True)
def data_directory():
    os.chdir(DATA_DIRECTORY)


def download_mesh(
    mesh: str, storm: str, input_directory: PathLike = None, overwrite: bool = False
):
    try:
        url = MESH_URLS[mesh][storm]
    except KeyError:
        raise NotImplementedError(f'no test mesh available for "{mesh} {storm}"')

    if input_directory is None:
        input_directory = INPUT_DIRECTORY / f'{mesh}_{storm}'

    mesh_directory = input_directory / 'mesh'
    if not (mesh_directory / 'fort.14').exists() or overwrite:
        logging.info(f'downloading mesh files to {mesh_directory}')
        extract_download(url, mesh_directory, ['fort.13', 'fort.14'])

    return mesh_directory


def extract_download(
    url: str, directory: PathLike, filenames: [str] = None, overwrite: bool = False
):
    if not isinstance(directory, Path):
        directory = Path(directory)

    if filenames is None:
        filenames = []

    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)

    temporary_filename = directory / 'temp.tar.gz'
    logging.debug(f'downloading {url} -> {temporary_filename}')
    wget.download(url, f'{temporary_filename}')
    logging.debug(f'extracting {temporary_filename} -> {directory}')
    with tarfile.open(temporary_filename) as local_file:
        if len(filenames) > 0:
            for filename in filenames:
                if filename in local_file.getnames():
                    path = directory / filename
                    if not path.exists() or overwrite:
                        if path.exists():
                            os.remove(path)
                        local_file.extract(filename, directory)
        else:
            local_file.extractall(directory)

    os.remove(temporary_filename)


def check_reference_directory(
    test_directory: PathLike, reference_directory: PathLike, skip_lines: int = None
):
    if not isinstance(test_directory, Path):
        test_directory = Path(test_directory)
    if not isinstance(reference_directory, Path):
        reference_directory = Path(reference_directory)
    if skip_lines is None:
        skip_lines = 0

    for reference_filename in reference_directory.iterdir():
        if reference_filename.is_dir():
            check_reference_directory(
                test_directory / reference_filename.name, reference_filename, skip_lines
            )
        else:
            test_filename = test_directory / reference_filename.name
            with open(test_filename) as test_file, open(reference_filename) as reference_file:
                assert (
                    test_file.readlines()[skip_lines:]
                    == reference_file.readlines()[skip_lines:]
                ), f'"{test_filename}" != "{reference_filename}"'
