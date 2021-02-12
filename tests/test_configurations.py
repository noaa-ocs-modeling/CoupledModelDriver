from datetime import datetime, timedelta
import logging
import os
from os import PathLike
from pathlib import Path
import sys
import tarfile

from adcircpy import Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, WaveMeshEntry
import pytest
import wget

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.job_script import Platform

DATA_DIRECTORY = Path(__file__).parent / 'data'

INPUT_DIRECTORY = DATA_DIRECTORY / 'input'

MESH_URLS = {
    'shinnecock': {
        'ike': 'https://www.dropbox.com/s/1wk91r67cacf132/NetCDF_shinnecock_inlet.tar.bz2?dl=1',
    },
}


def test_local_shinnecock_ike():
    platform = 'local'
    mesh = 'shinnecock'
    storm = 'ike'

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)
    forcings_directory = input_directory / 'forcings'

    output_directory = Path('.') / 'output' / f'{platform}_{mesh}_{storm}'
    reference_directory = Path('.') / 'reference' / f'{platform}_{mesh}_{storm}'

    runs = {f'test_case_1': (None, None)}

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        end_time=datetime(2008, 8, 23) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(forcings_directory / 'wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry(forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc'),
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
        mesh_directory,
        output_directory,
        email_address='example@email.gov',
        platform=Platform.LOCAL,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )

    check_reference_directory(DATA_DIRECTORY / output_directory, DATA_DIRECTORY / reference_directory)


def test_hera_shinnecock_ike():
    platform = 'hera'
    mesh = 'shinnecock'
    storm = 'ike'

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)
    forcings_directory = input_directory / 'forcings'

    output_directory = Path('.') / 'output' / f'{platform}_{mesh}_{storm}'
    reference_directory = Path('.') / 'reference' / f'{platform}_{mesh}_{storm}'

    runs = {f'test_case_1': (None, None)}

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        end_time=datetime(2008, 8, 23) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(forcings_directory / 'wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry(forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc'),
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
        mesh_directory,
        output_directory,
        email_address='example@email.gov',
        platform=Platform.HERA,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )

    check_reference_directory(DATA_DIRECTORY / output_directory, DATA_DIRECTORY / reference_directory)


def test_stampede2_shinnecock_ike():
    platform = 'stampede2'
    mesh = 'shinnecock'
    storm = 'ike'

    input_directory = Path('.') / 'input' / f'{mesh}_{storm}'
    mesh_directory = download_mesh(mesh, storm, input_directory)
    forcings_directory = input_directory / 'forcings'

    output_directory = Path('.') / 'output' / f'{platform}_{mesh}_{storm}'
    reference_directory = Path('.') / 'reference' / f'{platform}_{mesh}_{storm}'

    runs = {f'test_case_1': (None, None)}

    # init tidal forcing and setup requests
    tidal_forcing = Tides()
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(17, 3600)
    wave_forcing = WaveWatch3DataForcing(5, 3600)

    nems = ModelingSystem(
        start_time=datetime(2008, 8, 23),
        end_time=datetime(2008, 8, 23) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        atm=AtmosphericMeshEntry(forcings_directory / 'wind_atm_fin_ch_time_vec.nc'),
        wav=WaveMeshEntry(forcings_directory / 'ww3.Constant.20151214_sxy_ike_date.nc'),
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
        mesh_directory,
        output_directory,
        email_address='example@email.gov',
        platform=Platform.STAMPEDE2,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )

    check_reference_directory(DATA_DIRECTORY / output_directory, DATA_DIRECTORY / reference_directory)


@pytest.fixture(scope='session', autouse=True)
def tpxo():
    tpxo_filename = Path(sys.executable).parent.parent / 'lib/h_tpxo9.v1.nc'
    if not tpxo_filename.exists():
        url = 'https://www.dropbox.com/s/uc44cbo5s2x4n93/h_tpxo9.v1.tar.gz?dl=1'
        extract_download(url, tpxo_filename.parent, ['h_tpxo9.v1.nc'])


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
    wget.download(url, str(temporary_filename))
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


def check_reference_directory(test_directory: PathLike, reference_directory: PathLike):
    if not isinstance(test_directory, Path):
        test_directory = Path(test_directory)
    if not isinstance(reference_directory, Path):
        reference_directory = Path(reference_directory)

    for reference_filename in reference_directory.iterdir():
        if reference_filename.is_dir():
            check_reference_directory(
                test_directory / reference_filename.name, reference_filename
            )
        else:
            test_filename = test_directory / reference_filename.name
            with open(test_filename) as test_file, open(reference_filename) as reference_file:
                assert test_file.readlines()[1:] == reference_file.readlines()[1:]
