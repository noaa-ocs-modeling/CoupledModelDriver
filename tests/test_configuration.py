from datetime import datetime, timedelta
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
import wget

from coupledmodeldriver.adcirc import write_adcirc_configurations
from coupledmodeldriver.job_script import Platform

DATA_DIRECTORY = Path(__file__).parent / 'data'


def test_shinnecock_ike():
    test_directory = DATA_DIRECTORY / 'test_shinnecock_configuration'

    input_directory = test_directory / 'input'
    mesh_directory = input_directory / 'mesh'
    forcings_directory = input_directory / 'forcings'

    output_directory = test_directory / 'output'
    reference_directory = test_directory / 'reference'

    tpxo_filename = Path(sys.executable).parent.parent / 'lib/h_tpxo9.v1.nc'
    if not tpxo_filename.exists():
        url = 'https://www.dropbox.com/s/uc44cbo5s2x4n93/h_tpxo9.v1.tar.gz?dl=1'
        extract_download(url, tpxo_filename.parent, ['h_tpxo9.v1.nc'])

    if not (mesh_directory / 'fort.13').exists() or not (mesh_directory / 'fort.14').exists():
        url = 'https://www.dropbox.com/s/1wk91r67cacf132/NetCDF_shinnecock_inlet.tar.bz2?dl=1'
        extract_download(url, mesh_directory, ['fort.13', 'fort.14'])

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
        ocn=ADCIRCEntry(382),
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
        name='test_case_1',
        email_address='zachary.burnett@noaa.gov',
        platform=Platform.LOCAL,
        spinup=timedelta(days=12.5),
        forcings=[tidal_forcing, wind_forcing, wave_forcing],
    )

    check_reference_directory(output_directory, reference_directory)


def extract_download(url: str, directory: PathLike, filenames: [str] = None):
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
            check_reference_directory(test_directory / reference_filename.name, reference_filename)
        else:
            test_filename = test_directory / reference_filename.name
            with open(test_filename) as test_file, open(reference_filename) as reference_file:
                assert test_file.readlines()[1:] == reference_file.readlines()[1:]