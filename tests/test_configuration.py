from datetime import datetime, timedelta
from pathlib import Path

from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, WaveWatch3MeshEntry
import pytest

from coupledmodeldriver import Platform
from coupledmodeldriver.configure import (
    ATMESHForcingJSON,
    ModelDriverJSON,
    NEMSJSON,
    SlurmJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON
from tests import INPUT_DIRECTORY


def test_update():
    configuration = SlurmJSON(account='coastal', tasks=602, job_duration=timedelta(hours=6))

    configuration.update({'email_address': 'test@email.gov', 'test_entry_1': 'test value 1'})

    configuration['test_entry_2'] = 2

    assert configuration['email_address'] == 'test@email.gov'
    assert configuration['test_entry_1'] == 'test value 1'
    assert configuration['test_entry_2'] == 2
    assert configuration.fields['test_entry_1'] == str
    assert configuration.fields['test_entry_2'] == int


def test_slurm():
    configuration = SlurmJSON(
        account='coastal',
        tasks=602,
        partition=None,
        job_duration=timedelta(hours=6),
        run_directory=None,
        run_name=None,
        email_type=None,
        email_address=None,
        log_filename=None,
        modules=None,
        path_prefix=None,
        extra_commands=None,
        launcher=None,
        nodes=None,
    )

    slurm = configuration.to_adcircpy()

    assert slurm.nprocs == 602


def test_nems():
    model_entries = [
        AtmosphericMeshEntry('Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc'),
        WaveWatch3MeshEntry('ww3.HWRF.NOV2018.2012_sxy.nc'),
        ADCIRCEntry(600),
    ]

    connections = [['ATM', 'OCN'], ['WAV', 'OCN']]
    mediations = None
    sequence = [
        'ATM -> OCN',
        'WAV -> OCN',
        'ATM',
        'WAV',
        'OCN',
    ]

    configuration = NEMSJSON(
        executable_path='NEMS.x',
        modeled_start_time=datetime(2012, 10, 22, 6),
        modeled_end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        interval=timedelta(hours=1),
        connections=connections,
        mediations=mediations,
        sequence=sequence,
    )

    modeling_system = configuration.to_nemspy(model_entries)

    assert modeling_system.sequence == [
        'ATM -> OCN   :remapMethod=redist',
        'WAV -> OCN   :remapMethod=redist',
        'ATM',
        'WAV',
        'OCN',
    ]


def test_adcirc():
    configuration = ADCIRCJSON(
        adcirc_executable_path='adcirc',
        adcprep_executable_path='adcprep',
        modeled_start_time=datetime(2012, 10, 22, 6),
        modeled_end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        modeled_timestep=timedelta(seconds=2),
        fort_13_path=None,
        fort_14_path=INPUT_DIRECTORY / 'shinnecock' / 'mesh' / 'fort.14',
        tidal_spinup_duration=timedelta(days=12.5),
    )

    assert configuration.adcircpy_driver.IM == 511113

    configuration['attributes']['gwce_solution_scheme'] = 'semi-implicit-legacy'

    assert configuration.adcircpy_driver.IM == 511111

    configuration['attributes']['smagorinsky'] = False

    assert configuration.adcircpy_driver.IM == 111111

    configuration['attributes']['gwce_solution_scheme'] = 'explicit'

    assert configuration.adcircpy_driver.IM == 111112

    configuration['attributes']['smagorinsky'] = True

    assert configuration.adcircpy_driver.IM == 511112


def test_tidal():
    configuration = TidalForcingJSON(tidal_source='HAMTIDE', constituents='all')

    assert list(configuration.adcircpy_forcing.active_constituents) == [
        'Q1',
        'O1',
        'P1',
        'K1',
        'N2',
        'M2',
        'S2',
        'K2',
    ]

    configuration['constituents'] = 'major'

    assert list(configuration.adcircpy_forcing.active_constituents) == [
        'Q1',
        'O1',
        'P1',
        'K1',
        'N2',
        'M2',
        'S2',
        'K2',
    ]

    configuration['tidal_source'] = 'TPXO'
    configuration['resource'] = 'nonexistent/path/to/h_tpxo9.nc'

    with pytest.raises((FileNotFoundError, OSError)):
        configuration.adcircpy_forcing

    configuration['resource'] = None
    configuration['constituents'] = ['q1', 'p1', 'm2']

    assert list(configuration.adcircpy_forcing.active_constituents) == ['Q1', 'P1', 'M2']


def test_atmesh():
    configuration = ATMESHForcingJSON(
        resource='Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc',
        nws=17,
        interval=timedelta(hours=1),
    )

    assert configuration.configuration == {
        'nws': 17,
        'interval': timedelta(hours=1),
        'resource': Path('Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc'),
        'processors': 1,
        'nems_parameters': {},
    }


def test_ww3data():
    configuration = WW3DATAForcingJSON(
        resource='ww3.HWRF.NOV2018.2012_sxy.nc', nrs=5, interval=timedelta(hours=1),
    )

    assert configuration.configuration == {
        'nrs': 5,
        'interval': timedelta(hours=1),
        'resource': Path('ww3.HWRF.NOV2018.2012_sxy.nc'),
        'processors': 1,
        'nems_parameters': {},
    }


def test_modeldriver():
    configuration = ModelDriverJSON(platform=Platform.HERA, perturbations=None)

    assert configuration.configuration == {
        'platform': Platform.HERA,
        'perturbations': {'unperturbed': None},
    }
