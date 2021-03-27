from datetime import datetime, timedelta
from pathlib import Path

from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, \
    WaveMeshEntry
import pytest

from coupledmodeldriver.configuration import (
    ADCIRCConfiguration,
    ATMESHForcingConfiguration,
    CoupledModelDriverConfiguration,
    NEMSConfiguration,
    SlurmConfiguration,
    TidalForcingConfiguration,
    WW3DATAForcingConfiguration,
)
from coupledmodeldriver.platforms import Platform


def test_update():
    configuration = SlurmConfiguration(
        account='coastal', tasks=602, job_duration=timedelta(hours=6),
    )

    configuration.update(
        {'email_address': 'test@email.gov', 'test_entry_1': 'test value 1', }
    )

    configuration['test_entry_2'] = 2

    assert configuration['email_address'] == 'test@email.gov'
    assert configuration['test_entry_1'] == 'test value 1'
    assert configuration['test_entry_2'] == 2
    assert configuration.fields['test_entry_1'] == str
    assert configuration.fields['test_entry_2'] == int


def test_slurm():
    configuration = SlurmConfiguration(
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

    slurm = configuration.slurm_configuration

    assert slurm.nprocs == 602


def test_nems():
    model_entries = [
        AtmosphericMeshEntry('Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc'),
        WaveMeshEntry('ww3.HWRF.NOV2018.2012_sxy.nc'),
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

    configuration = NEMSConfiguration(
        executable_path='NEMS.x',
        modeled_start_time=datetime(2012, 10, 22, 6),
        modeled_end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        modeled_timestep=timedelta(hours=1),
        models=model_entries,
        connections=connections,
        mediations=mediations,
        sequence=sequence,
    )

    modeling_system = configuration.modeling_system

    assert modeling_system.sequence == [
        'ATM -> OCN   :remapMethod=redist',
        'WAV -> OCN   :remapMethod=redist',
        'ATM',
        'WAV',
        'OCN',
    ]


def test_adcirc():
    configuration = ADCIRCConfiguration(
        adcprep_executable_path='adcprep',
        modeled_start_time=datetime(2012, 10, 22, 6),
        modeled_end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
        modeled_timestep=timedelta(seconds=2),
        fort_13_path=None,
        fort_14_path='tests/data/input/shinnecock_ike/mesh/fort.14',
        tidal_spinup_duration=timedelta(days=12.5),
    )

    assert configuration.driver.IM == 511113

    configuration['gwce_solution_scheme'] = 'semi-implicit-legacy'
    configuration['use_smagorinsky'] = False

    assert configuration.driver.IM == 111111


def test_tidal():
    configuration = TidalForcingConfiguration(tidal_source='HAMTIDE', constituents='all', )

    assert list(configuration.forcing.active_constituents) == [
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

    assert list(configuration.forcing.active_constituents) == [
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
    configuration['resource'] = 'nonesistant/path/to/h_tpxo9.nc'

    with pytest.raises(FileNotFoundError):
        configuration.forcing

    configuration['tidal_source'] = 'HAMTIDE'
    configuration['resource'] = None
    configuration['constituents'] = ['q1', 'p1', 'm2']

    assert list(configuration.forcing.active_constituents) == ['Q1', 'P1', 'M2']


def test_atmesh():
    configuration = ATMESHForcingConfiguration(
        resource='Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc',
        nws=17,
        modeled_timestep=timedelta(hours=1),
    )

    assert configuration.configuration == {
        'name': 'ATMESH',
        'NWS': 17,
        'modeled_timestep': timedelta(hours=1),
        'resource': Path('Wind_HWRF_SANDY_Nov2018_ExtendedSmoothT.nc'),
    }


def test_ww3data():
    configuration = WW3DATAForcingConfiguration(
        resource='ww3.HWRF.NOV2018.2012_sxy.nc', nrs=5, modeled_timestep=timedelta(hours=1),
    )

    assert configuration.configuration == {
        'name': 'WW3DATA',
        'NRS': 5,
        'modeled_timestep': timedelta(hours=1),
        'resource': Path('ww3.HWRF.NOV2018.2012_sxy.nc'),
    }


def test_coupledmodeldriver():
    configuration = CoupledModelDriverConfiguration(
        platform=Platform.HERA,
        output_directory='.',
        models=[],
        runs={'test_case_1': (None, None)},
        verbose=False,
    )

    assert configuration.configuration == {
        'name': 'CoupledModelDriver',
        'platform': Platform.HERA,
        'output_directory': Path('.'),
        'models': [],
        'runs': {'test_case_1': (None, None)},
        'verbose': False,
    }
