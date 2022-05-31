import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from coupledmodeldriver.client.initialize_adcirc import parse_initialize_adcirc_arguments
from coupledmodeldriver.client.initialize_schism import parse_initialize_schism_arguments
from coupledmodeldriver.platforms import Platform

# noinspection PyUnresolvedReferences
from tests import INPUT_DIRECTORY


ADCIRC_ARGUMENT_TYPES = {
    'platform': Platform,
    'mesh_directory': Path,
    'modeled_start_time': datetime,
    'modeled_duration': timedelta,
    'modeled_timestep': timedelta,
    'nems_interval': (timedelta, type(None)),
    'tidal_spinup_duration': (timedelta, type(None)),
    'modulefile': (Path, type(None)),
    'forcings': list,
    'adcirc_executable': Path,
    'adcprep_executable': Path,
    'aswip_executable': Path,
    'adcirc_processors': int,
    'job_duration': timedelta,
    'output_directory': Path,
    'absolute_paths': bool,
    'overwrite': bool,
    'verbose': bool,
}
SCHISM_ARGUMENT_TYPES = {
    'platform': Platform,
    'mesh_directory': Path,
    'modeled_start_time': datetime,
    'modeled_duration': timedelta,
    'modeled_timestep': timedelta,
    'nems_interval': (timedelta, type(None)),
    'tidal_spinup_duration': (timedelta, type(None)),
    'modulefile': (Path, type(None)),
    'forcings': list,
    'schism_executable': Path,
    'schism_hotstart_combiner': Path,
    'schism_schout_combiner': Path,
    'schism_use_old_io': bool,
    'schism_processors': int,
    'job_duration': timedelta,
    'output_directory': Path,
    'absolute_paths': bool,
    'overwrite': bool,
    'verbose': bool,
}


def cli_test_helper(parse_func, *test_args):
    # Ref: https://stackoverflow.com/questions/18668947/how-do-i-set-sys-argv-so-i-can-unit-test-it
    mock_args = ['prog', *test_args]
    with patch.object(sys, 'argv', mock_args):
        return parse_func()


def test_initialize_adcirc_noargs(capsys):
    try:
        cli_test_helper(parse_initialize_adcirc_arguments)

    except SystemExit as e:
        assert e.code == 2

        out, err = capsys.readouterr()
        assert (
            ''.join(
                [
                    'error: the following arguments are required:',
                    ' --platform, --mesh-directory,',
                    ' --modeled-start-time, --modeled-duration,',
                    ' --modeled-timestep',
                ]
            )
            in err
        )


def test_initialize_adcirc_requied_args():
    results = cli_test_helper(
        parse_initialize_adcirc_arguments,
        '--platform',
        'HERA',
        '--mesh-directory',
        str(INPUT_DIRECTORY / 'meshes/shinnecock'),
        '--modeled-start-time',
        '20180908',
        '--modeled-duration',
        '10:00:00:00',
        '--modeled-timestep',
        '150',
    )

    assert isinstance(results, dict)
    for arg_name, arg_type in ADCIRC_ARGUMENT_TYPES.items():
        assert isinstance(results[arg_name], arg_type)

    assert results['modeled_start_time'] == datetime(2018, 9, 8)
    assert results['modeled_duration'] == timedelta(days=10)
    assert results['modeled_timestep'] == timedelta(seconds=150)


# TODO: Add tests for optional args, e.g. best track nhc-code, etc.


def test_initialize_schism_noargs(capsys):
    try:
        cli_test_helper(parse_initialize_schism_arguments)

    except SystemExit as e:
        assert e.code == 2

        out, err = capsys.readouterr()
        assert (
            ''.join(
                [
                    'error: the following arguments are required:',
                    ' --platform, --mesh-directory,',
                    ' --modeled-start-time, --modeled-duration,',
                    ' --modeled-timestep',
                ]
            )
            in err
        )


def test_initialize_schism_requied_args():
    results = cli_test_helper(
        parse_initialize_schism_arguments,
        '--platform',
        'HERA',
        '--mesh-directory',
        str(INPUT_DIRECTORY / 'meshes/shinnecock'),
        '--modeled-start-time',
        '20180908',
        '--modeled-duration',
        '10:00:00:00',
        '--modeled-timestep',
        '150',
    )

    assert isinstance(results, dict)
    for arg_name, arg_type in SCHISM_ARGUMENT_TYPES.items():
        assert isinstance(results[arg_name], arg_type)

    assert results['modeled_start_time'] == datetime(2018, 9, 8)
    assert results['modeled_duration'] == timedelta(days=10)
    assert results['modeled_timestep'] == timedelta(seconds=150)
