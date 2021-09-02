from enum import Enum
from glob import glob
import os
from os import PathLike
from pathlib import Path
import re
from typing import Dict, Iterable, Union

from file_read_backwards import FileReadBackwards

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON


class CompletionStatus(Enum):
    NOT_CONFIGURED = 'not_configured'
    NOT_STARTED = 'not_started'
    IN_SETUP = 'in_setup'
    FAILED = 'failed'
    ERROR = 'error'
    RUNNING = 'running'
    COMPLETED = 'completed'


def tail(file, lines: int = 20) -> [str]:
    """ https://stackoverflow.com/a/136368 """

    total_lines_wanted = lines

    block_size = 1024
    file.seek(0, 2)
    block_end_byte = file.tell()
    lines_to_go = total_lines_wanted
    block_number = -1
    blocks = []
    while lines_to_go > 0 and block_end_byte > 0:
        if block_end_byte - block_size > 0:
            file.seek(block_number * block_size, 2)
            blocks.append(file.read(block_size))
        else:
            file.seek(0, 0)
            blocks.append(file.read(block_end_byte))
        lines_found = blocks[-1].count(b'\n')
        lines_to_go -= lines_found
        block_end_byte -= block_size
        block_number -= 1
    all_read_text = b''.join(reversed(blocks))
    return [line.decode() for line in all_read_text.splitlines()[-total_lines_wanted:]]


def is_adcirc_run_directory(directory: PathLike = None) -> bool:
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    required_files = ['fort.14', 'fort.15']
    nonexistant_files = [
        filename for filename in required_files if not (directory / filename).exists()
    ]

    return len(nonexistant_files) == 0


def collect_adcirc_errors(directory: PathLike = None) -> {str: Union[str, Dict[str, str]]}:
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    not_configured = {}
    not_started = {}
    failures = {}
    errors = {}
    running = {}

    if not is_adcirc_run_directory(directory):
        not_configured['none'] = f'not an ADCIRC run directory'

    adcirc_output_log_filename = directory / 'fort.16'
    slurm_error_log_pattern = directory / 'ADCIRC_*_*.err.log'
    slurm_out_log_pattern = directory / 'ADCIRC_*_*.out.log'
    esmf_log_pattern = directory / 'PET*.ESMF_LogFile'
    output_netcdf_pattern = directory / 'fort.*.nc'

    completion_percentage = 0

    if not adcirc_output_log_filename.exists():
        not_started[
            adcirc_output_log_filename.name
        ] = f'ADCIRC output file `fort.16` was not found at {adcirc_output_log_filename}'

    slurm_error_log_filenames = [
        Path(filename) for filename in glob(str(slurm_error_log_pattern))
    ]
    if len(slurm_error_log_filenames) > 0:
        for filename in slurm_error_log_filenames:
            with open(filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) > 0:
                    if filename.name not in errors:
                        errors[filename.name] = []
                    errors[filename.name].extend(lines)
    else:
        not_started[
            slurm_error_log_pattern.name
        ] = f'no Slurm error log files found with pattern `{os.path.relpath(slurm_error_log_pattern, directory)}`'

    slurm_output_log_filenames = [
        Path(filename) for filename in glob(str(slurm_out_log_pattern))
    ]
    if len(slurm_output_log_filenames) > 0:
        error_pattern = re.compile('error', re.IGNORECASE)
        for filename in slurm_output_log_filenames:
            with open(filename, 'rb') as log_file:
                lines = tail(log_file, lines=100)
                percentages = re.findall('[0-9|.]+% COMPLETE', '\n'.join(lines))

                if len(percentages) > 0:
                    completion_percentage = float(percentages[-1].split('%')[0])

                for line in lines:
                    if re.match(error_pattern, line):
                        if filename.name not in errors:
                            errors[filename.name] = []
                        errors[filename.name].append(line)

                if len(lines) == 0 or 'End Epilogue' not in lines[-1]:
                    if filename.name not in running:
                        running[filename.name] = []

                    running[filename.name] = f'job is still running (no `Epilogue`)'
    else:
        not_started[
            slurm_out_log_pattern.name
        ] = f'no Slurm output log files found with pattern `{os.path.relpath(slurm_out_log_pattern, directory)}`'

    esmf_log_filenames = [Path(filename) for filename in glob(str(esmf_log_pattern))]
    if len(esmf_log_filenames) > 0:
        error_pattern = re.compile('error', re.IGNORECASE)
        for filename in esmf_log_filenames:
            with open(filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) == 0:
                    failures[filename.name] = 'empty ESMF log file'
                else:
                    for line in lines:
                        if re.match(error_pattern, line):
                            if filename.name not in errors:
                                errors[filename.name] = []
                            errors[filename.name].append(line)
    else:
        not_started[
            esmf_log_pattern.name
        ] = f'no ESMF log files found with pattern `{os.path.relpath(esmf_log_pattern, directory)}`'

    for filename in [Path(filename) for filename in glob(str(output_netcdf_pattern))]:
        if filename.exists():
            minimum_file_size = 43081

            if filename.stat().st_size <= minimum_file_size:
                if filename.name not in failures:
                    running[
                        filename.name
                    ] = f'empty file (size {filename.stat().st_size} not greater than {minimum_file_size})'
        else:
            not_started[filename.name] = f'output file not found {filename}'

    completion = {'completion_percentage': completion_percentage}

    if len(not_configured) > 0:
        completion['not_configured'] = not_configured
    if len(not_started) > 0:
        completion['not_started'] = not_started
    if len(failures) > 0:
        completion['failures'] = failures
    if len(errors) > 0:
        completion['errors'] = errors
    if len(running) > 0:
        completion['running'] = running

    return completion


def check_adcirc_completion(directory: PathLike = None) -> (CompletionStatus, float):
    completion_status = collect_adcirc_errors(directory)

    completion_percentage = completion_status['completion_percentage']

    if not isinstance(completion_status, CompletionStatus):
        if len(completion_status) > 1:
            if 'not_configured' in completion_status:
                completion_status = CompletionStatus.NOT_CONFIGURED
            elif 'not_started' in completion_status:
                completion_status = CompletionStatus.NOT_STARTED
            elif 'failures' in completion_status:
                completion_status = CompletionStatus.FAILED
            elif 'errors' in completion_status:
                completion_status = CompletionStatus.ERROR
            elif 'running' in completion_status:
                completion_status = CompletionStatus.RUNNING
            else:
                completion_status = CompletionStatus.COMPLETED
        else:
            completion_status = CompletionStatus.COMPLETED

    return completion_status, completion_percentage


def check_completion(
    directory: PathLike = None, model: ModelJSON = None, verbose: bool = False
):
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path) and (
        not isinstance(directory, Iterable) or isinstance(directory, str)
    ):
        directory = Path(directory)

    if model is None:
        model = ADCIRCJSON

    completion_status = {}

    if isinstance(directory, Iterable):
        with ProcessPoolExecutor() as process_pool:
            for subdirectory_completion_status in process_pool.map(
                partial(check_completion, model=model, verbose=verbose), directory
            ):
                completion_status.update(subdirectory_completion_status)
    elif isinstance(directory, Path):
        subdirectories = [member.name for member in directory.iterdir()]
        if 'spinup' in subdirectories:
            completion_status.update(
                check_completion(directory=directory / 'spinup', model=model, verbose=verbose)
            )
        if 'runs' in subdirectories:
            completion_status['runs'] = check_completion(
                directory=(directory / 'runs').iterdir(), model=model, verbose=verbose
            )
        else:
            if model == ADCIRCJSON:
                if verbose:
                    completion = collect_adcirc_errors(directory=directory)
                    percentage = completion['completion_percentage']
                else:
                    completion, percentage = check_adcirc_completion(directory=directory)

                if isinstance(completion, CompletionStatus):
                    completion = f'{completion.value} - {percentage}%'

                completion_status[directory.name] = completion

    return completion_status
