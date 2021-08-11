from enum import Enum
from glob import glob
import os
from os import PathLike
from pathlib import Path
import re
from typing import Dict, Union


class CompletionStatus(Enum):
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

    if not is_adcirc_run_directory(directory):
        raise FileNotFoundError(f'not an ADCIRC run directory: {directory}')

    not_started = {}
    failures = {}
    errors = {}
    running = {}

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
        for filename in slurm_output_log_filenames:
            with open(filename, 'rb') as log_file:
                lines = tail(log_file, lines=30)
                percentages = re.findall('[0-9|.]+% COMPLETE', '\n'.join(lines))

                if len(percentages) > 0:
                    completion_percentage = float(percentages[-1].split('%')[0])

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
        for filename in esmf_log_filenames:
            with open(filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) == 0:
                    failures[filename.name] = 'empty ESMF log file'
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
            if 'not_started' in completion_status:
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
