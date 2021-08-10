from enum import Enum
from glob import glob
import os
from os import PathLike
from pathlib import Path
from typing import Dict, Union


class CompletionStatus(Enum):
    NOT_STARTED = 'not_started'
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


def collect_adcirc_errors(directory: PathLike = None) -> {str: Union[str, Dict[str, str]]}:
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    required_files = ['fort.14', 'fort.15']
    nonexistant_files = [
        filename for filename in required_files if not (directory / filename).exists()
    ]
    if len(nonexistant_files) > 0:
        raise FileNotFoundError(
            f'file(s) not found: {", ".join(nonexistant_files)} - not an ADCIRC run directory: {directory}'
        )

    not_started = {}
    failures = {}
    errors = {}
    running = {}

    adcirc_output_log_filename = directory / 'fort.16'
    slurm_error_log_pattern = directory / 'ADCIRC_*_*.err.log'
    slurm_out_log_pattern = directory / 'ADCIRC_*_*.out.log'
    esmf_log_pattern = directory / 'PET*.ESMF_LogFile'
    output_netcdf_pattern = directory / 'fort.*.nc'

    if not adcirc_output_log_filename.exists():
        not_started[
            adcirc_output_log_filename.name
        ] = f'ADCIRC output file `fort.16` was not found at {adcirc_output_log_filename}'

    for filename in [Path(filename) for filename in glob(str(slurm_error_log_pattern))]:
        with open(filename) as log_file:
            lines = list(log_file.readlines())
            if len(lines) > 0:
                if filename.name not in errors:
                    errors[filename.name] = []
                errors[filename.name].extend(lines)

    for filename in [Path(filename) for filename in glob(str(slurm_out_log_pattern))]:
        with open(filename, 'rb') as log_file:
            lines = tail(log_file, lines=2)
            if len(lines) == 0 or 'End Epilogue' not in lines[-1]:
                if filename.name not in running:
                    running[filename.name] = []
                running[filename.name] = f'job is still running (no `Epilogue`) - {lines}'

    esmf_log_filenames = [Path(filename) for filename in glob(str(esmf_log_pattern))]
    if len(esmf_log_filenames) == 0:
        not_started[
            esmf_log_pattern.name
        ] = f'no ESMF log files found with pattern `{os.path.relpath(esmf_log_pattern, directory)}`'
    else:
        for filename in esmf_log_filenames:
            with open(filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) == 0:
                    failures[filename.name] = 'empty ESMF log file'

    for filename in [Path(filename) for filename in glob(str(output_netcdf_pattern))]:
        if filename.exists():
            minimum_file_size = 43081

            if filename.stat().st_size <= minimum_file_size:
                if filename.name not in failures:
                    failures[
                        filename.name
                    ] = f'empty file (size {filename.stat().st_size} not greater than {minimum_file_size})'
        else:
            not_started[filename.name] = f'output file not found {filename}'

    issues = {}

    if len(not_started) > 0:
        issues['not_started'] = not_started
    if len(failures) > 0:
        issues['failures'] = failures
    if len(errors) > 0:
        issues['errors'] = errors
    if len(running) > 0:
        issues['running'] = running

    return issues


def check_adcirc_completion(directory: PathLike = None) -> CompletionStatus:
    errors = collect_adcirc_errors(directory)

    if len(errors) > 0:
        if 'not_started' in errors:
            completion_status = CompletionStatus.NOT_STARTED
        elif 'failures' in errors:
            completion_status = CompletionStatus.FAILED
        elif 'errors' in errors:
            completion_status = CompletionStatus.ERROR
        elif 'running' in errors:
            completion_status = CompletionStatus.RUNNING
        else:
            completion_status = CompletionStatus.COMPLETED
    else:
        completion_status = CompletionStatus.COMPLETED

    return completion_status
