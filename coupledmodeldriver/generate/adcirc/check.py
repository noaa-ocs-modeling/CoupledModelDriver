from enum import Enum
from glob import glob
import os
from os import PathLike
from pathlib import Path
import re

from file_read_backwards import FileReadBackwards


class CompletionStatus(Enum):
    NOT_CONFIGURED = 'not_configured'
    NOT_STARTED = 'not_started'
    IN_SETUP = 'in_setup'
    FAILED = 'failed'
    ERROR = 'error'
    RUNNING = 'running'
    COMPLETED = 'completed'


def is_adcirc_run_directory(directory: PathLike = None) -> bool:
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    required_files = ['fort.14', 'fort.15']
    for filename in required_files:
        if not (directory / filename).exists():
            return False
    else:
        return True


def check_adcirc_completion(
    directory: PathLike = None, verbose: bool = False
) -> ({str: {str, str}}, float):
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    not_configured = {}
    not_started = {}
    failures = {}
    errors = {}
    running = {}

    completion_percentage = 0

    if not is_adcirc_run_directory(directory):
        if verbose:
            not_configured['none'] = f'ADCIRC configuration files not found'
        else:
            return CompletionStatus.NOT_CONFIGURED, completion_percentage

    slurm_out_log_pattern = directory / 'ADCIRC_*_*.out.log'
    slurm_error_log_pattern = directory / 'ADCIRC_*_*.err.log'
    adcirc_output_log_filename = directory / 'fort.16'
    esmf_log_pattern = directory / 'PET*.ESMF_LogFile'
    output_netcdf_pattern = directory / 'fort.*.nc'

    slurm_output_log_filenames = [
        Path(filename) for filename in glob(str(slurm_out_log_pattern))
    ]
    if len(slurm_output_log_filenames) > 0:
        error_pattern = re.compile('error', re.IGNORECASE)
        percentage_pattern = re.compile('[0-9|.]+% COMPLETE')
        for filename in slurm_output_log_filenames:
            with FileReadBackwards(filename) as log_file:
                ended = False
                log_file_errors = []
                for line in log_file:
                    if completion_percentage == 0:
                        percentages = re.findall(percentage_pattern, line)
                        if len(percentages) > 0:
                            completion_percentage = float(percentages[0].split('%')[0])

                    if not ended and 'End Epilogue' in line:
                        ended = True

                    if re.match(error_pattern, line):
                        if verbose:
                            log_file_errors.append(line)
                        else:
                            return CompletionStatus.ERROR, completion_percentage

                if len(log_file_errors) > 0:
                    log_file_errors = list(reversed(log_file_errors))
                    if filename.name in errors:
                        errors[filename.name].extend(log_file_errors)
                    else:
                        errors[filename.name] = log_file_errors

                if not ended:
                    if filename.name not in running:
                        running[filename.name] = []
                    running[filename.name] = f'job is still running (no `Epilogue`)'
    else:
        if verbose:
            not_started[
                slurm_out_log_pattern.name
            ] = f'Slurm output log files `{slurm_out_log_pattern.name}` not found'
        else:
            return CompletionStatus.NOT_STARTED, completion_percentage

    slurm_error_log_filenames = [
        Path(filename) for filename in glob(str(slurm_error_log_pattern))
    ]
    if len(slurm_error_log_filenames) > 0:
        for filename in slurm_error_log_filenames:
            with open(filename) as log_file:
                lines = list(log_file.readlines())
                if len(lines) > 0:
                    if verbose:
                        if filename.name not in errors:
                            errors[filename.name] = []
                        errors[filename.name].extend(lines)
                    else:
                        return CompletionStatus.ERROR, completion_percentage

    if not adcirc_output_log_filename.exists():
        if verbose:
            failures[
                adcirc_output_log_filename.name
            ] = f'ADCIRC output file `{adcirc_output_log_filename.name}` not found'
        else:
            return CompletionStatus.FAILED, completion_percentage

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
                            if verbose:
                                if filename.name not in errors:
                                    errors[filename.name] = []
                                errors[filename.name].append(line)
                            else:
                                return CompletionStatus.ERROR, completion_percentage
    else:
        not_started[
            esmf_log_pattern.name
        ] = f'ESMF log files `{esmf_log_pattern.name}` not found'

    for filename in [Path(filename) for filename in glob(str(output_netcdf_pattern))]:
        if filename.exists():
            minimum_file_size = 43081

            if filename.stat().st_size <= minimum_file_size:
                if filename.name not in failures:
                    not_started[
                        filename.name
                    ] = f'file size ({filename.stat().st_size}) does not exceed the minimum file size ({minimum_file_size})'
        else:
            not_started[filename.name] = f'output file `{filename.name}` not found'

    completion = {}
    if len(not_configured) > 0:
        if verbose:
            completion['not_configured'] = not_configured
        else:
            return CompletionStatus.NOT_CONFIGURED, completion_percentage
    if len(not_started) > 0:
        if verbose:
            completion['not_started'] = not_started
        else:
            return CompletionStatus.NOT_STARTED, completion_percentage
    if len(failures) > 0:
        if verbose:
            completion['failures'] = failures
        else:
            return CompletionStatus.FAILED, completion_percentage
    if len(errors) > 0:
        if verbose:
            completion['errors'] = errors
        else:
            return CompletionStatus.ERROR, completion_percentage
    if len(running) > 0:
        if verbose:
            completion['running'] = running
        else:
            return CompletionStatus.RUNNING, completion_percentage
    if not verbose and len(completion) == 0:
        return CompletionStatus.COMPLETED, completion_percentage

    return completion, completion_percentage
