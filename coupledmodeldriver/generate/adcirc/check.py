from concurrent.futures import ProcessPoolExecutor
from enum import Enum
from functools import partial
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
            not_configured['none'] = f'not an ADCIRC run directory'
        else:
            return CompletionStatus.NOT_CONFIGURED, completion_percentage

    adcirc_output_log_filename = directory / 'fort.16'
    slurm_error_log_pattern = directory / 'ADCIRC_*_*.err.log'
    slurm_out_log_pattern = directory / 'ADCIRC_*_*.out.log'
    esmf_log_pattern = directory / 'PET*.ESMF_LogFile'
    output_netcdf_pattern = directory / 'fort.*.nc'

    if not adcirc_output_log_filename.exists():
        if verbose:
            not_started[
                adcirc_output_log_filename.name
            ] = f'ADCIRC output file `fort.16` was not found at {adcirc_output_log_filename}'
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
            ] = f'no Slurm output log files found with pattern `{os.path.relpath(slurm_out_log_pattern, directory)}`'
        else:
            return CompletionStatus.NOT_STARTED, completion_percentage

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
        running[
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
            running[filename.name] = f'output file not found {filename}'

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

    if not verbose:
        if len(completion) > 1:
            if 'not_configured' in completion:
                completion = CompletionStatus.NOT_CONFIGURED
            elif 'not_started' in completion:
                completion = CompletionStatus.NOT_STARTED
            elif 'failures' in completion:
                completion = CompletionStatus.FAILED
            elif 'errors' in completion:
                completion = CompletionStatus.ERROR
            elif 'running' in completion:
                completion = CompletionStatus.RUNNING
            else:
                completion = CompletionStatus.COMPLETED
        else:
            completion = CompletionStatus.COMPLETED

    return completion, completion_percentage


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
                completion, percentage = check_adcirc_completion(
                    directory=directory, verbose=verbose
                )

                if isinstance(completion, CompletionStatus):
                    completion = f'{completion.value} - {percentage}%'

                completion_status[directory.name] = completion

    return completion_status
