from enum import Enum
from glob import glob
from os import PathLike
from pathlib import Path
import re
from typing import Any, Dict

from file_read_backwards import FileReadBackwards


class CompletionStatus(Enum):
    """
    options for completion status
    """

    NOT_CONFIGURED = 5
    NOT_STARTED = 4
    FAILED = 3
    ERROR = 2
    RUNNING = 1
    COMPLETED = 0


def is_adcirc_run_directory(directory: PathLike = None) -> bool:
    """
    check if the given directory has the baseline ADCIRC configuration files

    :param directory: path to directory
    :return: whether the directory is an ADCIRC configuration
    """

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
) -> Dict[str, Any]:
    """
    return the status of ADCIRC execution within the given directory

    :param directory: path to directory
    :param verbose: whether to include errors and detailed status checks in output
    :return: status of ADCIRC execution in JSON format
    """

    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    status = CompletionStatus.COMPLETED
    progress = 0

    completion = {entry: {} for entry in CompletionStatus}

    if not is_adcirc_run_directory(directory):
        completion[CompletionStatus.NOT_CONFIGURED][
            'fort.14,fort.15'
        ] = f'ADCIRC configuration files not found'

    if verbose or status.value < CompletionStatus.NOT_CONFIGURED.value:
        slurm_out_log_pattern = directory / 'ADCIRC_*_*.out.log'
        slurm_output_log_filenames = [
            Path(filename) for filename in glob(str(slurm_out_log_pattern))
        ]
        if len(slurm_output_log_filenames) > 0:
            error_pattern = re.compile('error', re.IGNORECASE)
            percentage_pattern = re.compile('[0-9|.]+% COMPLETE')
            for filename in slurm_output_log_filenames:
                if verbose or status.value < CompletionStatus.ERROR.value:
                    with FileReadBackwards(filename) as log_file:
                        ended = False
                        log_file_errors = []
                        for line in log_file:
                            if progress == 0:
                                percentages = re.findall(percentage_pattern, line)
                                if len(percentages) > 0:
                                    progress = float(percentages[0].split('%')[0])

                            if not ended and 'End Epilogue' in line:
                                ended = True

                            if re.match(error_pattern, line):
                                log_file_errors.append(line)
                                if not verbose:
                                    break

                        if len(log_file_errors) > 0:
                            log_file_errors = list(reversed(log_file_errors))
                            if filename.name in completion[CompletionStatus.ERROR]:
                                completion[CompletionStatus.ERROR][filename.name].extend(
                                    log_file_errors
                                )
                            else:
                                completion[CompletionStatus.ERROR][
                                    filename.name
                                ] = log_file_errors

                        if not ended:
                            if filename.name not in completion[CompletionStatus.RUNNING]:
                                completion[CompletionStatus.RUNNING][filename.name] = []
                            completion[CompletionStatus.RUNNING][
                                filename.name
                            ] = f'job is still running (no `Epilogue`)'
        else:
            completion[CompletionStatus.NOT_STARTED][
                slurm_out_log_pattern.name
            ] = f'Slurm output log files `{slurm_out_log_pattern.name}` not found'

    if verbose or status.value < CompletionStatus.ERROR.value:
        slurm_error_log_pattern = directory / 'ADCIRC_*_*.err.log'
        slurm_error_log_filenames = [
            Path(filename) for filename in glob(str(slurm_error_log_pattern))
        ]
        if len(slurm_error_log_filenames) > 0:
            for filename in slurm_error_log_filenames:
                if verbose or status.value < CompletionStatus.ERROR.value:
                    with open(filename) as log_file:
                        lines = list(log_file.readlines())
                        if len(lines) > 0:
                            if filename.name not in completion[CompletionStatus.ERROR]:
                                completion[CompletionStatus.ERROR][filename.name] = []
                            completion[CompletionStatus.ERROR][filename.name].extend(lines)

    if verbose or status.value < CompletionStatus.FAILED.value:
        adcirc_output_log_filename = directory / 'fort.16'
        if not adcirc_output_log_filename.exists():
            completion[CompletionStatus.FAILED][
                adcirc_output_log_filename.name
            ] = f'ADCIRC output file `{adcirc_output_log_filename.name}` not found'

    if verbose or status.value < CompletionStatus.NOT_STARTED.value:
        esmf_log_pattern = directory / 'PET*.ESMF_LogFile'
        esmf_log_filenames = [Path(filename) for filename in glob(str(esmf_log_pattern))]
        if len(esmf_log_filenames) > 0:
            error_pattern = re.compile('error', re.IGNORECASE)
            for filename in esmf_log_filenames:
                with open(filename) as log_file:
                    lines = list(log_file.readlines())
                    if len(lines) == 0:
                        completion[CompletionStatus.FAILED][
                            filename.name
                        ] = f'empty ESMF log file `{filename.name}`'
                    else:
                        for line in lines:
                            if re.match(error_pattern, line):
                                if filename.name not in completion[CompletionStatus.ERROR]:
                                    completion[CompletionStatus.ERROR][filename.name] = []
                                completion[CompletionStatus.ERROR][filename.name].append(line)

    if verbose or status.value < CompletionStatus.NOT_STARTED.value:
        output_netcdf_pattern = directory / 'fort.*.nc'
        for filename in [Path(filename) for filename in glob(str(output_netcdf_pattern))]:
            if filename.exists():
                minimum_file_size = 43081

                if filename.stat().st_size <= minimum_file_size:
                    completion[CompletionStatus.NOT_STARTED][
                        filename.name
                    ] = f'file size ({filename.stat().st_size}) does not exceed the minimum file size ({minimum_file_size})'
            else:
                completion[CompletionStatus.NOT_STARTED][
                    filename.name
                ] = f'output file `{filename.name}` not found'

    if progress > 0 and status == CompletionStatus.NOT_STARTED:
        status = CompletionStatus.RUNNING

    for entry in CompletionStatus:
        if len(completion[entry]) > 0 and entry.value > status.value:
            status = entry

    return {
        'status': status,
        'progress': progress,
        **{entry.name.lower(): value for entry, value in completion.items()},
    }
