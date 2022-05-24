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


def is_schism_run_directory(directory: PathLike = None) -> bool:
    """
    check if the given directory has the baseline SCHISM configuration files

    :param directory: path to directory
    :return: whether the directory is a SCHISM configuration
    """

    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    required_files = ['hgrid.gr3', 'vgrid.in', 'param.nml', 'outputs', 'bctides.in']
    for filename in required_files:
        if not (directory / filename).exists():
            return False
    else:
        return True


def check_schism_completion(
    directory: PathLike = None, verbose: bool = False
) -> Dict[str, Any]:
    """
    return the status of SCHISM execution within the given directory

    :param directory: path to directory
    :param verbose: whether to include errors and detailed status checks in output
    :return: status of SCHISM execution in JSON format
    """

    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    status = CompletionStatus.COMPLETED
    progress = 0

    completion = {entry: {} for entry in CompletionStatus}

    if not is_schism_run_directory(directory):
        completion[CompletionStatus.NOT_CONFIGURED][
            'hgrid.gr3,vgrid.in'
        ] = f'SCHISM configuration files not found'

    if verbose or status.value < CompletionStatus.NOT_CONFIGURED.value:
        # NOTE: Slurm log files are NOT very useful for SCHISM outputs
        slurm_out_log_pattern = directory / 'SCHISM_*_*.out.log'
        slurm_output_log_filenames = [
            Path(filename) for filename in glob(str(slurm_out_log_pattern))
        ]
        if len(slurm_output_log_filenames) > 0:
            for filename in slurm_output_log_filenames:
                if verbose or status.value < CompletionStatus.ERROR.value:
                    with FileReadBackwards(filename) as log_file:
                        ended = False
                        for line in log_file:
                            if not ended and 'End Epilogue' in line:
                                ended = True

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
        slurm_error_log_pattern = directory / 'SCHISM_*_*.err.log'
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
        schism_output_log_filename = directory / 'outputs' / 'mirror.out'
        if not schism_output_log_filename.exists():
            completion[CompletionStatus.FAILED][
                schism_output_log_filename.name
            ] = f'SCHISM output file `{schism_output_log_filename.name}` not found'
        else:
            stepping_init_pattern = re.compile(
                '^\stime stepping begins\.\.\.\s+\d+\s+(\d+).*$'
            )
            stepping_run_pattern = re.compile('^TIME STEP=\s+(\d+);.*$')
            finish_pattern = re.compile('^Run completed successfully.*$')
            with FileReadBackwards(schism_output_log_filename) as log_file:
                total_steps = -1
                current_step = -1
                for line in log_file:
                    match_fini = finish_pattern.match(line)
                    match_init = stepping_init_pattern.match(line)
                    match_step = stepping_run_pattern.match(line)
                    if match_fini:
                        progress = 100
                        break
                    elif match_init:
                        total_steps = int(match_init.group(1))
                    elif match_step:
                        current_step = max(current_step, int(match_step.group(1)))
                if progress != 100 and current_step > -1 and total_steps > -1:
                    progress = round(100 * current_step / total_steps, 2)

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
        output_netcdf_pattern = directory / 'outputs' / 'schout*.nc'
        output_netcdf_files = [Path(filename) for filename in glob(str(output_netcdf_pattern))]
        if (directory / 'outputs' / 'out2d_1.nc').is_file():
            output_netcdf_files.append(directory / 'outputs' / 'out2d_1.nc')
        for filename in output_netcdf_files:
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
