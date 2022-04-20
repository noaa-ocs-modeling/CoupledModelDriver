from enum import Enum
from os import PathLike
from pathlib import Path
from typing import Any, Dict


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

    # TODO enumerate SCHISM required files here
    required_files = [...]
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
            ...
        ] = f'SCHISM configuration files not found'

    # TODO check SCHISM run status here
    ...

    return {
        'status': status,
        'progress': progress,
        **{entry.name.lower(): value for entry, value in completion.items()},
    }
