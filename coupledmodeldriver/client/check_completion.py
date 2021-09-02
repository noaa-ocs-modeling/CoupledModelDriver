from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import json
from os import PathLike
from pathlib import Path
from typing import Iterable

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON
from coupledmodeldriver.generate.adcirc.check import (
    check_adcirc_completion,
    CompletionStatus,
    is_adcirc_run_directory,
)
from coupledmodeldriver.utilities import convert_value

MODELS = {model.name.lower(): model for model in ModelJSON.__subclasses__()}


def parse_check_completion_arguments():
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        'directory',
        nargs='*',
        default=Path.cwd(),
        help='directory containing model run configuration',
    )
    argument_parser.add_argument('--model', help='model that is running, one of: `ADCIRC`')
    argument_parser.add_argument(
        '--verbose', action='store_true', help='list all errors and problems with runs'
    )

    arguments = argument_parser.parse_args()

    model = arguments.model
    if model is not None:
        model = MODELS[model.lower()]

    directory = convert_value(arguments.directory, [Path])
    if len(directory) == 1:
        directory = directory[0]

    return {
        'directory': directory,
        'model': model,
        'verbose': arguments.verbose,
    }


def is_model_directory(directory: PathLike, model: ModelJSON = None) -> bool:
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    if model is None:
        model = ADCIRCJSON

    if model == ADCIRCJSON:
        is_model_directory = is_adcirc_run_directory(directory)
    else:
        raise NotImplementedError(f'model "{model}" not implemented')

    return is_model_directory


def check_model_directory(
    directory: PathLike, verbose: bool = False, model: ModelJSON = None
) -> ({str: {str, str}}, float):
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    if model is None:
        model = ADCIRCJSON

    if model == ADCIRCJSON:
        return check_adcirc_completion(directory, verbose=verbose)
    else:
        raise NotImplementedError(f'model "{model}" not implemented')


def check_completion(
    directory: PathLike = None, verbose: bool = False, model: ModelJSON = None
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
            subdirectory_completion_statuses = process_pool.map(
                partial(check_completion, model=model, verbose=verbose),
                (subdirectory for subdirectory in directory if Path(subdirectory).is_dir()),
            )
            for subdirectory_completion_status in subdirectory_completion_statuses:
                completion_status.update(subdirectory_completion_status)
    elif isinstance(directory, Path):
        if is_model_directory(directory, model=model):
            completion, percentage = check_model_directory(
                directory=directory, verbose=verbose
            )
            if isinstance(completion, CompletionStatus):
                completion = f'{completion.value} - {percentage}%'
            else:
                completion['completion_percentage'] = percentage

            completion_status[directory.name] = completion
        else:
            subdirectory_completion_statuses = check_completion(
                directory=directory.iterdir(), verbose=verbose, model=model
            )
            if len(subdirectory_completion_statuses) > 0:
                completion_status[directory.name] = subdirectory_completion_statuses

    return completion_status


def main():
    completion_status = check_completion(**parse_check_completion_arguments())
    print(json.dumps(completion_status, indent=4))


if __name__ == '__main__':
    main()
