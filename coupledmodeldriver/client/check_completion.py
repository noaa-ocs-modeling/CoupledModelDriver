from argparse import ArgumentParser
from functools import partial
import json
from os import PathLike
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from typepigeon import convert_value

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON
from coupledmodeldriver.generate.adcirc.check import (
    check_adcirc_completion,
    CompletionStatus,
    is_adcirc_run_directory,
)
from coupledmodeldriver.generate.schism.base import SCHISMJSON
from coupledmodeldriver.generate.schism.check import (
    check_schism_completion,
    CompletionStatus,
    is_schism_run_directory,
)
from coupledmodeldriver.utilities import ProcessPoolExecutorStackTraced

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
    elif model == SCHISMJSON:
        is_model_directory = is_schism_run_directory(directory)
    else:
        raise NotImplementedError(f'model "{model}" not implemented')

    return is_model_directory


def check_model_directory(
    directory: PathLike, verbose: bool = False, model: ModelJSON = None
) -> Dict[str, Any]:
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    if model is None:
        model = ADCIRCJSON

    if model == ADCIRCJSON:
        return check_adcirc_completion(directory, verbose=verbose)
    elif model == SCHISMJSON:
        return check_schism_completion(directory, verbose=verbose)
    else:
        raise NotImplementedError(f'model "{model}" not implemented')


def check_completion(
    directory: PathLike = None, verbose: bool = False, model: ModelJSON = None
) -> Dict[str, Any]:
    """
    check the completion status of a running model

    :param directory: directory containing model run configuration
    :param verbose: list all errors and problems with runs
    :param model: model that is running, one of: ``ADCIRC``
    :return: JSON output of completion status
    """

    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path) and (
        not isinstance(directory, Iterable) or isinstance(directory, str)
    ):
        directory = Path(directory)

    completion_status = {}
    if isinstance(directory, Iterable):
        with ProcessPoolExecutorStackTraced() as process_pool:
            subdirectory_completion_statuses = process_pool.map(
                partial(check_completion, model=model, verbose=verbose),
                (subdirectory for subdirectory in directory if Path(subdirectory).is_dir()),
            )
            for subdirectory_completion_status in subdirectory_completion_statuses:
                completion_status.update(subdirectory_completion_status)
    elif isinstance(directory, Path):
        if model is None:
            # model = ADCIRCJSON
            for model_type in MODELS.values():
                if not is_model_directory(directory, model=model_type):
                    continue
                model = model_type
                break

        if is_model_directory(directory, model=model):
            completion = check_model_directory(
                directory=directory, verbose=verbose, model=model
            )
            for key in [
                key
                for key, value in completion.items()
                if isinstance(value, Mapping) and len(value) == 0
            ]:
                del completion[key]
            completion['status'] = completion['status'].name.lower()
            completion['progress'] = f'{completion["progress"]}%'
            completion_status[directory.name] = completion
        else:
            subdirectory_completion_statuses = check_completion(
                directory=directory.iterdir(), verbose=verbose, model=model
            )
            if len(subdirectory_completion_statuses) > 0:
                completion_status[directory.name] = subdirectory_completion_statuses

    try:
        # sort by progress percentage (reversed), then completion status, then by run number, then alphabetically
        completion_status = dict(
            sorted(
                completion_status.items(),
                key=lambda item: (
                    -float(
                        (
                            item[1]['progress']
                            if isinstance(item[1], Mapping)
                            else item[1].split(' - ')[1]
                        )[:-1]
                    ),
                    CompletionStatus[
                        (
                            item[1]['status']
                            if isinstance(item[1], Mapping)
                            else item[1].split(' - ')[0]
                        ).upper()
                    ].value,
                    float(item[0].split('_')[-1])
                    if '_' in item[0] and item[0].split('_')[-1].isdecimal()
                    else -1,
                    item[0],
                ),
            )
        )
    except KeyError:
        pass

    if not verbose:
        for key, value in completion_status.items():
            if isinstance(value, Mapping) and 'status' in value and 'progress' in value:
                completion_status[key] = f'{value["status"]} - {value["progress"]}'

    if (
        isinstance(completion_status, Mapping)
        and len(completion_status) > 0
        and isinstance(directory, Path)
    ):
        directory_completion_status = completion_status[directory.name]
        if (
            isinstance(directory_completion_status, Mapping)
            and len(directory_completion_status) > 0
        ):
            # collapse statuses if all statuses in the directory are the same
            statuses = list(directory_completion_status.values())
            if all(status == statuses[0] for status in statuses):
                completion_status[directory.name] = statuses[0]

    return completion_status


def main():
    completion_status = check_completion(**parse_check_completion_arguments())
    print(json.dumps(completion_status, indent=4))


if __name__ == '__main__':
    main()
