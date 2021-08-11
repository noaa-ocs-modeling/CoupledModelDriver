from argparse import ArgumentParser
import json
from os import PathLike
from pathlib import Path
from typing import Collection

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON
from coupledmodeldriver.generate.adcirc.check import (
    check_adcirc_completion,
    collect_adcirc_errors,
    CompletionStatus,
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


def check_completion(
    directory: PathLike = None, model: ModelJSON = None, verbose: bool = False
):
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path) and (
        not isinstance(directory, Collection) or isinstance(directory, str)
    ):
        directory = Path(directory)

    if model is None:
        model = ADCIRCJSON

    completion_status = {}

    if isinstance(directory, Collection):
        for subdirectory in directory:
            completion_status[subdirectory.name] = check_completion(
                directory=subdirectory, model=model, verbose=verbose
            )
    elif isinstance(directory, Path):
        subdirectories = [member.name for member in directory.iterdir()]
        if 'spinup' in subdirectories:
            completion_status.update(
                check_completion(directory=directory / 'spinup', model=model, verbose=verbose)
            )
        if 'runs' in subdirectories:
            completion_status['runs'] = {}
            for run_directory in (directory / 'runs').iterdir():
                completion_status['runs'].update(
                    check_completion(directory=run_directory, model=model, verbose=verbose)
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


def main():
    completion_status = check_completion(**parse_check_completion_arguments())
    print(json.dumps(completion_status, indent=4))


if __name__ == '__main__':
    main()
