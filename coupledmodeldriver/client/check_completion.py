from argparse import ArgumentParser
import json
from os import PathLike
from pathlib import Path

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON
from coupledmodeldriver.generate.adcirc.check import \
    check_adcirc_completion
from coupledmodeldriver.utilities import convert_value

MODELS = {model.name.lower(): model for model in ModelJSON.__subclasses__()}


def parse_check_completion_arguments():
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        '--directory', help='directory containing model run configuration'
    )
    argument_parser.add_argument(
        '--model', help='model that is running, one of: `ADCIRC`'
    )

    arguments = argument_parser.parse_args()

    model = arguments.model
    if model is not None:
        model = MODELS[model.lower()]

    return {
        'directory': convert_value(arguments.directory, Path),
        'model': model,
    }


def check_completion(directory: PathLike = None, model: ModelJSON = None):
    if directory is None:
        directory = Path.cwd()
    elif not isinstance(directory, Path):
        directory = Path(directory)

    if model is None:
        model = ADCIRCJSON

    completion_status = {}
    if 'runs' in directory.iterdir():
        completion_status['runs'] = {}
        for run_directory in (directory / 'runs').iterdir():
            completion_status['runs'].update(check_completion(directory=run_directory))
    if 'spinup' in directory.iterdir():
        completion_status.update(check_completion(directory=directory / 'spinup'))
    else:
        if model == ADCIRCJSON:
            completion_status[directory.name] = check_adcirc_completion(directory=directory)

    return completion_status


def main():
    completion_status = check_completion(**parse_check_completion_arguments())
    print(json.dumps(completion_status, indent=4))


if __name__ == '__main__':
    main()
