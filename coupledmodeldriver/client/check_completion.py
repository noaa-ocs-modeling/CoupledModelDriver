from argparse import ArgumentParser
import json
from pathlib import Path

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.generate.adcirc.check import check_completion
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


def main():
    completion_status = check_completion(**parse_check_completion_arguments())
    print(json.dumps(completion_status, indent=4))


if __name__ == '__main__':
    main()
