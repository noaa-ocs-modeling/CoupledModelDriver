from argparse import ArgumentParser
from os import PathLike
from pathlib import Path

from typepigeon import convert_value

from coupledmodeldriver.configure import ModelJSON
from coupledmodeldriver.configure.configure import RunConfiguration
from coupledmodeldriver.generate import ADCIRCRunConfiguration
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON

MODELS = {model.name.lower(): model for model in ModelJSON.__subclasses__()}


def parse_initialize_from_configuration_arguments():
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        'input-directory',
        nargs='*',
        default=Path.cwd(),
        help='directory containing model configuration',
    )
    argument_parser.add_argument(
        'output-directory',
        nargs='*',
        default=Path.cwd(),
        help='directory to write JSON configuration',
    )
    argument_parser.add_argument('--model', help='model of configuraiton, one of: `ADCIRC`')
    argument_parser.add_argument(
        '--skip-existing', action='store_true', help='skip existing files',
    )

    arguments = argument_parser.parse_args()

    input_directory = convert_value(arguments.input_directory, Path)
    output_directory = convert_value(arguments.output_directory, Path)

    model = arguments.model
    if model is not None:
        model = MODELS[model.lower()]

    overwrite = not arguments.skip_existing

    return {
        'input_directory': input_directory,
        'output_directory': output_directory,
        'model': model,
        'overwrite': overwrite,
    }


def initialize_from_model_configuration_directory(
    directory: PathLike, model: ModelJSON = None
) -> RunConfiguration:
    if model is None:
        model = ADCIRCJSON

    if model == ADCIRCJSON:
        run_configuration = ADCIRCRunConfiguration.from_model_configuration_directory(
            directory=directory
        )
    else:
        raise NotImplementedError(f'model "{model}" not implemented')

    return run_configuration


def main():
    arguments = parse_initialize_from_configuration_arguments()
    run_configuration = initialize_from_model_configuration_directory(
        directory=arguments['input_directory'], model=arguments['model'],
    )
    run_configuration.write_directory(
        directory=arguments['output_directory'], overwrite=arguments['overwrite'],
    )


if __name__ == '__main__':
    main()
