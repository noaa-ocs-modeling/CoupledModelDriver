from argparse import ArgumentParser
from pathlib import Path

from coupledmodeldriver.generate import (
    generate_adcirc_configuration,
    generate_nems_adcirc_configuration,
)
from coupledmodeldriver.utilities import convert_value


def main():
    argument_parser = ArgumentParser()

    argument_parser.add_argument(
        '--configuration-directory',
        default=Path().cwd(),
        help='path containing JSON configuration files',
    )
    argument_parser.add_argument(
        '--output-directory', default=None, help='path to store generated configuration files'
    )
    argument_parser.add_argument(
        '--skip-existing', action='store_true', help='skip existing files',
    )
    argument_parser.add_argument(
        '--verbose', action='store_true', help='show more verbose log messages'
    )

    arguments = argument_parser.parse_args()

    configuration_directory = convert_value(arguments.configuration_directory, Path)
    output_directory = convert_value(arguments.output_directory, Path)
    overwrite = not arguments.skip_existing
    verbose = arguments.verbose

    if output_directory is None:
        output_directory = configuration_directory

    if 'configure_nems.json' in [
        filename.name.lower() for filename in configuration_directory.iterdir()
    ]:
        generate = generate_nems_adcirc_configuration
    else:
        generate = generate_adcirc_configuration

    generate(
        configuration_directory=configuration_directory,
        output_directory=output_directory,
        overwrite=overwrite,
        verbose=verbose,
    )


if __name__ == '__main__':
    main()
