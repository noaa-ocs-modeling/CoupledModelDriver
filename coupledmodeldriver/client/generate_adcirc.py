from argparse import ArgumentParser
from pathlib import Path

from coupledmodeldriver.generate import generate_adcirc_configuration
from coupledmodeldriver.utilities import convert_value


def parse_generate_adcirc_arguments():
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
        '--relative-paths',
        action='store_true',
        help='use relative paths in output configuration',
    )
    argument_parser.add_argument(
        '--skip-existing', action='store_true', help='skip existing files',
    )
    argument_parser.add_argument(
        '--verbose', action='store_true', help='show more verbose log messages'
    )

    arguments = argument_parser.parse_args()

    return {
        'configuration_directory': convert_value(arguments.configuration_directory, Path),
        'output_directory': convert_value(arguments.output_directory, Path),
        'relative_paths': arguments.relative_paths,
        'overwrite': not arguments.skip_existing,
        'verbose': arguments.verbose,
    }


def main():
    generate_adcirc_configuration(**parse_generate_adcirc_arguments())


if __name__ == '__main__':
    main()
