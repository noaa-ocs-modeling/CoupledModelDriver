from os import PathLike

from coupledmodeldriver.generate import (
    generate_adcirc_configuration,
    generate_schism_configuration,
)


def generate_configuration(
    configuration_directory: PathLike,
    output_directory: PathLike = None,
    overwrite: bool = False,
    verbose: bool = False,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param configuration_directory: path containing JSON configuration files
    :param output_directory: path to store generated configuration files
    :param overwrite: whether to overwrite existing files
    :param verbose: whether to show more verbose log messages
    """

    filenames = [filename.name.lower() for filename in configuration_directory.iterdir()]

    models = ['adcirc', 'schism']
    models = {model: f'configure_{model}.json' in filenames for model in models}

    if sum(models) > 1:
        raise NotImplementedError('using more than one circulation model is not implemented')

    if models['adcirc']:
        generate_adcirc_configuration(
            configuration_directory=configuration_directory,
            output_directory=output_directory,
            overwrite=overwrite,
            verbose=verbose,
        )
    elif models['schism']:
        generate_schism_configuration(
            configuration_directory=configuration_directory,
            output_directory=output_directory,
            overwrite=overwrite,
            verbose=verbose,
        )
