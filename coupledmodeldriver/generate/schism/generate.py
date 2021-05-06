import logging
import os
from os import PathLike
from pathlib import Path

from coupledmodeldriver.generate.schism.configure import (
    NEMSSCHISMRunConfiguration,
    SCHISMRunConfiguration,
)
from coupledmodeldriver.utilities import LOGGER, get_logger


def generate_schism_configuration(
    configuration_directory: PathLike,
    output_directory: PathLike = None,
    overwrite: bool = False,
    verbose: bool = False,
):
    """
    Generate SCHISM run configuration for given variable values.

    :param configuration_directory: path containing JSON configuration files
    :param output_directory: path to store generated configuration files
    :param overwrite: whether to overwrite existing files
    :param verbose: whether to show more verbose log messages
    """

    get_logger(LOGGER.name, console_level=logging.DEBUG if verbose else logging.INFO)

    if not isinstance(configuration_directory, Path):
        configuration_directory = Path(configuration_directory)

    if output_directory is None:
        output_directory = configuration_directory
    elif not isinstance(output_directory, Path):
        output_directory = Path(output_directory)

    if not output_directory.exists():
        os.makedirs(output_directory, exist_ok=True)

    output_directory = output_directory.resolve()
    if not os.path.isabs(output_directory):
        output_directory = output_directory.absolute()

    starting_directory = Path.cwd()
    if configuration_directory.absolute() != starting_directory:
        LOGGER.debug(f'moving into "{configuration_directory}"')
        os.chdir(configuration_directory.absolute())
        configuration_directory = Path.cwd()
    else:
        starting_directory = None

    use_nems = 'configure_nems.json' in [
        filename.name.lower() for filename in configuration_directory.iterdir()
    ]

    if use_nems:
        LOGGER.debug(f'generating NEMS configuration')
        ensemble_configuration = NEMSSCHISMRunConfiguration.read_directory(
            configuration_directory
        )
    else:
        LOGGER.debug(f'generating ADCIRC-only configuration')
        ensemble_configuration = SCHISMRunConfiguration.read_directory(configuration_directory)

    platform = ensemble_configuration['modeldriver']['platform']

    job_duration = ensemble_configuration['slurm']['job_duration']
    partition = ensemble_configuration['slurm']['partition']
    email_type = ensemble_configuration['slurm']['email_type']
    email_address = ensemble_configuration['slurm']['email_address']

    original_fort13_filename = ensemble_configuration['adcirc'].fort13_path
    original_fort14_filename = ensemble_configuration['adcirc'].fort14_path
    adcirc_executable = ensemble_configuration['adcirc']['executable']
    adcprep_executable = ensemble_configuration['adcirc']['adcprep_executable']
    adcirc_processors = ensemble_configuration['adcirc']['processors']
    tidal_spinup_duration = ensemble_configuration['adcirc']['tidal_spinup_duration']
    source_filename = ensemble_configuration['adcirc']['source_filename']
    use_original_mesh = ensemble_configuration['adcirc']['use_original_mesh']
