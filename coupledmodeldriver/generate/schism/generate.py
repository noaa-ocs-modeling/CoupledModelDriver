import logging
import os
from os import PathLike
from pathlib import Path

from coupledmodeldriver.generate.schism.configure import (
    NEMSSCHISMRunConfiguration,
    SCHISMRunConfiguration,
)
from coupledmodeldriver.utilities import get_logger, LOGGER


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

    schism_path = ensemble_configuration['schism']['executable']
    schism_processors = ensemble_configuration['schism']['processors']
    tidal_spinup_duration = ensemble_configuration['schism']['tidal_spinup_duration']
    source_filename = ensemble_configuration['schism']['source_filename']

    if use_nems:
        nems_configuration = ensemble_configuration['nems'].nemspy_modeling_system
        run_processors = nems_configuration.processors
        run_executable = ensemble_configuration['nems']['executable']
    else:
        nems_configuration = None
        run_processors = schism_processors
        run_executable = schism_path

    if source_filename is not None:
        LOGGER.debug(f'sourcing modules from "{source_filename}"')

    # TODO finish this function
