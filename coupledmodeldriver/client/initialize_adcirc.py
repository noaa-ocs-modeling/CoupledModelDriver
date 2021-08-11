from argparse import ArgumentParser
from datetime import datetime, timedelta
from enum import Enum
import logging
import os
from pathlib import Path
from typing import Any, Mapping

from adcircpy import TidalSource

from coupledmodeldriver import Platform
from coupledmodeldriver.configure import (
    ATMESHForcingJSON,
    BestTrackForcingJSON,
    OWIForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from coupledmodeldriver.configure.base import NEMSCapJSON
from coupledmodeldriver.configure.forcings.base import (
    FileForcingJSON,
    ForcingJSON,
    TimestepForcingJSON,
    WaveForcingJSON,
    WindForcingJSON,
)
from coupledmodeldriver.generate import ADCIRCRunConfiguration, NEMSADCIRCRunConfiguration
from coupledmodeldriver.utilities import convert_value, get_logger


class ForcingConfigurations(Enum):
    tidal = TidalForcingJSON
    atmesh = ATMESHForcingJSON
    besttrack = BestTrackForcingJSON
    owi = OWIForcingJSON
    ww3data = WW3DATAForcingJSON


FORCING_NAMES = list(entry.name for entry in ForcingConfigurations)
DEFAULT_TIDAL_SOURCE = TidalSource.TPXO
DEFAULT_TIDAL_CONSTITUENTS = 'all'


def parse_initialize_adcirc_arguments(extra_arguments: {str: type} = None) -> {str: Any}:
    if extra_arguments is None:
        extra_arguments = {}
    elif not isinstance(extra_arguments, Mapping):
        extra_arguments = {extra_argument: None for extra_argument in extra_arguments}
    extra_arguments = {
        extra_argument.strip('-'): extra_arguments[extra_argument]
        for extra_argument in extra_arguments
    }

    argument_parser = ArgumentParser()

    argument_parser.add_argument(
        '--platform', required=True, help='HPC platform for which to configure'
    )
    argument_parser.add_argument(
        '--mesh-directory', required=True, help='path to input mesh (`fort.13`, `fort.14`)'
    )
    argument_parser.add_argument(
        '--modeled-start-time', required=True, help='start time within the modeled system'
    )
    argument_parser.add_argument(
        '--modeled-duration', required=True, help=' end time within the modeled system'
    )
    argument_parser.add_argument(
        '--modeled-timestep', required=True, help='time interval within the modeled system'
    )
    argument_parser.add_argument(
        '--nems-interval', default=None, help='main loop interval of NEMS run'
    )
    argument_parser.add_argument(
        '--modulefile', default=None, help='path to module file to `source`'
    )
    argument_parser.add_argument(
        '--forcings',
        default=None,
        help=f'comma-separated list of forcings to configure, from {FORCING_NAMES}',
    )
    argument_parser.add_argument(
        '--adcirc-executable',
        default='adcirc',
        help='filename of compiled `adcirc` or `NEMS.x`',
    )
    argument_parser.add_argument(
        '--adcprep-executable', default='adcprep', help='filename of compiled `adcprep`'
    )
    argument_parser.add_argument(
        '--aswip-executable', default='aswip', help='filename of compiled `aswip`'
    )
    argument_parser.add_argument(
        '--adcirc-processors', default=11, help='numbers of processors to assign for ADCIRC'
    )
    argument_parser.add_argument(
        '--job-duration', default='06:00:00', help='wall clock time for job'
    )
    argument_parser.add_argument(
        '--output-directory',
        default=Path().cwd(),
        help='directory to which to write configuration files (defaults to `.`)',
    )
    argument_parser.add_argument(
        '--skip-existing', action='store_true', help='skip existing files',
    )
    argument_parser.add_argument(
        '--absolute-paths',
        action='store_true',
        help='write paths as absolute in configuration',
    )
    argument_parser.add_argument(
        '--verbose', action='store_true', help='show more verbose log messages'
    )

    for extra_argument in extra_arguments:
        argument_parser.add_argument(f'--{extra_argument}')

    arguments, unknown_arguments = argument_parser.parse_known_args()

    extra_arguments = {
        extra_argument: convert_value(
            arguments.__dict__[extra_argument], extra_arguments[extra_argument]
        )
        for extra_argument in extra_arguments
    }

    platform = convert_value(arguments.platform, Platform)
    mesh_directory = convert_value(arguments.mesh_directory, Path).resolve().absolute()

    modeled_start_time = convert_value(arguments.modeled_start_time, datetime)
    modeled_duration = convert_value(arguments.modeled_duration, timedelta)
    modeled_timestep = convert_value(arguments.modeled_timestep, timedelta)
    nems_interval = convert_value(arguments.nems_interval, timedelta)

    adcirc_processors = convert_value(arguments.adcirc_processors, int)

    modulefile = convert_value(arguments.modulefile, Path)
    if modulefile is not None:
        modulefile = modulefile.resolve().absolute()

    forcings = arguments.forcings
    if forcings is not None:
        forcings = [forcing.strip() for forcing in forcings.split(',')]
    else:
        forcings = []

    adcirc_executable = convert_value(arguments.adcirc_executable, Path).resolve().absolute()
    adcprep_executable = convert_value(arguments.adcprep_executable, Path).resolve().absolute()
    aswip_executable = convert_value(arguments.aswip_executable, Path).resolve().absolute()

    job_duration = convert_value(arguments.job_duration, timedelta)
    output_directory = convert_value(arguments.output_directory, Path).resolve().absolute()

    absolute_paths = arguments.absolute_paths
    overwrite = not arguments.skip_existing
    verbose = arguments.verbose

    arguments = {}
    unrecognized_arguments = []
    for index in range(len(unknown_arguments)):
        argument = unknown_arguments[index]
        value = None
        if argument.startswith('-'):
            parsed_argument = argument.strip('-').strip()
            if len(unknown_arguments) > index + 1 and not unknown_arguments[
                index + 1
            ].startswith('-'):
                value = unknown_arguments[index + 1].strip()
            forcing = parsed_argument.split('-')[0]
            if forcing not in forcings:
                if forcing.lower() in FORCING_NAMES:
                    forcings.append(forcing.lower())
                else:
                    unrecognized_arguments.append(argument)
            arguments[parsed_argument] = value

    if len(unrecognized_arguments) > 0:
        argument_parser.error(f'unrecognized arguments: {" ".join(unrecognized_arguments)}')

    unknown_arguments = arguments
    del arguments

    tidal_spinup_duration = None

    # initialize `adcircpy` forcing objects
    forcing_configurations = []
    for provided_name in forcings:
        if provided_name.lower() in FORCING_NAMES:
            forcing_configuration_class = ForcingConfigurations[provided_name.lower()].value
            kwargs = {}
            if issubclass(forcing_configuration_class, TidalForcingJSON):
                tidal_spinup_duration = get_argument(
                    argument=f'tidal-spinup-duration',
                    arguments=unknown_arguments,
                    required=True,
                    message=f'enter tidal spinup duration (`HH:MM:SS`): ',
                )
                tidal_spinup_duration = convert_value(tidal_spinup_duration, timedelta)
                tidal_source_options = '/'.join(
                    tidal_source.name.lower()
                    if tidal_source != DEFAULT_TIDAL_SOURCE
                    else tidal_source.name.upper()
                    for tidal_source in TidalSource
                )
                tidal_source = get_argument(
                    argument=f'tidal-source',
                    arguments=unknown_arguments,
                    required=True,
                    message=f'enter tidal forcing source ({tidal_source_options}): ',
                )
                if tidal_source is not None:
                    tidal_source = convert_value(tidal_source, TidalSource)
                else:
                    tidal_source = DEFAULT_TIDAL_SOURCE
                tidal_constituents = get_argument(
                    argument='tidal-constituents', arguments=unknown_arguments,
                )
                if tidal_constituents is not None:
                    tidal_constituents = [
                        entry.strip() for entry in tidal_constituents.split(',')
                    ]
                else:
                    tidal_constituents = DEFAULT_TIDAL_CONSTITUENTS
                kwargs['tidal_source'] = tidal_source
                kwargs['constituents'] = tidal_constituents
            if issubclass(forcing_configuration_class, BestTrackForcingJSON):
                best_track_storm_id = get_argument(
                    argument='besttrack-storm-id',
                    arguments=unknown_arguments,
                    required=True,
                    message='enter storm ID for best track: ',
                )
                kwargs['storm_id'] = best_track_storm_id
                best_track_start_date = get_argument(
                    argument='besttrack-start-date', arguments=unknown_arguments,
                )
                kwargs['start_date'] = best_track_start_date
                best_track_end_date = get_argument(
                    argument='besttrack-end-date', arguments=unknown_arguments,
                )
                kwargs['end_date'] = best_track_end_date

            if issubclass(forcing_configuration_class, FileForcingJSON):
                forcing_path = get_argument(
                    argument=f'{provided_name}-path', arguments=unknown_arguments,
                )
                kwargs['resource'] = forcing_path
            if issubclass(forcing_configuration_class, NEMSCapJSON):
                nems_cap_processors = get_argument(
                    argument=f'{provided_name}-processors', arguments=unknown_arguments,
                )
                kwargs['processors'] = nems_cap_processors
                nems_cap_parameters = get_argument(
                    argument=f'{provided_name}-nems-parameters', arguments=unknown_arguments,
                )
                kwargs['nems_parameters'] = nems_cap_parameters
            if issubclass(forcing_configuration_class, TimestepForcingJSON):
                forcing_interval = get_argument(
                    argument=f'{provided_name}-interval', arguments=unknown_arguments,
                )
                if forcing_interval is None:
                    forcing_interval = nems_interval
                kwargs['interval'] = forcing_interval
            if issubclass(forcing_configuration_class, WindForcingJSON):
                forcing_nws = get_argument(
                    argument=f'{provided_name}-nws', arguments=unknown_arguments,
                )
                kwargs['nws'] = forcing_nws
            if issubclass(forcing_configuration_class, WaveForcingJSON):
                forcing_nrs = get_argument(
                    argument=f'{provided_name}-nrs', arguments=unknown_arguments,
                )
                kwargs['nrs'] = forcing_nrs

            forcing_configurations.append(forcing_configuration_class(**kwargs))
        else:
            raise NotImplementedError(
                f'unrecognized forcing "{provided_name}"; must be from {FORCING_NAMES}'
            )

    return {
        'platform': platform,
        'mesh_directory': mesh_directory,
        'modeled_start_time': modeled_start_time,
        'modeled_duration': modeled_duration,
        'modeled_timestep': modeled_timestep,
        'nems_interval': nems_interval,
        'tidal_spinup_duration': tidal_spinup_duration,
        'perturbations': None,
        'modulefile': modulefile,
        'forcings': forcing_configurations,
        'adcirc_executable': adcirc_executable,
        'adcprep_executable': adcprep_executable,
        'aswip_executable': aswip_executable,
        'adcirc_processors': adcirc_processors,
        'job_duration': job_duration,
        'output_directory': output_directory,
        'absolute_paths': absolute_paths,
        'overwrite': overwrite,
        'verbose': verbose,
        **{extra_argument: value for extra_argument, value in extra_arguments.items()},
    }


def initialize_adcirc(
    platform: Platform,
    mesh_directory: os.PathLike,
    modeled_start_time: datetime,
    modeled_duration: timedelta,
    modeled_timestep: timedelta,
    tidal_spinup_duration: timedelta = None,
    perturbations: {str: {str: Any}} = None,
    nems_interval: timedelta = None,
    nems_connections: [str] = None,
    nems_mediations: [str] = None,
    nems_sequence: [str] = None,
    modulefile: os.PathLike = None,
    forcings: [ForcingJSON] = None,
    adcirc_executable: os.PathLike = None,
    adcprep_executable: os.PathLike = None,
    aswip_executable: os.PathLike = None,
    adcirc_processors: int = None,
    job_duration: timedelta = None,
    output_directory: os.PathLike = None,
    absolute_paths: bool = True,
    overwrite: bool = None,
    verbose: bool = False,
):
    logger = get_logger(
        'initialize_adcirc', console_level=logging.DEBUG if verbose else logging.INFO
    )

    if nems_interval is not None:
        configuration = NEMSADCIRCRunConfiguration(
            mesh_directory=mesh_directory,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_start_time + modeled_duration,
            modeled_timestep=modeled_timestep,
            nems_interval=nems_interval,
            nems_connections=nems_connections,
            nems_mediations=nems_mediations,
            nems_sequence=nems_sequence,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            perturbations=perturbations,
            forcings=forcings,
            adcirc_processors=adcirc_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            nems_executable=adcirc_executable,
            adcprep_executable=adcprep_executable,
            aswip_executable=aswip_executable,
            source_filename=modulefile,
        )
    else:
        configuration = ADCIRCRunConfiguration(
            mesh_directory=mesh_directory,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_start_time + modeled_duration,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            perturbations=perturbations,
            forcings=forcings,
            adcirc_processors=adcirc_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            adcirc_executable=adcirc_executable,
            adcprep_executable=adcprep_executable,
            aswip_executable=aswip_executable,
            source_filename=modulefile,
        )

    if not absolute_paths:
        output_directory = Path(os.path.relpath(output_directory, Path.cwd()))
        configuration.relative_to(output_directory, inplace=True)

    components = [
        configuration_json.name for configuration_json in configuration.configurations
    ]

    logger.info(f'writing {"+".join(components)} configuration to "{output_directory}"')

    for configuration_json in configuration.configurations:
        logger.debug(repr(configuration_json))

    configuration.write_directory(
        directory=output_directory, overwrite=overwrite,
    )


def get_argument(
    argument: str, arguments: {str: str} = None, required: bool = False, message: str = None
) -> str:
    if message is None:
        message = f'enter value for "{argument}": '

    if argument in arguments:
        value = arguments[argument]
    elif required:
        value = input(message)
    else:
        value = ''

    if len(value.strip()) == 0:
        value = None

    return value


def main():
    initialize_adcirc(**parse_initialize_adcirc_arguments())


if __name__ == '__main__':
    main()
