from argparse import ArgumentParser
from datetime import datetime, timedelta
from enum import Enum
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

from typepigeon import convert_value
from pyschism.forcing.bctides.tides import TidalDatabase

from coupledmodeldriver import Platform
from coupledmodeldriver.configure import (
    ATMESHForcingJSON,
    BestTrackForcingJSON,
    OWIForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
    NationalWaterModelFocringJSON,
)
from coupledmodeldriver.configure.base import NEMSCapJSON
from coupledmodeldriver.configure.forcings.base import (
    FileForcingJSON,
    ForcingJSON,
    TimestepForcingJSON,
    WaveForcingJSON,
    WindForcingJSON,
)
from coupledmodeldriver.generate import (
    SCHISMRunConfiguration,
    # NEMSSCHISMRunConfiguration,
)
from coupledmodeldriver.generate.schism.script import SchismEnsembleGenerationJob
from coupledmodeldriver.utilities import get_logger


class ForcingConfigurations(Enum):
    tidal = TidalForcingJSON
    besttrack = BestTrackForcingJSON
    nwm = NationalWaterModelFocringJSON


FORCING_NAMES = list(entry.name for entry in ForcingConfigurations)
DEFAULT_TIDAL_SOURCE = TidalDatabase.TPXO
DEFAULT_TIDAL_CONSTITUENTS = 'all'


def parse_initialize_schism_arguments(
    extra_arguments: Dict[str, Tuple[type, str]] = None
) -> Dict[str, Any]:
    if extra_arguments is None:
        extra_arguments = {}
    elif not isinstance(extra_arguments, Mapping):
        extra_arguments = {extra_argument: (None, None) for extra_argument in extra_arguments}
    extra_arguments = {
        extra_argument.strip('-'): argument_info
        for extra_argument, argument_info in extra_arguments.items()
    }

    argument_parser = ArgumentParser()

    argument_parser.add_argument(
        '--platform', required=True, help='HPC platform for which to configure'
    )
    argument_parser.add_argument(
        '--mesh-directory',
        required=True,
        help='path to input mesh (`hgrid.gr3`, `manning.gr3` or `drag.gr3`)',
    )
    argument_parser.add_argument(
        '--modeled-start-time', required=True, help='start time within the modeled system'
    )
    argument_parser.add_argument(
        '--modeled-duration', required=True, help='duration within the modeled system'
    )
    argument_parser.add_argument(
        '--modeled-timestep', required=True, help='time interval within the modeled system'
    )
    argument_parser.add_argument(
        '--nems-interval', default=None, help='main loop interval of NEMS run'
    )
    argument_parser.add_argument(
        '--modulefile',
        default=None,
        help='path to modulefile to source before model execution`',
    )
    argument_parser.add_argument(
        '--forcings',
        default=None,
        help=f'comma-separated list of forcings to configure, from {FORCING_NAMES}',
    )
    argument_parser.add_argument(
        '--schism-executable',
        default='pschism-TVD_VL',
        help='filename of compiled `pschism-TVD_VL` or `NEMS.x`',
    )
    argument_parser.add_argument(
        '--schism-hotstart-combiner',
        default='combine_hotstart7',
        help='filename of compiled `combine_hotstart7` executable',
    )
    argument_parser.add_argument(
        '--schism-schout-combiner',
        default='combine_output11',
        help='filename of compiled `combine_output11` executable',
    )
    argument_parser.add_argument(
        '--schism-use-old-io',
        action='store_true',
        help='flag to indicate whether SCHISM executable uses old or new IO',
    )
    argument_parser.add_argument(
        '--schism-processors', default=11, help='numbers of processors to assign for SCHISM'
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

    # add extra arguments with bool types and descriptions
    for extra_argument, (argument_type, argument_description) in extra_arguments.items():
        kwargs = {}
        if argument_type is bool:
            kwargs['action'] = 'store_true'
        if argument_description is not None:
            kwargs['help'] = argument_description
        argument_parser.add_argument(f'--{extra_argument}', **kwargs)

    arguments, unknown_arguments = argument_parser.parse_known_args()

    # convert extra arguments to their given type
    extra_arguments = {
        extra_argument: convert_value(
            arguments.__dict__[extra_argument.replace('-', '_')], argument_info[0]
        )
        for extra_argument, argument_info in extra_arguments.items()
    }

    platform = convert_value(arguments.platform, Platform)
    mesh_directory = convert_value(arguments.mesh_directory, Path).resolve().absolute()

    modeled_start_time = convert_value(arguments.modeled_start_time, datetime)
    modeled_duration = convert_value(arguments.modeled_duration, timedelta)
    modeled_timestep = convert_value(arguments.modeled_timestep, timedelta)
    nems_interval = convert_value(arguments.nems_interval, timedelta)

    schism_processors = convert_value(arguments.schism_processors, int)

    modulefile = convert_value(arguments.modulefile, Path)
    if modulefile is not None:
        modulefile = modulefile.resolve().absolute()

    forcings = arguments.forcings
    if forcings is not None:
        forcings = [forcing.strip() for forcing in forcings.split(',')]
    else:
        forcings = []

    schism_executable = convert_value(arguments.schism_executable, Path).resolve().absolute()
    schism_hotstart_combiner = (
        convert_value(arguments.schism_hotstart_combiner, Path).resolve().absolute()
    )
    schism_schout_combiner = (
        convert_value(arguments.schism_schout_combiner, Path).resolve().absolute()
    )
    schism_use_old_io = convert_value(arguments.schism_use_old_io, bool)

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

    # initialize `pyschism` forcing objects
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
                    for tidal_source in TidalDatabase
                )
                tidal_source = get_argument(
                    argument=f'tidal-source',
                    arguments=unknown_arguments,
                    required=True,
                    message=f'enter tidal forcing source ({tidal_source_options}): ',
                )
                if tidal_source is not None:
                    tidal_source = convert_value(tidal_source, TidalDatabase)
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
                nhc_code = get_argument(
                    argument='besttrack-nhc-code',
                    arguments=unknown_arguments,
                    required=True,
                    message='enter NHC code of storm: ',
                )
                kwargs['nhc_code'] = nhc_code
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
        'schism_executable': schism_executable,
        'schism_hotstart_combiner': schism_hotstart_combiner,
        'schism_schout_combiner': schism_schout_combiner,
        'schism_use_old_io': schism_use_old_io,
        'schism_processors': schism_processors,
        'job_duration': job_duration,
        'output_directory': output_directory,
        'absolute_paths': absolute_paths,
        'overwrite': overwrite,
        'verbose': verbose,
        **{extra_argument: value for extra_argument, value in extra_arguments.items()},
    }


def initialize_schism(
    platform: Platform,
    mesh_directory: os.PathLike,
    modeled_start_time: datetime,
    modeled_duration: timedelta,
    modeled_timestep: timedelta,
    tidal_spinup_duration: timedelta = None,
    perturbations: Dict[str, Dict[str, Any]] = None,
    nems_interval: timedelta = None,
    nems_connections: List[str] = None,
    nems_mediations: List[str] = None,
    nems_sequence: List[str] = None,
    modulefile: os.PathLike = None,
    forcings: List[ForcingJSON] = None,
    schism_executable: os.PathLike = None,
    schism_hotstart_combiner: os.PathLike = None,
    schism_schout_combiner: os.PathLike = None,
    schism_use_old_io: bool = False,
    schism_processors: int = None,
    job_duration: timedelta = None,
    output_directory: os.PathLike = None,
    absolute_paths: bool = True,
    overwrite: bool = None,
    verbose: bool = False,
):
    """
    creates a set of JSON configuration files according to the given parameters

    :param platform: HPC platform for which to configure
    :param mesh_directory: path to input mesh (`fort.13`, `fort.14`)
    :param modeled_start_time: start time within the modeled system
    :param modeled_duration: duration within the modeled system
    :param modeled_timestep: time interval within the modeled system
    :param tidal_spinup_duration: duration of tidal spinup in model time
    :param perturbations: mapping of perturbation names to changed values
    :param nems_interval: modeled time interval of NEMS main loop
    :param nems_connections: list of NEMS connections as strings (i.e. ``ATM -> OCN``)
    :param nems_mediations: list of NEMS mediations, including functions
    :param nems_sequence: list of NEMS entries in sequence order
    :param modulefile: path to modulefile to source before model execution
    :param forcings: list of forcings to configure, from ['tidal', 'atmesh', 'besttrack', 'owi', 'ww3data']
    :param schism_executable: filename of compiled ``pschism-TVD_VL` `or ``NEMS.x``
    :param schism_hotstart_combiner: filename of compiled hotstart combiner
    :param schism_schout_combiner: filename of compiled SCHISM old output combiner
    :param schism_use_old_io: flag to indicate if the compiled SCHISM uses old IO
    :param schism_processors: numbers of processors to assign for SCHISM
    :param job_duration: wall clock time for job
    :param output_directory: directory to which to write configuration files (defaults to ``.``)
    :param absolute_paths: whether to use absolute paths in configuration
    :param overwrite: whether to overwrite existing files
    :param verbose: whether to log extra debugging messages
    """

    logger = get_logger(
        'initialize_schism', console_level=logging.DEBUG if verbose else logging.INFO
    )

    if nems_interval is not None:
        raise ValueError('NEMS run setup is not supported for SCHISM yet!')

        # configuration = NEMSSCHISMRunConfiguration(
        #     mesh_directory=mesh_directory,
        #     modeled_start_time=modeled_start_time,
        #     modeled_end_time=modeled_start_time + modeled_duration,
        #     modeled_timestep=modeled_timestep,
        #     nems_interval=nems_interval,
        #     nems_connections=nems_connections,
        #     nems_mediations=nems_mediations,
        #     nems_sequence=nems_sequence,
        #     tidal_spinup_duration=tidal_spinup_duration,
        #     platform=platform,
        #     perturbations=perturbations,
        #     forcings=forcings,
        #     schism_processors=schism_processors,
        #     slurm_partition=None,
        #     slurm_job_duration=job_duration,
        #     slurm_email_address=None,
        #     nems_executable=schism_executable,
        #     schism_hotstart_combiner=schism_hotstart_combiner,
        #     schism_schout_combiner=schism_schout_combiner,
        #     schism_use_old_io=schism_use_old_io,
        #     source_filename=modulefile,
        # )
    else:
        configuration = SCHISMRunConfiguration(
            mesh_directory=mesh_directory,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_start_time + modeled_duration,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            perturbations=perturbations,
            forcings=forcings,
            schism_processors=schism_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            schism_executable=schism_executable,
            schism_hotstart_combiner=schism_hotstart_combiner,
            schism_schout_combiner=schism_schout_combiner,
            schism_use_old_io=schism_use_old_io,
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
        directory=output_directory, absolute=absolute_paths, overwrite=overwrite,
    )

    if platform == Platform.HERA:
        partition = 'bigmem'
    else:
        partition = None

    generation_job_script = SchismEnsembleGenerationJob(
        platform=platform, parallel=True, slurm_partition=partition
    )
    generation_job_script.write(filename=output_directory / 'generate.job', overwrite=True)


def get_argument(
    argument: str,
    arguments: Dict[str, str] = None,
    required: bool = False,
    message: str = None,
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
    initialize_schism(**parse_initialize_schism_arguments())


if __name__ == '__main__':
    main()
