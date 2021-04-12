from argparse import ArgumentParser
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

from adcircpy.forcing.tides.tides import TidalSource

from coupledmodeldriver import Platform
from coupledmodeldriver.configure.forcings.base import (
    ATMESHForcingJSON,
    BestTrackForcingJSON,
    FileForcingJSON,
    OWIForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from coupledmodeldriver.generate import (
    ADCIRCGenerationScript,
    ADCIRCRunConfiguration,
    NEMSADCIRCRunConfiguration,
)
from coupledmodeldriver.utilities import convert_value


class ForcingConfigurations(Enum):
    tidal = TidalForcingJSON
    atmesh = ATMESHForcingJSON
    besttrack = BestTrackForcingJSON
    owi = OWIForcingJSON
    ww3data = WW3DATAForcingJSON


FORCING_NAMES = list(entry.name for entry in ForcingConfigurations)

DEFAULT_TIDAL_SOURCE = TidalSource.TPXO
DEFAULT_TIDAL_CONSTITUENTS = 'all'


def main():
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
        '--tidal-spinup-duration', default=None, help='spinup time for ADCIRC tidal coldstart'
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
        '--generate-script',
        action='store_true',
        help='write a Python script to load configuration',
    )
    argument_parser.add_argument(
        '--skip-existing', action='store_true', help='skip existing files',
    )

    arguments, extra_arguments = argument_parser.parse_known_args()

    platform = convert_value(arguments.platform, Platform)
    mesh_directory = convert_value(arguments.mesh_directory, Path)

    modeled_start_time = convert_value(arguments.modeled_start_time, datetime)
    modeled_duration = convert_value(arguments.modeled_duration, timedelta)
    modeled_timestep = convert_value(arguments.modeled_timestep, timedelta)
    nems_interval = convert_value(arguments.nems_interval, timedelta)

    adcirc_processors = convert_value(arguments.adcirc_processors, int)

    modulefile = convert_value(arguments.modulefile, Path)

    tidal_spinup_duration = convert_value(arguments.tidal_spinup_duration, timedelta)
    forcings = arguments.forcings
    if forcings is not None:
        forcings = [forcing.strip() for forcing in forcings.split(',')]
    else:
        forcings = []

    adcirc_executable = convert_value(arguments.adcirc_executable, Path)
    adcprep_executable = convert_value(arguments.adcprep_executable, Path)

    job_duration = convert_value(arguments.job_duration, timedelta)
    output_directory = convert_value(arguments.output_directory, Path)

    overwrite = not arguments.skip_existing

    generate_script = arguments.generate_script

    if tidal_spinup_duration is not None and 'tidal' not in forcings:
        forcings.append('tidal')

    arguments = {}
    unrecognized_arguments = []
    for index in range(len(extra_arguments)):
        argument = extra_arguments[index]
        value = None
        if argument.startswith('-'):
            parsed_argument = argument.strip('-').strip()
            if len(extra_arguments) > index + 1 and not extra_arguments[index + 1].startswith(
                '-'
            ):
                value = extra_arguments[index + 1].strip()
            forcing = parsed_argument.split('-')[0]
            if forcing not in forcings:
                if forcing.lower() in FORCING_NAMES:
                    forcings.append(forcing.lower())
                else:
                    unrecognized_arguments.append(argument)
            arguments[parsed_argument] = value

    if len(unrecognized_arguments) > 0:
        argument_parser.error(f'unrecognized arguments: {" ".join(unrecognized_arguments)}')

    extra_arguments = arguments
    del arguments

    # initialize `adcircpy` forcing objects
    forcing_configurations = []
    for provided_name in forcings:
        if provided_name.lower() in FORCING_NAMES:
            forcing_configuration_class = ForcingConfigurations[provided_name.lower()].value
            kwargs = {}
            if issubclass(forcing_configuration_class, TidalForcingJSON):
                if tidal_spinup_duration is None:
                    tidal_spinup_duration = input('enter tidal spinup duration (`HH:MM:SS`): ')
                    if len(tidal_spinup_duration.strip()) == 0:
                        tidal_spinup_duration = None
                    else:
                        tidal_spinup_duration = convert_value(tidal_spinup_duration, timedelta)

                if tidal_spinup_duration is not None:
                    argument = f'{provided_name}-source'
                    if argument in extra_arguments:
                        tidal_source = extra_arguments[argument]
                    else:
                        tidal_source = input(
                            f'enter tidal forcing source '
                            f'({"/".join(tidal_source.name.lower() if tidal_source != DEFAULT_TIDAL_SOURCE else tidal_source.name.upper() for tidal_source in TidalSource)}): '
                        )
                    if len(tidal_source.strip()) == 0:
                        tidal_source = None
                    else:
                        tidal_source = convert_value(tidal_source, TidalSource)
                    if tidal_source is None:
                        tidal_source = DEFAULT_TIDAL_SOURCE

                    argument = f'{provided_name}-constituents'
                    if argument in extra_arguments:
                        tidal_constituents = extra_arguments[argument]
                    else:
                        tidal_constituents = ''
                    if len(tidal_constituents.strip()) == 0:
                        tidal_constituents = None
                    else:
                        tidal_constituents = [
                            entry.strip() for entry in tidal_constituents.split(',')
                        ]
                    if tidal_constituents is None:
                        tidal_constituents = DEFAULT_TIDAL_CONSTITUENTS
                else:
                    tidal_source = None
                    tidal_constituents = None
                kwargs['tidal_source'] = tidal_source
                kwargs['constituents'] = tidal_constituents
            if issubclass(forcing_configuration_class, FileForcingJSON):
                argument = f'{provided_name}-path'
                if argument in extra_arguments:
                    forcing_path = extra_arguments[argument]
                else:
                    forcing_path = input(
                        f'enter resource path for ' f'"{forcing_configuration_class.name}": '
                    )
                kwargs['resource'] = forcing_path
            forcing_configurations.append(forcing_configuration_class(**kwargs))
        else:
            raise NotImplementedError(
                f'unrecognized forcing "{provided_name}"; ' f'must be from {FORCING_NAMES}'
            )

    if nems_interval is not None:
        configuration = NEMSADCIRCRunConfiguration(
            mesh_directory=mesh_directory,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_start_time + modeled_duration,
            modeled_timestep=modeled_timestep,
            nems_interval=nems_interval,
            nems_connections=None,
            nems_mediations=None,
            nems_sequence=None,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            runs=None,
            forcings=forcing_configurations,
            adcirc_processors=adcirc_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            nems_executable=adcirc_executable,
            adcprep_executable=adcprep_executable,
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
            runs=None,
            forcings=forcing_configurations,
            adcirc_processors=adcirc_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            adcirc_executable=adcirc_executable,
            adcprep_executable=adcprep_executable,
            source_filename=modulefile,
        )

    configuration.write_directory(
        directory=output_directory, overwrite=overwrite,
    )

    if generate_script:
        generation_script = ADCIRCGenerationScript()
        generation_script.write(filename=output_directory, overwrite=overwrite)


if __name__ == '__main__':
    main()
