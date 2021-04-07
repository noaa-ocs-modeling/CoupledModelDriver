from argparse import ArgumentParser
from datetime import datetime, timedelta
from pathlib import Path

from adcircpy import Tides
from adcircpy.forcing.tides.tides import TidalSource

from coupledmodeldriver import Platform
from coupledmodeldriver.configure.forcings.base import FORCING_SOURCES, \
    FileForcingJSON
from coupledmodeldriver.generate import (
    ADCIRCGenerationScript,
    ADCIRCRunConfiguration,
    NEMSADCIRCGenerationScript,
    NEMSADCIRCRunConfiguration,
)
from coupledmodeldriver.utilities import convert_value


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
        '--tidal-forcing-source',
        default='TPXO',
        help=f'source of tidal forcing (can be one of `{[entry.name for entry in TidalSource]}`)',
    )
    argument_parser.add_argument(
        '--tidal-forcing-path', default=None, help='resource path for tidal forcing'
    )
    argument_parser.add_argument(
        '--forcings',
        default=None,
        help=f'comma-separated list of forcings to configure, from {list(FORCING_SOURCES)}',
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
        '--directory',
        default=Path().cwd(),
        help='directory to which to write configuration files',
    )
    argument_parser.add_argument(
        '--generate-script',
        action='store_true',
        help='write a Python script to load configuration',
    )

    arguments = argument_parser.parse_args()

    platform = convert_value(arguments.platform, Platform)
    mesh_directory = convert_value(arguments.mesh_directory, Path)

    modeled_start_time = convert_value(arguments.modeled_start_time, datetime)
    modeled_duration = convert_value(arguments.modeled_duration, timedelta)
    modeled_timestep = convert_value(arguments.modeled_timestep, timedelta)
    nems_interval = convert_value(arguments.nems_interval, timedelta)

    adcirc_processors = convert_value(arguments.adcirc_processors, int)

    modulefile = convert_value(arguments.modulefile, Path)

    tidal_spinup_duration = convert_value(arguments.tidal_spinup_duration, timedelta)
    tidal_source = convert_value(arguments.tidal_forcing_source, TidalSource)
    tidal_forcing_path = convert_value(arguments.tidal_forcing_path, Path)
    forcings = arguments.forcings
    if forcings is not None:
        forcings = [forcing.strip() for forcing in forcings.split(',')]
    else:
        forcings = []

    adcirc_executable = convert_value(arguments.adcirc_executable, Path)
    adcprep_executable = convert_value(arguments.adcprep_executable, Path)

    job_duration = convert_value(arguments.job_duration, timedelta)
    directory = convert_value(arguments.directory, Path)

    # initialize `adcircpy` forcing objects
    adcircpy_forcings = []

    if tidal_spinup_duration is not None:
        tidal_forcing = Tides(tidal_source=tidal_source, resource=tidal_forcing_path)
        tidal_forcing.use_all()
        adcircpy_forcings.append(tidal_forcing)

    for forcing in forcings:
        if forcing.upper() in FORCING_SOURCES:
            forcing_class = FORCING_SOURCES[forcing.upper()]
            kwargs = {}
            if issubclass(forcing_class, FileForcingJSON):
                kwargs['resource'] = input(f'enter "{forcing_class.name}" resource path: ')
            forcing = forcing_class(**kwargs)
            adcircpy_forcings.append(forcing)
        else:
            raise NotImplementedError(
                f'unrecognized forcing "{forcing}"; must be one of {list(FORCING_SOURCES)}'
            )

    fort13_filename = mesh_directory / 'fort.13'
    fort14_filename = mesh_directory / 'fort.14'

    if not fort13_filename.exists():
        fort13_filename = None

    if nems_interval is not None:
        configuration = NEMSADCIRCRunConfiguration(
            fort13=fort13_filename,
            fort14=fort14_filename,
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
            forcings=adcircpy_forcings,
            adcirc_processors=adcirc_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            nems_executable=adcirc_executable,
            adcprep_executable=adcprep_executable,
            source_filename=modulefile,
        )
        generation_script = NEMSADCIRCGenerationScript()
    else:
        configuration = ADCIRCRunConfiguration(
            fort13=fort13_filename,
            fort14=fort14_filename,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_start_time + modeled_duration,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            runs=None,
            forcings=adcircpy_forcings,
            adcirc_processors=adcirc_processors,
            slurm_partition=None,
            slurm_job_duration=job_duration,
            slurm_email_address=None,
            adcirc_executable=adcirc_executable,
            adcprep_executable=adcprep_executable,
            source_filename=modulefile,
        )
        generation_script = ADCIRCGenerationScript()

    configuration.write_directory(directory=directory, overwrite=True)

    if arguments.generate_script:
        generation_script.write(filename=directory / 'generate_adcirc.py', overwrite=True)


if __name__ == '__main__':
    main()
