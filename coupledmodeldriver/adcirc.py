import copy
from datetime import timedelta
from glob import glob
import os
from os import PathLike
from pathlib import Path
import re

from adcircpy import AdcircMesh, AdcircRun
from adcircpy.forcing.base import Forcing
from adcircpy.server import SlurmConfig
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry
import numpy

from .job_script import EnsembleSlurmScript, Platform, SlurmEmailType
from .utilities import get_logger

LOGGER = get_logger('configuration.adcirc')


def write_adcirc_configurations(
    nems: ModelingSystem,
    runs: {str: (float, str)},
    mesh_directory: PathLike,
    output_directory: PathLike,
    platform: Platform,
    name: str = None,
    partition: str = None,
    email_address: str = None,
    wall_clock_time: timedelta = None,
    spinup: timedelta = None,
    forcings: [Forcing] = None,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param runs: dictionary of run name to run value and mesh attribute name
    :param nems: NEMSpy ModelingSystem object, populated with models and connections
    :param mesh_directory: path to directory containing input mesh (fort.13 and fort.14) as well as stations file if applicable
    :param output_directory: path to store run configuration
    :param platform: HPC platform for which to configure
    :param name: name of this perturbation
    :param partition: Slurm partition
    :param email_address: email address
    :param wall_clock_time: wall clock time of job
    :param spinup: spinup time for ADCIRC coldstart
    """

    if not isinstance(mesh_directory, Path):
        mesh_directory = Path(mesh_directory)
    if not isinstance(output_directory, Path):
        output_directory = Path(output_directory)

    if not mesh_directory.exists():
        os.makedirs(mesh_directory, exist_ok=True)
    if not output_directory.exists():
        os.makedirs(output_directory, exist_ok=True)

    if name is None:
        name = 'nems_run'

    if 'ocn' not in nems or not isinstance(nems['ocn'], ADCIRCEntry):
        nems['ocn'] = ADCIRCEntry(11)

    if forcings is None:
        forcings = []

    if platform in [Platform.HERA, Platform.ORION]:
        launcher = 'srun'
    elif platform in [Platform.STAMPEDE2]:
        launcher = 'ibrun'
    else:
        launcher = ''

    run_name = 'ADCIRC_GAHM_GENERIC'

    if partition is None:
        partition = 'development'

    if wall_clock_time is None:
        wall_clock_time = timedelta(minutes=30)

    fort13_filename = mesh_directory / 'fort.13'
    if not fort13_filename.exists():
        LOGGER.warning(f'mesh values (nodal attributes) not found at {fort13_filename}')
    fort14_filename = mesh_directory / 'fort.14'
    if not fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at {fort14_filename}')

    if spinup is not None and isinstance(spinup, timedelta):
        spinup = ModelingSystem(
            nems.start_time - spinup,
            nems.start_time,
            nems.interval,
            ocn=copy.deepcopy(nems['OCN']),
            **nems.attributes,
        )

    # open mesh file
    mesh = AdcircMesh.open(fort14_filename, crs=4326)

    for forcing in forcings:
        mesh.add_forcing(forcing)

    mesh.generate_tau0()

    atm_namelist_filename = output_directory / 'atm_namelist.rc'

    if spinup is not None:
        coldstart_filenames = spinup.write(
            output_directory, overwrite=True, include_version=True
        )
    else:
        coldstart_filenames = nems.write(
            output_directory, overwrite=True, include_version=True
        )

    for filename in coldstart_filenames + [atm_namelist_filename]:
        coldstart_filename = Path(f'{filename}.coldstart')
        if coldstart_filename.exists():
            os.remove(coldstart_filename)
        filename.rename(coldstart_filename)

    if spinup is not None:
        hotstart_filenames = nems.write(output_directory, overwrite=True, include_version=True)
    else:
        hotstart_filenames = []

    for filename in hotstart_filenames + [atm_namelist_filename]:
        hotstart_filename = Path(f'{filename}.hotstart')
        if hotstart_filename.exists():
            os.remove(hotstart_filename)
        filename.rename(hotstart_filename)

    extra_commands = []
    if platform == Platform.STAMPEDE2:
        extra_commands.append('source /work/07531/zrb/stampede2/builds/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.stampede')
    elif platform == Platform.HERA:
        extra_commands.append(
            'source /scratch2/COASTAL/coastal/save/Zachary.Burnett/nems/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera')

    ensemble_slurm_script = EnsembleSlurmScript(
        account=None,
        tasks=nems.processors,
        duration=wall_clock_time,
        nodes=int(numpy.ceil(nems.processors / 68)) if platform == Platform.STAMPEDE2 else None,
        partition=partition,
        platform=platform,
        launcher=launcher,
        run=name,
        email_type=SlurmEmailType.ALL if email_address is not None else None,
        email_address=email_address,
        error_filename=f'{name}.err.log',
        log_filename=f'{name}.out.log',
        modules=[],
        commands=extra_commands,
    )
    ensemble_slurm_script.write(output_directory, overwrite=True)

    slurm = SlurmConfig(
        account=None,
        ntasks=nems.processors,
        run_name=run_name,
        partition=partition,
        walltime=wall_clock_time,
        nodes=int(numpy.ceil(nems.processors / 68)) if platform == Platform.STAMPEDE2 else None,
        mail_type='all' if email_address is not None else None,
        mail_user=email_address,
        log_filename=f'{name}.out.log',
        modules=[],
        launcher=launcher,
        extra_commands=extra_commands,
    )

    # instantiate AdcircRun object.
    driver = AdcircRun(
        mesh=mesh,
        start_date=nems.start_time,
        end_date=nems.end_time,
        spinup_time=timedelta(days=5),
        server_config=slurm,
    )

    # spinup_start = spinup.start_time if spinup is not None else None
    # spinup_end = spinup.end_time if spinup is not None else None
    spinup_interval = spinup.interval if spinup is not None else None

    stations_filename = mesh_directory / 'stations.txt'
    if stations_filename.exists():
        driver.import_stations(stations_filename)
        driver.set_elevation_stations_output(nems.interval, spinup=spinup_interval)
        # spinup_start=spinup_start, spinup_end=spinup_end)
        driver.set_velocity_stations_output(nems.interval, spinup=spinup_interval)
        # spinup_start=spinup_start, spinup_end=spinup_end)

    driver.set_elevation_surface_output(nems.interval, spinup=spinup_interval)
    # spinup_start=spinup_start, spinup_end=spinup_end)
    driver.set_velocity_surface_output(nems.interval, spinup=spinup_interval)
    # spinup_start=spinup_start, spinup_end=spinup_end)

    coldstart_directory = output_directory / 'coldstart'
    runs_directory = output_directory / 'runs'

    for directory in [coldstart_directory, runs_directory]:
        if not directory.exists():
            directory.mkdir()

    for run_name, (value, attribute_name) in runs.items():
        run_directory = runs_directory / run_name
        LOGGER.info(f'writing config to "{run_directory}"')
        if not isinstance(value, numpy.ndarray):
            value = numpy.full([len(driver.mesh.coords)], fill_value=value)
        if not driver.mesh.has_attribute(attribute_name):
            driver.mesh.add_attribute(attribute_name)
        driver.mesh.set_attribute(attribute_name, value)
        driver.write(run_directory, overwrite=True)
        for phase in ['hotstart']:
            directory = run_directory / phase
            if not directory.exists():
                directory.mkdir()

    pattern = re.compile(' p*adcirc')
    replacement = ' NEMS.x'
    for job_filename in glob(str(output_directory / '**' / 'slurm.job'), recursive=True):
        with open(job_filename) as job_file:
            text = job_file.read()
        matched = pattern.search(text)
        if matched:
            LOGGER.debug(
                f'replacing `{matched.group(0)}` with `{replacement}`' f' in "{job_filename}"'
            )
            text = re.sub(pattern, replacement, text)
            with open(job_filename, 'w') as job_file:
                job_file.write(text)
