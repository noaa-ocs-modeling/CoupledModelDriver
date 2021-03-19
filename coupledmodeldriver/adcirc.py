import copy
from datetime import timedelta
import logging
import os
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun
from adcircpy.forcing.base import Forcing
from adcircpy.server import SlurmConfig
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry
import numpy

from .job_script import (
    AdcircMeshPartitionScript,
    AdcircRunScript,
    AdcircSetupScript,
    EnsembleRunScript,
    EnsembleSetupScript,
    Platform,
    SlurmEmailType,
)
from .utilities import create_symlink, get_logger

LOGGER = get_logger('adcirc')


def write_adcirc_configurations(
    nems: ModelingSystem,
    runs: {str: (float, str)},
    mesh_directory: PathLike,
    output_directory: PathLike,
    nems_executable: PathLike,
    adcprep_executable: PathLike,
    platform: Platform,
    partition: str = None,
    email_address: str = None,
    wall_clock_time: timedelta = None,
    spinup: timedelta = None,
    forcings: [Forcing] = None,
    overwrite: bool = False,
    source_filename: PathLike = None,
    use_original_mesh: bool = False,
    verbose: bool = False,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param runs: dictionary of run name to run value and mesh attribute name
    :param nems: NEMSpy ModelingSystem object, populated with models and connections
    :param mesh_directory: path to directory containing input mesh (fort.13 and fort.14) as well as stations file if applicable
    :param output_directory: path to store run configuration
    :param nems_executable: filename of compiled `NEMS.x`
    :param adcprep_executable: filename of compiled `adcprep`
    :param platform: HPC platform for which to configure
    :param partition: Slurm partition
    :param email_address: email address
    :param wall_clock_time: wall clock time of job
    :param spinup: spinup time for ADCIRC coldstart
    :param overwrite: whether to overwrite existing files
    :param source_filename: path to modulefile to `source`
    :param use_original_mesh: whether to use the original `fort.14` file instead of rewriting with `adcircpy`
    :param verbose: show log messages
    """

    if not isinstance(mesh_directory, Path):
        mesh_directory = Path(mesh_directory)
    if not isinstance(output_directory, Path):
        output_directory = Path(output_directory)

    if not mesh_directory.exists():
        os.makedirs(mesh_directory, exist_ok=True)
    if not output_directory.exists():
        os.makedirs(output_directory, exist_ok=True)

    if output_directory.is_absolute():
        output_directory = output_directory.resolve()
    else:
        output_directory = output_directory.resolve().relative_to(Path().cwd())

    if not isinstance(nems_executable, Path):
        nems_executable = Path(nems_executable)
    if not isinstance(adcprep_executable, Path):
        adcprep_executable = Path(adcprep_executable)

    if 'ocn' not in nems or not isinstance(nems['ocn'], ADCIRCEntry):
        nems['ocn'] = ADCIRCEntry(11)

    if forcings is None:
        forcings = []

    if wall_clock_time is None:
        wall_clock_time = timedelta(minutes=30)

    if source_filename is None:
        if platform == Platform.STAMPEDE2:
            source_filename = '/work/07531/zrb/stampede2/builds/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.stampede'
        elif platform == Platform.HERA:
            source_filename = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera'

    if verbose:
        get_logger('adcirc', console_level=logging.DEBUG)

    LOGGER.info(
        f'generating {len(runs)} "{platform.value}" configuration(s) in "{output_directory}"'
    )

    LOGGER.debug(f'setting NEMS executable "{nems_executable}"')
    LOGGER.debug(f'setting mesh partitioner "{adcprep_executable}"')

    if source_filename is not None:
        LOGGER.debug(f'sourcing modules from "{source_filename}"')

    fort13_filename = mesh_directory / 'fort.13'
    if not fort13_filename.exists():
        LOGGER.warning(f'mesh values (nodal attributes) not found at "{fort13_filename}"')
    fort14_filename = mesh_directory / 'fort.14'
    if not fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at "{fort14_filename}"')

    if spinup is not None and isinstance(spinup, timedelta):
        LOGGER.debug(f'setting spinup to {spinup}')
        spinup = ModelingSystem(
            nems.start_time - spinup,
            nems.start_time,
            nems.interval,
            ocn=copy.deepcopy(nems['OCN']),
            **nems.attributes,
        )

    # open mesh file
    LOGGER.info(f'opening mesh "{fort14_filename}"')
    mesh = AdcircMesh.open(fort14_filename, crs=4326)

    LOGGER.debug(f'adding {len(forcings)} forcing(s) to mesh')
    for forcing in forcings:
        mesh.add_forcing(forcing)

    generate_tau0 = True
    if fort13_filename.exists():
        with open(fort13_filename) as fort13_file:
            for line in fort13_file:
                if 'primitive_weighting_in_continuity_equation' in line:
                    generate_tau0 = False
                    break
    if generate_tau0:
        LOGGER.debug(f'generating tau0 in mesh')
        mesh.generate_tau0()

    if spinup is not None:
        coldstart_filenames = spinup.write(
            output_directory,
            overwrite=overwrite,
            include_version=True,
            create_atm_namelist_rc=False,
        )
    else:
        coldstart_filenames = nems.write(
            output_directory,
            overwrite=overwrite,
            include_version=True,
            create_atm_namelist_rc=False,
        )
    LOGGER.info(f'wrote NEMS coldstart configuration: {", ".join((filename.name for filename in coldstart_filenames))}')

    for filename in coldstart_filenames:
        coldstart_filename = Path(f'{filename}.coldstart')
        if coldstart_filename.exists():
            os.remove(coldstart_filename)
        if filename.absolute().is_symlink():
            target = filename.resolve()
            if target.absolute() in [value.resolve() for value in coldstart_filenames]:
                target = Path(f'{target}.coldstart')
            create_symlink(target, coldstart_filename, relative=True)
            os.remove(filename)
        else:
            filename.rename(coldstart_filename)

    if spinup is not None:
        hotstart_filenames = nems.write(
            output_directory,
            overwrite=overwrite,
            include_version=True,
            create_atm_namelist_rc=False,
        )
        LOGGER.info(f'writing NEMS hotstart configuration: {", ".join((filename.name for filename in hotstart_filenames))}')
    else:
        hotstart_filenames = []

    for filename in hotstart_filenames:
        hotstart_filename = Path(f'{filename}.hotstart')
        if hotstart_filename.exists():
            os.remove(hotstart_filename)
        if filename.absolute().is_symlink():
            target = filename.resolve()
            if target.absolute() in [value.resolve() for value in hotstart_filenames]:
                target = Path(f'{target}.hotstart')
            create_symlink(target, hotstart_filename, relative=True)
            os.remove(filename)
        else:
            filename.rename(hotstart_filename)

    coldstart_directory = output_directory / 'coldstart'
    runs_directory = output_directory / 'runs'

    for directory in [coldstart_directory, runs_directory]:
        if not directory.exists():
            directory.mkdir()

    slurm_account = 'coastal'
    slurm_nodes = (
        int(numpy.ceil(nems.processors / 68)) if platform == Platform.STAMPEDE2 else None
    )

    adcprep_run_name = 'ADCIRC_MESH_PARTITION'
    adcirc_coldstart_run_name = 'ADCIRC_COLDSTART'
    adcirc_hotstart_run_name = 'ADCIRC_HOTSTART'
    adcircpy_run_name = 'ADCIRCPY'

    mesh_partitioning_job_script_filename = (
        output_directory / f'job_adcprep_{platform.value}.job'
    )
    coldstart_setup_script_filename = output_directory / f'setup.sh.coldstart'
    coldstart_run_script_filename = (
        output_directory / f'job_nems_adcirc_{platform.value}.job.coldstart'
    )
    hotstart_setup_script_filename = output_directory / f'setup.sh.hotstart'
    hotstart_run_script_filename = (
        output_directory / f'job_nems_adcirc_{platform.value}.job.hotstart'
    )
    setup_script_filename = output_directory / f'setup_{platform.value}.sh'
    run_script_filename = output_directory / f'run_{platform.value}.sh'

    adcprep_script = AdcircMeshPartitionScript(
        platform=platform,
        adcirc_mesh_partitions=spinup.processors,
        slurm_account=slurm_account,
        slurm_duration=wall_clock_time,
        slurm_nodes=slurm_nodes,
        slurm_partition=partition,
        slurm_run_name=adcprep_run_name,
        adcprep_path=adcprep_executable,
        slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
        slurm_email_address=email_address,
        slurm_error_filename=f'{adcprep_run_name}.err.log',
        slurm_log_filename=f'{adcprep_run_name}.out.log',
        source_filename=source_filename,
    )

    LOGGER.debug(
        f'writing mesh partitioning job script "{mesh_partitioning_job_script_filename.name}"'
    )
    adcprep_script.write(mesh_partitioning_job_script_filename, overwrite=overwrite)

    coldstart_setup_script = AdcircSetupScript(
        nems_configure_filename=Path('..') / 'nems.configure.coldstart',
        model_configure_filename=Path('..') / 'model_configure.coldstart',
        config_rc_filename=Path('..') / 'config.rc.coldstart',
    )

    LOGGER.debug(f'writing coldstart setup script "{coldstart_setup_script_filename.name}"')
    coldstart_setup_script.write(coldstart_setup_script_filename, overwrite=overwrite)

    if spinup is not None:
        coldstart_run_script = AdcircRunScript(
            platform=platform,
            slurm_tasks=spinup.processors,
            slurm_account=slurm_account,
            slurm_duration=wall_clock_time,
            slurm_run_name=adcirc_coldstart_run_name,
            nems_path=nems_executable,
            slurm_nodes=slurm_nodes,
            slurm_partition=partition,
            slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_coldstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_coldstart_run_name}.out.log',
            source_filename=source_filename,
        )
    else:
        coldstart_run_script = AdcircRunScript(
            platform=platform,
            slurm_tasks=nems.processors,
            slurm_account=slurm_account,
            slurm_duration=wall_clock_time,
            slurm_run_name=adcirc_coldstart_run_name,
            slurm_nodes=slurm_nodes,
            slurm_partition=partition,
            slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_coldstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_coldstart_run_name}.out.log',
            source_filename=source_filename,
        )

    LOGGER.debug(f'writing coldstart run script "{coldstart_run_script_filename.name}"')
    coldstart_run_script.write(coldstart_run_script_filename, overwrite=overwrite)

    if spinup is not None:
        hotstart_setup_script = AdcircSetupScript(
            nems_configure_filename=Path('../..') / 'nems.configure.hotstart',
            model_configure_filename=Path('../..') / 'model_configure.hotstart',
            config_rc_filename=Path('../..') / 'config.rc.hotstart',
            fort67_filename=Path('../..') / 'coldstart/fort.67.nc',
        )
        hotstart_run_script = AdcircRunScript(
            platform=platform,
            slurm_tasks=nems.processors,
            slurm_account=slurm_account,
            slurm_duration=wall_clock_time,
            slurm_run_name=adcirc_hotstart_run_name,
            nems_path=nems_executable,
            slurm_nodes=slurm_nodes,
            slurm_partition=partition,
            slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_hotstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_hotstart_run_name}.out.log',
            source_filename=source_filename,
        )

        LOGGER.debug(f'writing hotstart setup script "{hotstart_setup_script_filename.name}"')
        hotstart_setup_script.write(hotstart_setup_script_filename, overwrite=overwrite)

        LOGGER.debug(f'writing hotstart run script "{hotstart_run_script_filename.name}"')
        hotstart_run_script.write(hotstart_run_script_filename, overwrite=overwrite)

    slurm = SlurmConfig(
        account=slurm_account,
        ntasks=nems.processors,
        run_name=adcircpy_run_name,
        partition=partition,
        walltime=wall_clock_time,
        nodes=int(numpy.ceil(nems.processors / 68))
        if platform == Platform.STAMPEDE2
        else None,
        mail_type='all' if email_address is not None else None,
        mail_user=email_address,
        log_filename=f'{adcircpy_run_name}.out.log',
        modules=[],
        launcher=coldstart_run_script.launcher,
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

    if use_original_mesh:
        LOGGER.debug(f'using original mesh "{fort14_filename}"')

    LOGGER.debug(f'writing coldstart configuration to "{coldstart_directory}"')
    driver.write(
        coldstart_directory,
        overwrite=overwrite,
        fort14=None if use_original_mesh else 'fort.14',
        coldstart='fort.15',
        hotstart=None,
        driver=None,
    )
    if use_original_mesh:
        create_symlink(fort14_filename, coldstart_directory / 'fort.14')

    for run_name, (value, attribute_name) in runs.items():
        run_directory = runs_directory / run_name
        LOGGER.debug(f'writing hotstart configuration to "{run_directory}"')
        if not isinstance(value, numpy.ndarray):
            value = numpy.full([len(driver.mesh.coords)], fill_value=value)
        if not driver.mesh.has_attribute(attribute_name):
            driver.mesh.add_attribute(attribute_name)
        driver.mesh.set_attribute(attribute_name, value)

        driver.write(
            run_directory,
            overwrite=overwrite,
            coldstart=None,
            fort14=None if use_original_mesh else 'fort.14',
            hotstart='fort.15',
            driver=None,
        )
        if use_original_mesh:
            create_symlink(fort14_filename, run_directory / 'fort.14')

    LOGGER.debug(f'writing ensemble setup script "{setup_script_filename.name}"')
    setup_script = EnsembleSetupScript(platform)
    setup_script.write(setup_script_filename, overwrite=overwrite)

    LOGGER.info(f'writing ensemble run script "{run_script_filename.name}"')
    run_script = EnsembleRunScript(platform, setup_script_filename.name)
    run_script.write(run_script_filename, overwrite=overwrite)
