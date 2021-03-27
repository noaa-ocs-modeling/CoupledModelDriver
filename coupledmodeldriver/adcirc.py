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

from .configuration import (
    ADCIRCJSON,
    ATMESHForcingJSON,
    CoupledModelDriverJSON,
    Model, NEMSJSON,
    SlurmJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from .job_script import (
    AdcircMeshPartitionJob,
    AdcircRunJob,
    AdcircSetupScript,
    EnsembleCleanupScript,
    EnsembleRunScript,
    EnsembleSetupScript,
    SlurmEmailType,
)
from .platforms import Platform
from .utilities import LOGGER, create_symlink, get_logger

REQUIRED_CONFIGURATIONS = [
    CoupledModelDriverJSON,
    SlurmJSON,
    NEMSJSON,
    ADCIRCJSON,
]

SUPPLEMENTARY_CONFIGURATIONS = [
    TidalForcingJSON,
    ATMESHForcingJSON,
    WW3DATAForcingJSON,
]


def generate_barebones_configuration(
    output_directory: PathLike,
    fort13_filename: PathLike,
    fort14_filename: PathLike,
    nems: ModelingSystem,
    platform: Platform,
    nems_executable: PathLike,
    adcprep_executable: PathLike,
    tidal_spinup_duration: timedelta = None,
    runs: {str: (float, str)} = None,
    job_duration: timedelta = None,
    partition: str = None,
    email_address: str = None,
):
    """
    Generate required configuration files for an coupled ADCIRC run.

    :param output_directory: path to store configuration files
    :param fort13_filename: path to input mesh values (`fort.13`)
    :param fort14_filename: path to input mesh nodes (`fort.14`)
    :param nems: NEMSpy ModelingSystem object, populated with models and connections
    :param platform: HPC platform for which to configure
    :param nems_executable: filename of compiled `NEMS.x`
    :param adcprep_executable: filename of compiled `adcprep`
    :param tidal_spinup_duration: spinup time for ADCIRC coldstart
    :param runs: dictionary of run name to run value and mesh attribute name
    :param job_duration: wall clock time of job
    :param partition: Slurm partition
    :param email_address: email address
    """

    if not isinstance(output_directory, Path):
        output_directory = Path(output_directory)
    if not output_directory.exists():
        output_directory.mkdir(parents=True, exist_ok=True)

    modeled_start_time = nems.start_time
    modeled_end_time = nems.end_time
    modeled_timestep = nems.interval

    nems_configuration = NEMSJSON.from_nemspy(nems, executable_path=nems_executable)

    slurm_configuration = SlurmJSON(
        account=platform.value['slurm_account'],
        tasks=nems.processors,
        partition=partition,
        job_duration=job_duration,
        email_address=email_address,
    )

    adcirc_configuration = ADCIRCJSON(
        adcprep_executable_path=adcprep_executable,
        modeled_start_time=modeled_start_time,
        modeled_end_time=modeled_end_time,
        modeled_timestep=modeled_timestep,
        fort_13_path=fort13_filename,
        fort_14_path=fort14_filename,
        tidal_spinup_duration=tidal_spinup_duration,
        slurm_configuration=slurm_configuration,
    )

    main_configuration = CoupledModelDriverJSON(
        platform=platform,
        output_directory=output_directory,
        models=['ADCIRC'],
        runs=runs,
    )

    configurations = [main_configuration, nems_configuration, slurm_configuration, adcirc_configuration]

    for configuration in configurations:
        configuration.to_file(output_directory, overwrite=True)

    return configurations


def write_adcirc_configurations(
    output_directory: PathLike,
    fort13_filename: PathLike,
    fort14_filename: PathLike,
    nems: ModelingSystem,
    platform: Platform,
    runs: {str: (float, str)},
    nems_executable: PathLike,
    adcprep_executable: PathLike,
    forcings: [Forcing] = None,
    spinup: timedelta = None,
    source_filename: PathLike = None,
    wall_clock_time: timedelta = None,
    partition: str = None,
    email_address: str = None,
    overwrite: bool = False,
    verbose: bool = False,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param output_directory: path to store run configuration
    :param fort13_filename: path to input mesh values (`fort.13`)
    :param fort14_filename: path to input mesh nodes (`fort.14`)
    :param nems: NEMSpy ModelingSystem object, populated with models and connections
    :param platform: HPC platform for which to configure
    :param runs: dictionary of run name to run value and mesh attribute name
    :param nems_executable: filename of compiled `NEMS.x`
    :param adcprep_executable: filename of compiled `adcprep`
    :param forcings: list of Forcing objects to apply to the mesh
    :param spinup: spinup time for ADCIRC coldstart
    :param source_filename: path to modulefile to `source`
    :param wall_clock_time: wall clock time of job
    :param partition: Slurm partition
    :param email_address: email address
    :param overwrite: whether to overwrite existing files
    :param verbose: show log messages
    """

    if not isinstance(output_directory, Path):
        output_directory = Path(output_directory)

    models = [Model.ADCIRC]

    configurations = {}
    for configuration in REQUIRED_CONFIGURATIONS:
        filename = output_directory / configuration.name
        if not filename.exists():
            raise FileNotFoundError(f'missing required configuration file "{filename}"')
        configurations[configuration.name] = configuration.from_file(filename)

    for configuration in SUPPLEMENTARY_CONFIGURATIONS:
        filename = output_directory / configuration.name
        if filename.exists():
            configurations[configuration.name] = configuration.from_file(filename)

    if not isinstance(fort14_filename, Path):
        fort14_filename = Path(fort14_filename)

    if not fort14_filename.exists():
        os.makedirs(fort14_filename, exist_ok=True)
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

    if not isinstance(platform, Platform):
        platform = Platform[str(platform).upper()]

    if 'ocn' not in nems or not isinstance(nems['ocn'], ADCIRCEntry):
        nems['ocn'] = ADCIRCEntry(11)

    if forcings is None:
        forcings = []

    if wall_clock_time is None:
        wall_clock_time = timedelta(minutes=30)

    if source_filename is None:
        source_filename = platform.value['source_filename']

    if verbose:
        get_logger(LOGGER.name, console_level=logging.DEBUG)

    LOGGER.info(
        f'generating {len(runs)} '
        f'"{platform.name.lower()}" configuration(s) in "{output_directory}"'
    )

    if source_filename is not None:
        LOGGER.debug(f'sourcing modules from "{source_filename}"')

    fort13_filename = fort14_filename / 'fort.13'
    fort14_filename = fort14_filename / 'fort.14'
    if not fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at ' f'"{fort14_filename}"')

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

    if fort13_filename.exists():
        mesh.import_nodal_attributes(fort13_filename)
    else:
        LOGGER.warning(f'mesh values (nodal attributes) not found at ' f'"{fort13_filename}"')

    if not mesh.has_nodal_attribute('primitive_weighting_in_continuity_equation'):
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
    LOGGER.info(
        f'wrote NEMS coldstart configuration: '
        f'{", ".join((filename.name for filename in coldstart_filenames))}'
    )

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
        LOGGER.info(
            f'writing NEMS hotstart configuration: '
            f'{", ".join((filename.name for filename in hotstart_filenames))}'
        )
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

    slurm_account = platform.value['slurm_account']

    adcprep_run_name = 'ADC_MESH_DECOMP'
    adcirc_coldstart_run_name = 'ADC_COLD_RUN'
    adcirc_hotstart_run_name = 'ADC_HOT_RUN'
    adcircpy_run_name = 'ADCIRCPY'

    mesh_partitioning_job_script_filename = (
        output_directory / f'job_adcprep_{platform.name.lower()}.job'
    )
    coldstart_setup_script_filename = output_directory / f'setup.sh.coldstart'
    coldstart_run_script_filename = (
        output_directory / f'job_nems_adcirc_{platform.name.lower()}.job.coldstart'
    )
    hotstart_setup_script_filename = output_directory / f'setup.sh.hotstart'
    hotstart_run_script_filename = (
        output_directory / f'job_nems_adcirc_{platform.name.lower()}.job.hotstart'
    )
    setup_script_filename = output_directory / f'setup_{platform.name.lower()}.sh'
    run_script_filename = output_directory / f'run_{platform.name.lower()}.sh'
    cleanup_script_filename = output_directory / f'cleanup.sh'

    LOGGER.debug(f'setting mesh partitioner "{adcprep_executable}"')
    adcprep_script = AdcircMeshPartitionJob(
        platform=platform,
        adcirc_mesh_partitions=nems['OCN'].processors,
        slurm_account=slurm_account,
        slurm_duration=wall_clock_time,
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
        f'writing mesh partitioning job script '
        f'"{mesh_partitioning_job_script_filename.name}"'
    )
    adcprep_script.write(mesh_partitioning_job_script_filename, overwrite=overwrite)

    coldstart_setup_script = AdcircSetupScript(
        nems_configure_filename=Path('..') / 'nems.configure.coldstart',
        model_configure_filename=Path('..') / 'model_configure.coldstart',
        config_rc_filename=Path('..') / 'config.rc.coldstart',
    )

    LOGGER.debug(
        f'writing coldstart setup script ' f'"{coldstart_setup_script_filename.name}"'
    )
    coldstart_setup_script.write(coldstart_setup_script_filename, overwrite=overwrite)

    LOGGER.debug(f'setting NEMS executable "{nems_executable}"')
    if spinup is not None:
        coldstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=spinup.processors,
            slurm_account=slurm_account,
            slurm_duration=wall_clock_time,
            slurm_run_name=adcirc_coldstart_run_name,
            nems_path=nems_executable,
            slurm_partition=partition,
            slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_coldstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_coldstart_run_name}.out.log',
            source_filename=source_filename,
        )
    else:
        coldstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=nems.processors,
            slurm_account=slurm_account,
            slurm_duration=wall_clock_time,
            slurm_run_name=adcirc_coldstart_run_name,
            nems_path=nems_executable,
            slurm_partition=partition,
            slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_coldstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_coldstart_run_name}.out.log',
            source_filename=source_filename,
        )

    LOGGER.debug(f'writing coldstart run script ' f'"{coldstart_run_script_filename.name}"')
    coldstart_run_script.write(coldstart_run_script_filename, overwrite=overwrite)

    if spinup is not None:
        hotstart_setup_script = AdcircSetupScript(
            nems_configure_filename=Path('../..') / 'nems.configure.hotstart',
            model_configure_filename=Path('../..') / 'model_configure.hotstart',
            config_rc_filename=Path('../..') / 'config.rc.hotstart',
            fort67_filename=Path('../..') / 'coldstart/fort.67.nc',
        )
        hotstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=nems.processors,
            slurm_account=slurm_account,
            slurm_duration=wall_clock_time,
            slurm_run_name=adcirc_hotstart_run_name,
            nems_path=nems_executable,
            slurm_partition=partition,
            slurm_email_type=SlurmEmailType.ALL if email_address is not None else None,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_hotstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_hotstart_run_name}.out.log',
            source_filename=source_filename,
        )

        LOGGER.debug(
            f'writing hotstart setup script ' f'"{hotstart_setup_script_filename.name}"'
        )
        hotstart_setup_script.write(hotstart_setup_script_filename, overwrite=overwrite)

        LOGGER.debug(f'writing hotstart run script ' f'"{hotstart_run_script_filename.name}"')
        hotstart_run_script.write(hotstart_run_script_filename, overwrite=overwrite)

    slurm = SlurmConfig(
        account=slurm_account,
        ntasks=nems.processors,
        run_name=adcircpy_run_name,
        partition=partition,
        walltime=wall_clock_time,
        nodes=coldstart_run_script.slurm_nodes,
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

    stations_filename = fort14_filename / 'stations.txt'
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
        LOGGER.info(f'using original mesh "{fort14_filename}"')
    else:
        LOGGER.info(f'rewriting original mesh "{fort14_filename}"')

    LOGGER.debug(f'writing coldstart configuration to ' f'"{coldstart_directory}"')
    driver.write(
        coldstart_directory,
        overwrite=overwrite,
        fort13=None if use_original_mesh else 'fort.13',
        fort14=None if use_original_mesh else 'fort.14',
        coldstart='fort.15',
        hotstart=None,
        driver=None,
    )
    if use_original_mesh:
        if fort13_filename.exists():
            create_symlink(fort13_filename, coldstart_directory / 'fort.13')
        create_symlink(fort14_filename, coldstart_directory / 'fort.14')

    for run_name, (value, attribute_name) in runs.items():
        run_directory = runs_directory / run_name
        LOGGER.debug(f'writing hotstart configuration to ' f'"{run_directory}"')
        if not isinstance(value, numpy.ndarray):
            value = numpy.full([len(driver.mesh.coords)], fill_value=value)
        if not driver.mesh.has_attribute(attribute_name):
            driver.mesh.add_attribute(attribute_name)
        driver.mesh.set_attribute(attribute_name, value)

        driver.write(
            run_directory,
            overwrite=overwrite,
            coldstart=None,
            fort13=None if use_original_mesh else 'fort.13',
            fort14=None if use_original_mesh else 'fort.14',
            hotstart='fort.15',
            driver=None,
        )
        if use_original_mesh:
            if fort13_filename.exists():
                create_symlink(fort13_filename, run_directory / 'fort.13')
            create_symlink(fort14_filename, run_directory / 'fort.14')

    LOGGER.debug(f'writing ensemble setup script ' f'"{setup_script_filename.name}"')
    setup_script = EnsembleSetupScript(platform)
    setup_script.write(setup_script_filename, overwrite=overwrite)

    LOGGER.info(f'writing ensemble run script "{run_script_filename.name}"')
    run_script = EnsembleRunScript(platform, setup_script_filename.name)
    run_script.write(run_script_filename, overwrite=overwrite)

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(f'writing cleanup script "{cleanup_script_filename.name}"')
    cleanup_script.write(cleanup_script_filename, overwrite=overwrite)
