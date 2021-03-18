import copy
from datetime import timedelta
import os
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun
from adcircpy.forcing.base import Forcing
from adcircpy.server import SlurmConfig
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry
import numpy

from .job_script import (AdcircMeshPartitionScript, AdcircRunScript,
                         AdcircSetupScript, EnsembleRunScript,
                         EnsembleSetupScript, Platform, SlurmEmailType)
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
    use_original_fort14: bool = False,
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
    :param use_original_fort14: whether to use the original `fort.14` file instead of rewriting with `adcircpy`
    """

    if not isinstance(mesh_directory, Path):
        mesh_directory = Path(mesh_directory)
    if not isinstance(output_directory, Path):
        output_directory = Path(output_directory)

    if not mesh_directory.exists():
        os.makedirs(mesh_directory, exist_ok=True)
    if not output_directory.exists():
        os.makedirs(output_directory, exist_ok=True)

    if 'ocn' not in nems or not isinstance(nems['ocn'], ADCIRCEntry):
        nems['ocn'] = ADCIRCEntry(11)

    if forcings is None:
        forcings = []

    if wall_clock_time is None:
        wall_clock_time = timedelta(minutes=30)

    if source_filename is not None:
        if platform == Platform.STAMPEDE2:
            source_filename = '/work/07531/zrb/stampede2/builds/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.stampede'
        elif platform == Platform.HERA:
            source_filename = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera'

    fort13_filename = mesh_directory / 'fort.13'
    if not fort13_filename.exists():
        LOGGER.warning(f'mesh values (nodal attributes) not found at "{fort13_filename}"')
    fort14_filename = mesh_directory / 'fort.14'
    if not fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at "{fort14_filename}"')

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

    generate_tau0 = True
    if fort13_filename.exists():
        with open(fort13_filename) as fort13_file:
            for line in fort13_file:
                if 'primitive_weighting_in_continuity_equation' in line:
                    generate_tau0 = False
                    break
    if generate_tau0:
        mesh.generate_tau0()

    atm_namelist_filename = output_directory / 'atm_namelist.rc'

    if spinup is not None:
        coldstart_filenames = spinup.write(
            output_directory, overwrite=overwrite, include_version=True
        )
    else:
        coldstart_filenames = nems.write(
            output_directory, overwrite=overwrite, include_version=True
        )

    coldstart_filenames.append(atm_namelist_filename)
    for filename in coldstart_filenames:
        coldstart_filename = Path(f'{filename}.coldstart')
        if coldstart_filename.exists():
            os.remove(coldstart_filename)
        if filename.is_symlink():
            target = filename.resolve()
            if target in coldstart_filenames:
                target = f'{target}.coldstart'
            create_symlink(target, coldstart_filename)
            os.remove(filename)
        else:
            filename.rename(coldstart_filename)

    if spinup is not None:
        hotstart_filenames = nems.write(
            output_directory, overwrite=overwrite, include_version=True
        )
    else:
        hotstart_filenames = []

    for filename in hotstart_filenames + [atm_namelist_filename]:
        hotstart_filename = Path(f'{filename}.hotstart')
        if hotstart_filename.exists():
            os.remove(hotstart_filename)
        if filename.is_symlink():
            target = filename.resolve()
            if target in hotstart_filenames:
                target = f'{target}.coldstart'
            create_symlink(target, hotstart_filename)
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
    )
    adcprep_script.write(
        output_directory / f'job_adcprep_{adcprep_script.platform.value}.job',
        overwrite=overwrite,
    )

    coldstart_setup_script = AdcircSetupScript(
        nems_configure_filename=output_directory / 'nems.configure.coldstart',
        model_configure_filename=output_directory / 'model_configure.coldstart',
        atm_namelist_rc_filename=output_directory / 'atm_namelist.rc.coldstart',
        config_rc_filename=output_directory / 'config.rc.coldstart',
    )
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
    coldstart_setup_script.write(
        output_directory / f'setup_coldstart.sh',
        overwrite=overwrite,
    )
    coldstart_run_script.write(
        output_directory / f'job_nems_adcirc_{coldstart_run_script.platform.value}.job.coldstart',
        overwrite=overwrite,
    )

    if spinup is not None:
        hotstart_setup_script = AdcircSetupScript(
            nems_configure_filename=output_directory / 'nems.configure.hotstart',
            model_configure_filename=output_directory / 'model_configure.hotstart',
            atm_namelist_rc_filename=output_directory / 'atm_namelist.rc.hotstart',
            config_rc_filename=output_directory / 'config.rc.hotstart',
            fort67_filename=coldstart_directory / 'fort.67.nc',
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
        hotstart_setup_script.write(
            output_directory / f'setup_hotstart.sh',
            overwrite=overwrite,
        )
        hotstart_run_script.write(
            output_directory / f'job_nems_adcirc_{hotstart_run_script.platform.value}.job.hotstart',
            overwrite=overwrite,
        )

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

    driver.write(
        coldstart_directory,
        overwrite=overwrite,
        fort14=None if use_original_fort14 else 'fort.14',
        coldstart='fort.15',
        hotstart=None,
        driver=None,
    )
    if use_original_fort14:
        create_symlink(fort14_filename, coldstart_directory / 'fort.14')

    for run_name, (value, attribute_name) in runs.items():
        run_directory = runs_directory / run_name
        LOGGER.info(f'writing config to "{run_directory}"')
        if not isinstance(value, numpy.ndarray):
            value = numpy.full([len(driver.mesh.coords)], fill_value=value)
        if not driver.mesh.has_attribute(attribute_name):
            driver.mesh.add_attribute(attribute_name)
        driver.mesh.set_attribute(attribute_name, value)

        driver.write(
            run_directory,
            overwrite=overwrite,
            coldstart=None,
            fort14=None if use_original_fort14 else 'fort.14',
            hotstart='fort.15',
            driver=None,
        )
        if use_original_fort14:
            create_symlink(fort14_filename, run_directory / 'fort.14')

    setup_script_name = f'setup_{platform.value}.sh'
    run_script_name = f'run_{platform.value}.sh'

    setup_script = EnsembleSetupScript(platform)
    run_script = EnsembleRunScript(platform, setup_script_name)

    setup_script.write(output_directory / setup_script_name, overwrite=overwrite)
    run_script.write(output_directory / run_script_name, overwrite=overwrite)
