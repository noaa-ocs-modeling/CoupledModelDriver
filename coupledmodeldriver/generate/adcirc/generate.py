import asyncio
from copy import copy, deepcopy
from datetime import timedelta
from enum import Enum
import logging
import os
from os import PathLike
from pathlib import Path

from nemspy import ModelingSystem

from coupledmodeldriver import Platform
from coupledmodeldriver.configure.configure import RunConfiguration
from coupledmodeldriver.generate.adcirc.configure import (
    ADCIRCRunConfiguration,
    NEMSADCIRCRunConfiguration,
)
from coupledmodeldriver.generate.adcirc.script import (
    AdcircRunJob,
    AdcircSetupJob,
    AswipCommand,
)
from coupledmodeldriver.script import EnsembleCleanupScript, EnsembleRunScript, SlurmEmailType
from coupledmodeldriver.utilities import create_symlink, get_logger, LOGGER


class RunPhase(Enum):
    COLDSTART = 'coldstart'
    HOTSTART = 'hotstart'


def generate_adcirc_configuration(
    configuration_directory: PathLike,
    output_directory: PathLike = None,
    relative_paths: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param configuration_directory: path containing JSON configuration files
    :param output_directory: path to store generated configuration files
    :param relative_paths: whether to write relative paths in generated configuration files
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
    if not output_directory.is_absolute():
        output_directory = output_directory.absolute()

    use_nems = 'configure_nems.json' in [
        filename.name.lower() for filename in configuration_directory.iterdir()
    ]

    if use_nems:
        LOGGER.debug(f'generating NEMS configuration')
        base_configuration = NEMSADCIRCRunConfiguration.read_directory(configuration_directory)
    else:
        LOGGER.debug(f'generating ADCIRC-only configuration')
        base_configuration = ADCIRCRunConfiguration.read_directory(configuration_directory)

    base_configuration.move_paths(configuration_directory)

    platform = base_configuration['modeldriver']['platform']

    job_duration = base_configuration['slurm']['job_duration']
    partition = base_configuration['slurm']['partition']
    email_type = base_configuration['slurm']['email_type']
    email_address = base_configuration['slurm']['email_address']

    original_fort13_filename = base_configuration['adcirc']['fort_13_path']
    original_fort14_filename = base_configuration['adcirc']['fort_14_path']
    adcirc_processors = base_configuration['adcirc']['processors']
    spinup_duration = base_configuration['adcirc']['tidal_spinup_duration']
    use_original_mesh = base_configuration['adcirc']['use_original_mesh']

    if original_fort14_filename is None or not original_fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at "{original_fort14_filename}"')

    local_fort13_filename = output_directory / 'fort.13'
    local_fort14_filename = output_directory / 'fort.14'
    local_fort15_filename = output_directory / 'fort.15'

    do_spinup = spinup_duration is not None

    run_phase = 'HOTSTART' if do_spinup else 'COLDSTART'

    slurm_account = platform.value['slurm_account']

    ensemble_run_script_filename = output_directory / f'run_{platform.name.lower()}.sh'
    ensemble_cleanup_script_filename = output_directory / f'cleanup.sh'

    if 'besttrack' in base_configuration:
        nws = base_configuration['besttrack']['nws']
        use_aswip = nws in [8, 19, 20, 21]
        if use_aswip and base_configuration['adcirc']['aswip_executable_path'] is None:
            use_aswip = False
            LOGGER.warning(
                f'wind parameter {nws} but no `aswip` executable given; `aswip` will not be used'
            )
    else:
        use_aswip = False

    if use_original_mesh:
        LOGGER.info(f'using original mesh from "{original_fort14_filename}"')
        if original_fort13_filename.exists():
            create_symlink(original_fort13_filename, local_fort13_filename)
        create_symlink(original_fort14_filename, local_fort14_filename)
    else:
        LOGGER.info(f'rewriting original mesh to "{local_fort14_filename}"')
        adcircpy_driver = base_configuration.adcircpy_driver
        try:
            adcircpy_driver.write(
                output_directory,
                overwrite=overwrite,
                fort13=None,
                fort14='fort.14',
                fort15='fort.15',
                fort22=None,
                coldstart=None,
                hotstart=None,
                driver=None,
            )
        except Exception as error:
            LOGGER.warning(error)

    if local_fort15_filename.exists():
        os.remove(local_fort15_filename)

    event_loop = asyncio.get_event_loop()

    if do_spinup:
        spinup_directory = output_directory / 'spinup'
        event_loop.create_task(
            write_spinup_directory(
                spinup_directory=spinup_directory,
                spinup_configuration=copy(base_configuration),
                spinup_duration=spinup_duration,
                relative_paths=relative_paths,
                overwrite=overwrite,
                use_original_mesh=use_original_mesh,
                local_fort13_filename=local_fort13_filename,
                local_fort14_filename=local_fort14_filename,
                platform=platform,
                adcirc_processors=adcirc_processors,
                slurm_account=slurm_account,
                job_duration=job_duration,
                partition=partition,
                use_aswip=use_aswip,
                email_type=email_type,
                email_address=email_address,
                use_nems=use_nems,
            )
        )
    else:
        spinup_directory = None

    runs_directory = output_directory / 'runs'
    if not runs_directory.exists():
        runs_directory.mkdir(parents=True, exist_ok=True)

    perturbations = base_configuration.perturb()

    LOGGER.info(f'generating {len(perturbations)} run configuration(s) in "{runs_directory}"')
    for run_name, run_configuration in perturbations.items():
        event_loop.create_task(
            write_run_directory(
                run_directory=runs_directory / run_name,
                run_name=run_name,
                run_phase=run_phase,
                run_configuration=run_configuration,
                relative_paths=relative_paths,
                overwrite=overwrite,
                use_original_mesh=use_original_mesh,
                local_fort13_filename=local_fort13_filename,
                local_fort14_filename=local_fort14_filename,
                platform=platform,
                adcirc_processors=adcirc_processors,
                slurm_account=slurm_account,
                job_duration=job_duration,
                partition=partition,
                use_aswip=use_aswip,
                email_type=email_type,
                email_address=email_address,
                use_nems=use_nems,
                do_spinup=do_spinup,
                spinup_directory=spinup_directory,
            )
        )

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(f'writing cleanup script "{ensemble_cleanup_script_filename.name}"')
    cleanup_script.write(filename=ensemble_cleanup_script_filename, overwrite=overwrite)

    event_loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(event_loop)))

    LOGGER.info(f'writing ensemble run script "{ensemble_run_script_filename.name}"')
    run_job_script = EnsembleRunScript(
        platform=platform,
        commands=[
            'echo deleting previous ADCIRC output',
            f'sh {ensemble_cleanup_script_filename.name}',
        ],
        run_spinup=do_spinup,
    )
    run_job_script.write(ensemble_run_script_filename, overwrite=overwrite)


async def write_spinup_directory(
    spinup_directory: PathLike,
    spinup_configuration: RunConfiguration,
    spinup_duration: timedelta,
    local_fort14_filename: PathLike,
    local_fort13_filename: PathLike = None,
    relative_paths: bool = False,
    overwrite: bool = False,
    use_original_mesh: bool = False,
    platform: Platform = None,
    adcirc_processors: int = None,
    slurm_account: str = None,
    job_duration: timedelta = None,
    partition: str = None,
    use_aswip: bool = False,
    email_type: SlurmEmailType = None,
    email_address: str = None,
    use_nems: bool = False,
):
    if not isinstance(spinup_directory, Path):
        spinup_directory = Path(spinup_directory)
    if not isinstance(local_fort13_filename, Path):
        local_fort13_filename = Path(local_fort13_filename)

    if not spinup_directory.exists():
        spinup_directory.mkdir(parents=True, exist_ok=True)

    setup_job_name = 'ADCIRC_SETUP_SPINUP'
    spinup_job_name = 'ADCIRC_COLDSTART_SPINUP'

    spinup_adcircpy_driver = spinup_configuration.adcircpy_driver

    spinup_configuration.relative_to(spinup_directory, inplace=True)

    if use_nems:
        spinup_nems = spinup_configuration['nems'].nemspy_modeling_system
        spinup_nems = ModelingSystem(
            spinup_nems.start_time - spinup_duration,
            spinup_nems.start_time,
            spinup_nems.interval,
            ocn=deepcopy(spinup_nems['OCN']),
            **spinup_nems.attributes,
        )
        spinup_processors = spinup_nems.processors
        spinup_model_executable = spinup_configuration['nems']['executable_path']
    else:
        spinup_nems = None
        spinup_processors = adcirc_processors
        spinup_model_executable = spinup_configuration['adcirc']['adcirc_executable_path']

    spinup_adcprep_path = spinup_configuration['adcirc']['adcprep_executable_path']
    spinup_aswip_path = spinup_configuration['adcirc']['aswip_executable_path']
    spinup_source_filename = spinup_configuration['adcirc']['source_filename']

    spinup_model_executable = update_path_relative(
        spinup_model_executable, relative_paths, spinup_directory
    )
    spinup_adcprep_path = update_path_relative(
        spinup_adcprep_path, relative_paths, spinup_directory
    )
    spinup_aswip_path = update_path_relative(
        spinup_aswip_path, relative_paths, spinup_directory
    )
    spinup_source_filename = update_path_relative(
        spinup_source_filename, relative_paths, spinup_directory
    )

    spinup_setup_script_filename = spinup_directory / 'setup.job'
    spinup_job_script_filename = spinup_directory / 'adcirc.job'

    if use_aswip:
        aswip_command = AswipCommand(
            path=spinup_aswip_path, nws=spinup_configuration['besttrack']['nws'],
        )
    else:
        aswip_command = None

    spinup_setup_script = AdcircSetupJob(
        platform=platform,
        adcirc_mesh_partitions=adcirc_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_partition=partition,
        slurm_run_name=setup_job_name,
        adcprep_path=spinup_adcprep_path,
        aswip_command=aswip_command,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{setup_job_name}.err.log',
        slurm_log_filename=f'{setup_job_name}.out.log',
        source_filename=spinup_source_filename,
    )

    spinup_job_script = AdcircRunJob(
        platform=platform,
        slurm_tasks=spinup_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_run_name=spinup_job_name,
        executable=spinup_model_executable,
        slurm_partition=partition,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{spinup_job_name}.err.log',
        slurm_log_filename=f'{spinup_job_name}.out.log',
        source_filename=spinup_source_filename,
    )

    spinup_setup_script.write(spinup_setup_script_filename, overwrite=overwrite)
    spinup_job_script.write(spinup_job_script_filename, overwrite=overwrite)

    if use_nems:
        LOGGER.debug(f'setting spinup to {spinup_duration}')

        spinup_nems_filenames = spinup_nems.write(
            spinup_directory, overwrite=overwrite, include_version=True,
        )
        spinup_nems_filenames = (f'"{filename.name}"' for filename in spinup_nems_filenames)
        LOGGER.info(
            f'writing NEMS coldstart configuration: {", ".join(spinup_nems_filenames)}'
        )

    LOGGER.debug(f'writing tidal spinup configuration to "{spinup_directory}"')
    try:
        spinup_adcircpy_driver.write(
            spinup_directory,
            overwrite=overwrite,
            fort13=None if use_original_mesh else 'fort.13',
            fort14=None,
            coldstart='fort.15',
            hotstart=None,
            driver=None,
        )
        if use_original_mesh:
            if local_fort13_filename.exists():
                create_symlink(
                    local_fort13_filename, spinup_directory / 'fort.13', relative=True
                )
        create_symlink(local_fort14_filename, spinup_directory / 'fort.14', relative=True)
    except Exception as error:
        LOGGER.warning(error)


async def write_run_directory(
    run_directory: PathLike,
    run_name: str,
    run_phase: str,
    run_configuration: RunConfiguration,
    local_fort14_filename: PathLike,
    local_fort13_filename: PathLike = None,
    relative_paths: bool = False,
    overwrite: bool = False,
    use_original_mesh: bool = False,
    platform: Platform = None,
    adcirc_processors: int = None,
    slurm_account: str = None,
    job_duration: timedelta = None,
    partition: str = None,
    use_aswip: bool = False,
    email_type: SlurmEmailType = None,
    email_address: str = None,
    use_nems: bool = False,
    do_spinup: bool = False,
    spinup_directory: PathLike = None,
):
    if not isinstance(run_directory, Path):
        run_directory = Path(run_directory)
    if spinup_directory is not None and not isinstance(spinup_directory, Path):
        spinup_directory = Path(spinup_directory)
    if not isinstance(local_fort13_filename, Path):
        local_fort13_filename = Path(local_fort13_filename)

    if not run_directory.exists():
        run_directory.mkdir(parents=True, exist_ok=True)
    LOGGER.debug(f'writing run configuration to "{run_directory}"')

    setup_job_name = f'ADCIRC_SETUP_{run_name}'
    run_job_name = f'ADCIRC_{run_phase}_{run_name}'

    run_adcircpy_driver = run_configuration.adcircpy_driver

    run_configuration.relative_to(run_directory, inplace=True)

    if use_nems:
        run_nems = run_configuration['nems'].nemspy_modeling_system
        run_processors = run_nems.processors
        run_model_executable = run_configuration['nems']['executable_path']
    else:
        run_nems = None
        run_processors = adcirc_processors
        run_model_executable = run_configuration['adcirc']['adcirc_executable_path']

    run_adcprep_path = run_configuration['adcirc']['adcprep_executable_path']
    run_aswip_path = run_configuration['adcirc']['aswip_executable_path']
    run_source_filename = run_configuration['adcirc']['source_filename']

    run_model_executable = update_path_relative(
        run_model_executable, relative_paths, run_directory
    )
    run_adcprep_path = update_path_relative(run_adcprep_path, relative_paths, run_directory)
    run_aswip_path = update_path_relative(run_aswip_path, relative_paths, run_directory)
    run_source_filename = update_path_relative(
        run_source_filename, relative_paths, run_directory
    )

    run_setup_script_filename = run_directory / 'setup.job'
    run_job_script_filename = run_directory / 'adcirc.job'

    if use_aswip:
        aswip_command = AswipCommand(
            path=run_aswip_path, nws=run_configuration['besttrack']['nws'],
        )
    else:
        aswip_command = None

    run_setup_script = AdcircSetupJob(
        platform=platform,
        adcirc_mesh_partitions=adcirc_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_partition=partition,
        slurm_run_name=setup_job_name,
        adcprep_path=run_adcprep_path,
        aswip_command=aswip_command,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{setup_job_name}.err.log',
        slurm_log_filename=f'{setup_job_name}.out.log',
        source_filename=run_source_filename,
    )

    run_job_script = AdcircRunJob(
        platform=platform,
        slurm_tasks=run_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_run_name=run_job_name,
        executable=run_model_executable,
        slurm_partition=partition,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{run_job_name}.err.log',
        slurm_log_filename=f'{run_job_name}.out.log',
        source_filename=run_source_filename,
    )

    run_setup_script.write(run_setup_script_filename, overwrite=overwrite)
    run_job_script.write(run_job_script_filename, overwrite=overwrite)

    if use_nems:
        run_nems_filenames = run_nems.write(
            run_directory, overwrite=overwrite, include_version=True,
        )
        run_nems_filenames = (f'"{filename.name}"' for filename in run_nems_filenames)
        LOGGER.info(f'writing NEMS hotstart configuration: {", ".join(run_nems_filenames)}')

    try:
        run_adcircpy_driver.write(
            run_directory,
            overwrite=overwrite,
            fort13=None if use_original_mesh else 'fort.13',
            fort14=None,
            coldstart=None,
            hotstart='fort.15',
            driver=None,
        )
        if use_original_mesh:
            if local_fort13_filename.exists():
                create_symlink(local_fort13_filename, run_directory / 'fort.13', relative=True)
        create_symlink(local_fort14_filename, run_directory / 'fort.14', relative=True)
    except Exception as error:
        LOGGER.warning(error)

    if do_spinup:
        for hotstart_filename in ['fort.67.nc', 'fort.68.nc']:
            try:
                create_symlink(
                    spinup_directory / hotstart_filename,
                    run_directory / hotstart_filename,
                    relative=True,
                )
            except:
                LOGGER.warning(
                    f'unable to link `{hotstart_filename}` from coldstart to hotstart; '
                    'you must manually link or copy this file after coldstart completes'
                )


def update_path_relative(
    path: PathLike, relative: bool = False, relative_directory: PathLike = None
) -> Path:
    if path is not None:
        if not isinstance(path, Path):
            path = Path(path)
        if relative_directory is None:
            relative_directory = Path.cwd()
        if relative:
            if not isinstance(relative_directory, Path):
                relative_directory = Path(relative_directory)
            if path.is_absolute():
                path = Path(os.path.relpath(path, relative_directory))
        elif not path.is_absolute():
            path = (relative_directory / path).resolve().absolute()
    return path
