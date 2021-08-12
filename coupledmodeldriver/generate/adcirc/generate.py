import asyncio
from concurrent.futures import ProcessPoolExecutor
from copy import copy, deepcopy
from datetime import timedelta
from enum import Enum
from functools import partial
import logging
import os
from os import PathLike
from pathlib import Path
from typing import Union

from nemspy import ModelingSystem

from coupledmodeldriver import Platform
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

    output_directory = output_directory.resolve()
    if not output_directory.is_absolute():
        output_directory = output_directory.absolute()

    if not output_directory.exists():
        os.makedirs(output_directory, exist_ok=True)

    if configuration_directory.absolute().resolve() != Path.cwd():
        starting_directory = Path.cwd()
        os.chdir(configuration_directory)
        configuration_directory = Path.cwd()
    else:
        starting_directory = None

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
        LOGGER.info(
            f'using original mesh from "{os.path.relpath(original_fort14_filename.resolve(), Path.cwd())}"'
        )
        if original_fort13_filename.exists():
            create_symlink(original_fort13_filename, local_fort13_filename)
        create_symlink(original_fort14_filename, local_fort14_filename)
    else:
        LOGGER.info(
            f'rewriting original mesh to "{os.path.relpath(local_fort14_filename.resolve(), Path.cwd())}"'
        )
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
    process_pool = ProcessPoolExecutor()
    futures = []

    if do_spinup:
        spinup_directory = output_directory / 'spinup'
        futures.append(
            event_loop.run_in_executor(
                process_pool,
                partial(
                    write_spinup_directory,
                    directory=spinup_directory,
                    configuration=copy(base_configuration),
                    duration=spinup_duration,
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
                ),
            )
        )
    else:
        spinup_directory = None

    runs_directory = output_directory / 'runs'
    if not runs_directory.exists():
        runs_directory.mkdir(parents=True, exist_ok=True)

    perturbations = base_configuration.perturb()

    LOGGER.info(
        f'generating {len(perturbations)} run configuration(s) in "{os.path.relpath(runs_directory.resolve(), Path.cwd())}"'
    )
    for run_name, run_configuration in perturbations.items():
        futures.append(
            event_loop.run_in_executor(
                process_pool,
                partial(
                    write_run_directory,
                    directory=runs_directory / run_name,
                    name=run_name,
                    phase=run_phase,
                    configuration=run_configuration,
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
                ),
            )
        )

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(
        f'writing cleanup script "{os.path.relpath(ensemble_cleanup_script_filename.resolve(), Path.cwd())}"'
    )
    cleanup_script.write(filename=ensemble_cleanup_script_filename, overwrite=overwrite)

    event_loop.run_until_complete(asyncio.gather(*futures, return_exceptions=False))

    LOGGER.info(
        f'writing ensemble run script "{os.path.relpath(ensemble_run_script_filename.resolve(), Path.cwd())}"'
    )
    run_job_script = EnsembleRunScript(
        platform=platform,
        commands=[
            'echo deleting previous ADCIRC output',
            f'sh {ensemble_cleanup_script_filename.name}',
        ],
        run_spinup=do_spinup,
    )
    run_job_script.write(ensemble_run_script_filename, overwrite=overwrite)

    if starting_directory is not None:
        os.chdir(starting_directory)


def write_spinup_directory(
    directory: PathLike,
    configuration: Union[ADCIRCRunConfiguration, NEMSADCIRCRunConfiguration],
    duration: timedelta,
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
    if not isinstance(directory, Path):
        directory = Path(directory)
    if not isinstance(local_fort13_filename, Path):
        local_fort13_filename = Path(local_fort13_filename)

    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)

    setup_job_name = 'ADCIRC_SETUP_SPINUP'
    job_name = 'ADCIRC_COLDSTART_SPINUP'

    adcircpy_driver = configuration.adcircpy_driver

    if relative_paths:
        configuration.relative_to(directory, inplace=True)

    if use_nems:
        nems = configuration.nemspy_modeling_system
        nems = ModelingSystem(
            nems.start_time - duration,
            nems.start_time,
            nems.interval,
            ocn=deepcopy(nems['OCN']),
            **nems.attributes,
        )
        processors = nems.processors
        model_executable = configuration['nems']['executable_path']
    else:
        nems = None
        processors = adcirc_processors
        model_executable = configuration['adcirc']['adcirc_executable_path']

    adcprep_path = configuration['adcirc']['adcprep_executable_path']
    aswip_path = configuration['adcirc']['aswip_executable_path']
    source_filename = configuration['adcirc']['source_filename']

    model_executable = update_path_relative(model_executable, relative_paths, directory)
    adcprep_path = update_path_relative(adcprep_path, relative_paths, directory)
    aswip_path = update_path_relative(aswip_path, relative_paths, directory)
    source_filename = update_path_relative(source_filename, relative_paths, directory)

    setup_script_filename = directory / 'setup.job'
    job_script_filename = directory / 'adcirc.job'

    if use_aswip:
        aswip_command = AswipCommand(path=aswip_path, nws=configuration['besttrack']['nws'])
    else:
        aswip_command = None

    setup_script = AdcircSetupJob(
        platform=platform,
        adcirc_mesh_partitions=adcirc_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_partition=partition,
        slurm_run_name=setup_job_name,
        adcprep_path=adcprep_path,
        aswip_command=aswip_command,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{setup_job_name}.err.log',
        slurm_log_filename=f'{setup_job_name}.out.log',
        source_filename=source_filename,
    )

    job_script = AdcircRunJob(
        platform=platform,
        slurm_tasks=processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_run_name=job_name,
        executable=model_executable,
        slurm_partition=partition,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{job_name}.err.log',
        slurm_log_filename=f'{job_name}.out.log',
        source_filename=source_filename,
    )

    setup_script.write(setup_script_filename, overwrite=overwrite)
    job_script.write(job_script_filename, overwrite=overwrite)

    if use_nems:
        LOGGER.debug(f'setting spinup to {duration}')

        nems.write(
            directory, overwrite=overwrite, include_version=True,
        )
        LOGGER.info(
            f'writing NEMS+ADCIRC tidal spinup configuration to "{os.path.relpath(directory.resolve(), Path.cwd())}"'
        )
    else:
        LOGGER.debug(
            f'writing ADCIRC tidal spinup configuration to "{os.path.relpath(directory.resolve(), Path.cwd())}"'
        )
    adcircpy_driver.write(
        directory,
        overwrite=overwrite,
        fort13=None if use_original_mesh else 'fort.13',
        fort14=None,
        coldstart='fort.15',
        hotstart=None,
        driver=None,
    )
    if use_original_mesh:
        if local_fort13_filename.exists():
            create_symlink(local_fort13_filename, directory / 'fort.13', relative=True)
    create_symlink(local_fort14_filename, directory / 'fort.14', relative=True)


def write_run_directory(
    directory: PathLike,
    name: str,
    phase: str,
    configuration: Union[ADCIRCRunConfiguration, NEMSADCIRCRunConfiguration],
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
    if not isinstance(directory, Path):
        directory = Path(directory)
    if spinup_directory is not None and not isinstance(spinup_directory, Path):
        spinup_directory = Path(spinup_directory)
    if not isinstance(local_fort13_filename, Path):
        local_fort13_filename = Path(local_fort13_filename)

    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
    LOGGER.debug(
        f'writing run configuration to "{os.path.relpath(directory.resolve(), Path.cwd())}"'
    )

    setup_job_name = f'ADCIRC_SETUP_{name}'
    job_name = f'ADCIRC_{phase}_{name}'

    adcircpy_driver = configuration.adcircpy_driver

    if relative_paths:
        configuration.relative_to(directory, inplace=True)

    if use_nems:
        nems = configuration.nemspy_modeling_system
        processors = nems.processors
        model_executable = configuration['nems']['executable_path']
    else:
        nems = None
        processors = adcirc_processors
        model_executable = configuration['adcirc']['adcirc_executable_path']

    adcprep_path = configuration['adcirc']['adcprep_executable_path']
    aswip_path = configuration['adcirc']['aswip_executable_path']
    source_filename = configuration['adcirc']['source_filename']

    model_executable = update_path_relative(model_executable, relative_paths, directory)
    adcprep_path = update_path_relative(adcprep_path, relative_paths, directory)
    aswip_path = update_path_relative(aswip_path, relative_paths, directory)
    source_filename = update_path_relative(source_filename, relative_paths, directory)

    setup_script_filename = directory / 'setup.job'
    job_script_filename = directory / 'adcirc.job'

    if use_aswip:
        aswip_command = AswipCommand(path=aswip_path, nws=configuration['besttrack']['nws'],)
    else:
        aswip_command = None

    setup_script = AdcircSetupJob(
        platform=platform,
        adcirc_mesh_partitions=adcirc_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_partition=partition,
        slurm_run_name=setup_job_name,
        adcprep_path=adcprep_path,
        aswip_command=aswip_command,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{setup_job_name}.err.log',
        slurm_log_filename=f'{setup_job_name}.out.log',
        source_filename=source_filename,
    )

    job_script = AdcircRunJob(
        platform=platform,
        slurm_tasks=processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_run_name=job_name,
        executable=model_executable,
        slurm_partition=partition,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{job_name}.err.log',
        slurm_log_filename=f'{job_name}.out.log',
        source_filename=source_filename,
    )

    setup_script.write(setup_script_filename, overwrite=overwrite)
    job_script.write(job_script_filename, overwrite=overwrite)

    if use_nems:
        nems.write(
            directory, overwrite=overwrite, include_version=True,
        )
        LOGGER.info(
            f'writing NEMS+ADCIRC run configuration to "{os.path.relpath(directory.resolve(), Path.cwd())}"'
        )
    else:
        LOGGER.debug(
            f'writing ADCIRC run configuration to "{os.path.relpath(directory.resolve(), Path.cwd())}"'
        )
    adcircpy_driver.write(
        directory,
        overwrite=overwrite,
        fort13=None if use_original_mesh else 'fort.13',
        fort14=None,
        coldstart=None,
        hotstart='fort.15',
        driver=None,
    )
    if use_original_mesh:
        if local_fort13_filename.exists():
            create_symlink(local_fort13_filename, directory / 'fort.13', relative=True)
    create_symlink(local_fort14_filename, directory / 'fort.14', relative=True)

    if do_spinup:
        for hotstart_filename in ['fort.67.nc', 'fort.68.nc']:
            try:
                create_symlink(
                    spinup_directory / hotstart_filename,
                    directory / hotstart_filename,
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
