from copy import deepcopy
from enum import Enum
import logging
import os
from os import PathLike
from pathlib import Path

from nemspy import ModelingSystem

from coupledmodeldriver.generate.adcirc.configure import (
    ADCIRCRunConfiguration,
    NEMSADCIRCRunConfiguration,
)
from coupledmodeldriver.generate.adcirc.script import AdcircMeshPartitionJob, AdcircRunJob
from coupledmodeldriver.script import EnsembleCleanupScript, EnsembleRunScript
from coupledmodeldriver.utilities import LOGGER, create_symlink, get_logger


class RunPhase(Enum):
    COLDSTART = 'coldstart'
    HOTSTART = 'hotstart'


def generate_adcirc_configuration(
    configuration_directory: PathLike,
    output_directory: PathLike = None,
    overwrite: bool = False,
    verbose: bool = False,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param configuration_directory: path containing JSON configuration files
    :param output_directory: path to store generated configuration files
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
    if not os.path.isabs(output_directory):
        output_directory = output_directory.absolute()

    starting_directory = Path.cwd()
    if configuration_directory.absolute() != starting_directory:
        LOGGER.debug(f'moving into "{configuration_directory}"')
        os.chdir(configuration_directory.absolute())
        configuration_directory = Path.cwd()
    else:
        starting_directory = None

    use_nems = 'configure_nems.json' in [
        filename.name.lower() for filename in configuration_directory.iterdir()
    ]

    if use_nems:
        LOGGER.debug(f'generating NEMS configuration')
        ensemble_configuration = NEMSADCIRCRunConfiguration.read_directory(
            configuration_directory
        )
    else:
        LOGGER.debug(f'generating ADCIRC-only configuration')
        ensemble_configuration = ADCIRCRunConfiguration.read_directory(configuration_directory)

    platform = ensemble_configuration['modeldriver']['platform']

    job_duration = ensemble_configuration['slurm']['job_duration']
    partition = ensemble_configuration['slurm']['partition']
    email_type = ensemble_configuration['slurm']['email_type']
    email_address = ensemble_configuration['slurm']['email_address']

    original_fort13_filename = ensemble_configuration['adcirc']['fort_13_path']
    original_fort14_filename = ensemble_configuration['adcirc']['fort_14_path']
    adcirc_executable_path = ensemble_configuration['adcirc']['adcirc_executable_path']
    adcprep_executable_path = ensemble_configuration['adcirc']['adcprep_executable_path']
    adcirc_processors = ensemble_configuration['adcirc']['processors']
    tidal_spinup_duration = ensemble_configuration['adcirc']['tidal_spinup_duration']
    source_filename = ensemble_configuration['adcirc']['source_filename']
    use_original_mesh = ensemble_configuration['adcirc']['use_original_mesh']

    if use_nems:
        nems_configuration = ensemble_configuration['nems'].nemspy_modeling_system
        run_processors = nems_configuration.processors
        run_executable = ensemble_configuration['nems']['executable_path']
    else:
        nems_configuration = None
        run_processors = adcirc_processors
        run_executable = adcirc_executable_path

    if source_filename is not None:
        LOGGER.debug(f'sourcing modules from "{source_filename}"')

    if original_fort14_filename is None or not original_fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at "{original_fort14_filename}"')

    adcprep_run_name = 'ADCIRC_MESH_PREP'
    adcprep_job_script_filename = output_directory / f'job_adcprep_{platform.name.lower()}.job'

    local_fort13_filename = output_directory / 'fort.13'
    local_fort14_filename = output_directory / 'fort.14'
    local_fort15_filename = output_directory / 'fort.15'

    spinup_tides = tidal_spinup_duration is not None

    if spinup_tides:
        run_phase = RunPhase.HOTSTART
        coldstart_run_name = 'ADCIRC_SPINUP'
        hotstart_run_name = 'ADCIRC_HOTSTART'
        coldstart_run_script_filename = (
            output_directory / f'job_adcirc_{platform.name.lower()}.job.spinup'
        )
        hotstart_run_script_filename = (
            output_directory / f'job_adcirc_{platform.name.lower()}.job.hotstart'
        )
        run_script_filename = hotstart_run_script_filename
    else:
        run_phase = RunPhase.COLDSTART
        coldstart_run_name = 'ADCIRC_COLDSTART'
        hotstart_run_name = None
        coldstart_run_script_filename = (
            output_directory / f'job_adcirc_{platform.name.lower()}.job.coldstart'
        )
        hotstart_run_script_filename = None
        run_script_filename = coldstart_run_script_filename

    if use_nems:
        if spinup_tides:
            LOGGER.debug(f'setting spinup to {tidal_spinup_duration}')
            tidal_spinup_nems_configuration = ModelingSystem(
                nems_configuration.start_time - tidal_spinup_duration,
                nems_configuration.start_time,
                nems_configuration.interval,
                ocn=deepcopy(nems_configuration['OCN']),
                **nems_configuration.attributes,
            )
            coldstart_filenames = tidal_spinup_nems_configuration.write(
                output_directory,
                overwrite=overwrite,
                include_version=True,
                create_atm_namelist_rc=False,
            )
        else:
            coldstart_filenames = nems_configuration.write(
                output_directory,
                overwrite=overwrite,
                include_version=True,
                create_atm_namelist_rc=False,
            )
        filenames = (f'"{filename.name}"' for filename in coldstart_filenames)
        LOGGER.info(f'writing NEMS coldstart configuration: {", ".join(filenames)}')

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

        if spinup_tides:
            hotstart_filenames = nems_configuration.write(
                output_directory,
                overwrite=overwrite,
                include_version=True,
                create_atm_namelist_rc=False,
            )
            filenames = (f'"{filename.name}"' for filename in hotstart_filenames)
            LOGGER.info(f'writing NEMS hotstart configuration: {", ".join(filenames)}')

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

    slurm_account = platform.value['slurm_account']

    ensemble_run_script_filename = output_directory / f'run_{platform.name.lower()}.sh'
    ensemble_cleanup_script_filename = output_directory / f'cleanup.sh'

    LOGGER.debug(f'setting mesh partitioner "{adcprep_executable_path}"')
    adcprep_script = AdcircMeshPartitionJob(
        platform=platform,
        adcirc_mesh_partitions=adcirc_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_partition=partition,
        slurm_run_name=adcprep_run_name,
        adcprep_path=adcprep_executable_path,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{adcprep_run_name}.err.log',
        slurm_log_filename=f'{adcprep_run_name}.out.log',
        source_filename=source_filename,
    )

    LOGGER.debug(f'writing mesh partitioning job script "{adcprep_job_script_filename.name}"')
    adcprep_script.write(adcprep_job_script_filename, overwrite=overwrite)

    LOGGER.debug(f'setting run executable "{run_executable}"')
    coldstart_run_script = AdcircRunJob(
        platform=platform,
        slurm_tasks=run_processors,
        slurm_account=slurm_account,
        slurm_duration=job_duration,
        slurm_run_name=coldstart_run_name,
        executable=run_executable,
        slurm_partition=partition,
        slurm_email_type=email_type,
        slurm_email_address=email_address,
        slurm_error_filename=f'{coldstart_run_name}.err.log',
        slurm_log_filename=f'{coldstart_run_name}.out.log',
        source_filename=source_filename,
    )

    LOGGER.debug(f'writing coldstart run script "{coldstart_run_script_filename.name}"')
    coldstart_run_script.write(coldstart_run_script_filename, overwrite=overwrite)

    if spinup_tides:
        hotstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=run_processors,
            slurm_account=slurm_account,
            slurm_duration=job_duration,
            slurm_run_name=hotstart_run_name,
            executable=run_executable,
            slurm_partition=partition,
            slurm_email_type=email_type,
            slurm_email_address=email_address,
            slurm_error_filename=f'{hotstart_run_name}.err.log',
            slurm_log_filename=f'{hotstart_run_name}.out.log',
            source_filename=source_filename,
        )

        LOGGER.debug(f'writing hotstart run script "{hotstart_run_script_filename.name}"')
        hotstart_run_script.write(hotstart_run_script_filename, overwrite=overwrite)

    try:
        # instantiate AdcircRun object.
        coldstart_driver = ensemble_configuration.adcircpy_driver
    finally:
        if starting_directory is not None:
            LOGGER.debug(f'moving out of "{configuration_directory}"')
            os.chdir(starting_directory)

    if use_original_mesh:
        LOGGER.info(f'using original mesh from "{original_fort14_filename}"')
        if original_fort13_filename.exists():
            create_symlink(original_fort13_filename, local_fort13_filename)
        create_symlink(original_fort14_filename, local_fort14_filename)
    else:
        LOGGER.info(f'rewriting original mesh to "{local_fort14_filename}"')
        coldstart_driver.write(
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

    if local_fort15_filename.exists():
        os.remove(local_fort15_filename)

    if spinup_tides:
        tidal_spinup_directory = output_directory / 'spinup'
        if not tidal_spinup_directory.exists():
            tidal_spinup_directory.mkdir(parents=True, exist_ok=True)

        LOGGER.debug(f'writing tidal spinup configuration to "{tidal_spinup_directory}"')
        coldstart_driver.write(
            tidal_spinup_directory,
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
                    local_fort13_filename, tidal_spinup_directory / 'fort.13', relative=True
                )
        create_symlink(
            local_fort14_filename, tidal_spinup_directory / 'fort.14', relative=True
        )
        create_symlink(
            adcprep_job_script_filename, tidal_spinup_directory / 'adcprep.job', relative=True,
        )
        create_symlink(
            coldstart_run_script_filename,
            tidal_spinup_directory / 'adcirc.job',
            relative=True,
        )
        if use_nems:
            create_symlink(
                output_directory / 'nems.configure.coldstart',
                tidal_spinup_directory / 'nems.configure',
                relative=True,
            )
            create_symlink(
                output_directory / 'model_configure.coldstart',
                tidal_spinup_directory / 'model_configure',
                relative=True,
            )
            create_symlink(
                output_directory / 'config.rc.coldstart',
                tidal_spinup_directory / 'config.rc',
                relative=True,
            )
    else:
        tidal_spinup_directory = None

    perturbations = ensemble_configuration.perturb(relative_path=3)

    runs_directory = output_directory / 'runs'
    if not runs_directory.exists():
        runs_directory.mkdir(parents=True, exist_ok=True)

    LOGGER.info(f'generating {len(perturbations)} run configuration(s) in "{runs_directory}"')
    for run_name, run_configuration in perturbations.items():
        run_directory = runs_directory / run_name
        LOGGER.debug(f'writing run configuration to "{run_directory}"')

        run_driver = run_configuration.adcircpy_driver

        run_driver.write(
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
        create_symlink(
            adcprep_job_script_filename, run_directory / 'adcprep.job', relative=True,
        )
        create_symlink(
            run_script_filename, run_directory / 'adcirc.job', relative=True,
        )
        if use_nems:
            create_symlink(
                output_directory / f'nems.configure.{run_phase.value}',
                run_directory / 'nems.configure',
                relative=True,
            )
            create_symlink(
                output_directory / f'model_configure.{run_phase.value}',
                run_directory / 'model_configure',
                relative=True,
            )
            create_symlink(
                output_directory / f'config.rc.{run_phase.value}',
                run_directory / 'config.rc',
                relative=True,
            )
        if spinup_tides:
            for hotstart_filename in ['fort.67.nc', 'fort.68.nc']:
                try:
                    create_symlink(
                        tidal_spinup_directory / hotstart_filename,
                        run_directory / hotstart_filename,
                        relative=True,
                    )
                except:
                    LOGGER.warning(
                        f'unable to link `{hotstart_filename}` from coldstart to hotstart; '
                        'you must manually link or copy this file after coldstart completes'
                    )

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(f'writing cleanup script "{ensemble_cleanup_script_filename.name}"')
    cleanup_script.write(filename=ensemble_cleanup_script_filename, overwrite=overwrite)

    LOGGER.info(f'writing ensemble run script "{ensemble_run_script_filename.name}"')
    run_script = EnsembleRunScript(
        platform=platform,
        commands=[
            'echo deleting previous ADCIRC output',
            f'sh {ensemble_cleanup_script_filename.name}',
        ],
        spinup=spinup_tides,
    )
    run_script.write(ensemble_run_script_filename, overwrite=overwrite)

    if starting_directory is not None:
        LOGGER.debug(f'moving out of "{configuration_directory}"')
        os.chdir(starting_directory)
