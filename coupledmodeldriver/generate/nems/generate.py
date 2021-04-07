import copy
import logging
import os
from os import PathLike
from pathlib import Path

from nemspy import ModelingSystem

from coupledmodeldriver.script import (
    EnsembleCleanupScript,
    EnsembleRunScript,
)
from coupledmodeldriver.utilities import LOGGER, create_symlink, \
    get_logger
from .configure import NEMSADCIRCRunConfiguration
from ..adcirc.script import AdcircMeshPartitionJob, AdcircRunJob


def generate_nems_adcirc_configuration(
    output_directory: PathLike,
    configuration_directory: PathLike = None,
    overwrite: bool = False,
    verbose: bool = False,
):
    """
    Generate ADCIRC run configuration for given variable values.

    :param output_directory: path to store generated configuration files
    :param configuration_directory: path containing JSON configuration files
    :param overwrite: whether to overwrite existing files
    :param verbose: whether to show more verbose log messages
    """

    get_logger(LOGGER.name, console_level=logging.DEBUG if verbose else logging.INFO)

    if not isinstance(output_directory, Path):
        output_directory = Path(output_directory)

    if configuration_directory is None:
        configuration_directory = output_directory
    elif not isinstance(configuration_directory, Path):
        configuration_directory = Path(configuration_directory)

    if not output_directory.exists():
        os.makedirs(output_directory, exist_ok=True)

    if output_directory.is_absolute():
        output_directory = output_directory.resolve()
    else:
        output_directory = output_directory.resolve().relative_to(Path().cwd())

    coupled_configuration = NEMSADCIRCRunConfiguration.read_directory(configuration_directory)

    runs = coupled_configuration['modeldriver']['runs']
    platform = coupled_configuration['modeldriver']['platform']

    nems_executable = coupled_configuration['nems']['executable_path']
    nems = coupled_configuration['nems'].nemspy_modeling_system

    job_duration = coupled_configuration['slurm']['job_duration']
    partition = coupled_configuration['slurm']['partition']
    email_type = coupled_configuration['slurm']['email_type']
    email_address = coupled_configuration['slurm']['email_address']

    original_fort13_filename = coupled_configuration['adcirc']['fort_13_path']
    original_fort14_filename = coupled_configuration['adcirc']['fort_14_path']
    adcprep_executable_path = coupled_configuration['adcirc']['adcprep_executable_path']
    tidal_spinup_duration = coupled_configuration['adcirc']['tidal_spinup_duration']
    source_filename = coupled_configuration['adcirc']['source_filename']
    use_original_mesh = coupled_configuration['adcirc']['use_original_mesh']

    LOGGER.info(
        f'generating {len(runs)} '
        f'"{platform.name.lower()}" configuration(s) in "{output_directory}"'
    )

    if source_filename is not None:
        LOGGER.debug(f'sourcing modules from "{source_filename}"')

    if original_fort14_filename is None or not original_fort14_filename.exists():
        raise FileNotFoundError(f'mesh XY not found at "{original_fort14_filename}"')

    if tidal_spinup_duration is not None:
        LOGGER.debug(f'setting spinup to {tidal_spinup_duration}')
        tidal_spinup_nems = ModelingSystem(
            nems.start_time - tidal_spinup_duration,
            nems.start_time,
            nems.interval,
            ocn=copy.deepcopy(nems['OCN']),
            **nems.attributes,
        )
    else:
        tidal_spinup_nems = None

    if tidal_spinup_nems is not None:
        coldstart_filenames = tidal_spinup_nems.write(
            output_directory,
            overwrite=overwrite,
            include_version=True,
            create_atm_namelist_rc=False,
        )
    else:
        coldstart_filenames = nems.write_directory(
            output_directory,
            overwrite=overwrite,
            include_version=True,
            create_atm_namelist_rc=False,
        )
    filenames = (f'"{filename.name}"' for filename in coldstart_filenames)
    LOGGER.info(f'writing NEMS coldstart configuration: ' f'{", ".join(filenames)}')

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

    if tidal_spinup_nems is not None:
        hotstart_filenames = nems.write(
            output_directory,
            overwrite=overwrite,
            include_version=True,
            create_atm_namelist_rc=False,
        )
        filenames = (f'"{filename.name}"' for filename in hotstart_filenames)
        LOGGER.info(f'writing NEMS hotstart configuration: ' f'{", ".join(filenames)}')
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

    adcprep_job_script_filename = output_directory / f'job_adcprep_{platform.name.lower()}.job'
    coldstart_run_script_filename = (
        output_directory / f'job_adcirc_{platform.name.lower()}.job.coldstart'
    )
    hotstart_run_script_filename = (
        output_directory / f'job_adcirc_{platform.name.lower()}.job.hotstart'
    )
    setup_script_filename = output_directory / f'setup_{platform.name.lower()}.sh'
    run_script_filename = output_directory / f'run_{platform.name.lower()}.sh'
    cleanup_script_filename = output_directory / f'cleanup.sh'

    LOGGER.debug(f'setting mesh partitioner "{adcprep_executable_path}"')
    adcprep_script = AdcircMeshPartitionJob(
        platform=platform,
        adcirc_mesh_partitions=nems['OCN'].processors,
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

    LOGGER.debug(
        f'writing mesh partitioning job script ' f'"{adcprep_job_script_filename.name}"'
    )
    adcprep_script.write(adcprep_job_script_filename, overwrite=overwrite)

    LOGGER.debug(f'setting NEMS executable "{nems_executable}"')
    if tidal_spinup_nems is not None:
        coldstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=tidal_spinup_nems.processors,
            slurm_account=slurm_account,
            slurm_duration=job_duration,
            slurm_run_name=adcirc_coldstart_run_name,
            executable=nems_executable,
            slurm_partition=partition,
            slurm_email_type=email_type,
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
            slurm_duration=job_duration,
            slurm_run_name=adcirc_coldstart_run_name,
            executable=nems_executable,
            slurm_partition=partition,
            slurm_email_type=email_type,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_coldstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_coldstart_run_name}.out.log',
            source_filename=source_filename,
        )

    LOGGER.debug(f'writing coldstart run script ' f'"{coldstart_run_script_filename.name}"')
    coldstart_run_script.write(coldstart_run_script_filename, overwrite=overwrite)

    if tidal_spinup_nems is not None:
        hotstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=nems.processors,
            slurm_account=slurm_account,
            slurm_duration=job_duration,
            slurm_run_name=adcirc_hotstart_run_name,
            executable=nems_executable,
            slurm_partition=partition,
            slurm_email_type=email_type,
            slurm_email_address=email_address,
            slurm_error_filename=f'{adcirc_hotstart_run_name}.err.log',
            slurm_log_filename=f'{adcirc_hotstart_run_name}.out.log',
            source_filename=source_filename,
        )

        LOGGER.debug(f'writing hotstart run script ' f'"{hotstart_run_script_filename.name}"')
        hotstart_run_script.write(hotstart_run_script_filename, overwrite=overwrite)

    # instantiate AdcircRun object.
    driver = coupled_configuration.adcircpy_driver

    local_fort13_filename = output_directory / 'fort.13'
    local_fort14_filename = output_directory / 'fort.14'
    if use_original_mesh:
        LOGGER.info(f'using original mesh from "{original_fort14_filename}"')
        if original_fort13_filename.exists():
            create_symlink(original_fort13_filename, local_fort13_filename)
        create_symlink(original_fort14_filename, local_fort14_filename)
    else:
        LOGGER.info(f'rewriting original mesh to "{local_fort14_filename}"')
        driver.write(
            output_directory,
            overwrite=overwrite,
            fort13=None,
            fort14='fort.14',
            fort15=None,
            fort22=None,
            coldstart=None,
            hotstart=None,
            driver=None,
        )

    LOGGER.debug(f'writing coldstart configuration to ' f'"{coldstart_directory}"')
    driver.write(
        coldstart_directory,
        overwrite=overwrite,
        fort13=None if use_original_mesh else 'fort.13',
        fort14=None,
        coldstart='fort.15',
        hotstart=None,
        driver=None,
    )
    if use_original_mesh:
        if local_fort13_filename.exists():
            create_symlink('../fort.13', coldstart_directory / 'fort.13', relative=True)
    create_symlink('../fort.14', coldstart_directory / 'fort.14', relative=True)
    create_symlink(
        f'../{adcprep_job_script_filename.name}',
        coldstart_directory / 'adcprep.job',
        relative=True,
    )
    create_symlink(
        f'../{coldstart_run_script_filename.name}',
        coldstart_directory / 'adcirc.job',
        relative=True,
    )
    create_symlink(
        '../nems.configure.coldstart', coldstart_directory / 'nems.configure', relative=True
    )
    create_symlink(
        '../model_configure.coldstart', coldstart_directory / 'model_configure', relative=True
    )
    create_symlink('../config.rc.coldstart', coldstart_directory / 'config.rc', relative=True)

    for run_name, attributes in runs.items():
        hotstart_directory = runs_directory / run_name
        LOGGER.debug(f'writing hotstart configuration to ' f'"{hotstart_directory}"')
        if attributes is not None:
            for name, value in attributes.items():
                if name is not None:
                    # if not isinstance(value, numpy.ndarray):
                    #     value = numpy.full([len(driver.mesh.coords)], fill_value=value)
                    if not driver.mesh.has_attribute(name):
                        driver.mesh.add_attribute(name)
                    driver.mesh.set_attribute(name, value)

        driver.write(
            hotstart_directory,
            overwrite=overwrite,
            fort13=None if use_original_mesh else 'fort.13',
            fort14=None,
            coldstart=None,
            hotstart='fort.15',
            driver=None,
        )
        if use_original_mesh:
            if local_fort13_filename.exists():
                create_symlink('../../fort.13', hotstart_directory / 'fort.13', relative=True)
        create_symlink('../../fort.14', hotstart_directory / 'fort.14', relative=True)
        create_symlink(
            f'../../{adcprep_job_script_filename.name}',
            hotstart_directory / 'adcprep.job',
            relative=True,
        )
        create_symlink(
            f'../../{hotstart_run_script_filename.name}',
            hotstart_directory / 'adcirc.job',
            relative=True,
        )
        create_symlink(
            '../../nems.configure.hotstart',
            hotstart_directory / 'nems.configure',
            relative=True,
        )
        create_symlink(
            '../../model_configure.hotstart',
            hotstart_directory / 'model_configure',
            relative=True,
        )
        create_symlink(
            '../../config.rc.hotstart', hotstart_directory / 'config.rc', relative=True
        )
        try:
            create_symlink(
                '../../coldstart/fort.67.nc', hotstart_directory / 'fort.67.nc', relative=True
            )
        except:
            LOGGER.warning(
                'unable to link `fort.67.nc` from coldstart to hotstart; you must manually link or copy this file after coldstart completes'
            )

    LOGGER.info(f'writing ensemble run script "{run_script_filename.name}"')
    run_script = EnsembleRunScript(platform)
    run_script.write(run_script_filename, overwrite=overwrite)

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(f'writing cleanup script "{cleanup_script_filename.name}"')
    cleanup_script.write(cleanup_script_filename, overwrite=overwrite)
