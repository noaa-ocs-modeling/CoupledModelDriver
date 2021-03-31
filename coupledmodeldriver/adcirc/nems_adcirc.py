import copy
from datetime import datetime, timedelta
import logging
import os
from os import PathLike
from pathlib import Path

from adcircpy import Tides
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from nemspy import ModelingSystem
import numpy

from .adcirc import ADCIRCRunConfiguration
from ..configuration import (
    ADCIRCJSON,
    ATMESHForcingJSON,
    ForcingJSON,
    ModelDriverJSON,
    NEMSJSON,
    SlurmJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from ..job_script import (AdcircMeshPartitionJob, AdcircNEMSSetupScript,
                          AdcircRunJob, EnsembleCleanupScript,
                          EnsembleRunScript,
                          EnsembleSetupScript)
from ..platforms import Platform
from ..utilities import LOGGER, create_symlink, get_logger


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

    coupled_configuration = ADCIRCCoupledRunConfiguration.read_directory(
        configuration_directory
    )

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

    adcprep_job_script_filename = (
        output_directory / f'job_adcprep_{platform.name.lower()}.job'
    )
    coldstart_setup_script_filename = output_directory / f'setup.sh.coldstart'
    coldstart_run_script_filename = (
        output_directory / f'job_adcirc_{platform.name.lower()}.job.coldstart'
    )
    hotstart_setup_script_filename = output_directory / f'setup.sh.hotstart'
    hotstart_run_script_filename = (
        output_directory / f'job_adcirc_{platform.name.lower()}.job.hotstart'
    )
    setup_script_filename = output_directory / f'setup_{platform.name.lower()}.sh'
    run_script_filename = output_directory / f'run_{platform.name.lower()}.sh'
    cleanup_script_filename = output_directory / f'cleanup.sh'
    generation_script_filename = output_directory / f'generate.py'

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
        f'writing mesh partitioning job script '
        f'"{adcprep_job_script_filename.name}"'
    )
    adcprep_script.write(adcprep_job_script_filename, overwrite=overwrite)

    coldstart_setup_script = AdcircNEMSSetupScript(
        nems_configure_filename=Path('..') / 'nems.configure.coldstart',
        model_configure_filename=Path('..') / 'model_configure.coldstart',
        config_rc_filename=Path('..') / 'config.rc.coldstart',
    )

    LOGGER.debug(
        f'writing coldstart setup script ' f'"{coldstart_setup_script_filename.name}"'
    )
    coldstart_setup_script.write(coldstart_setup_script_filename, overwrite=overwrite)

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
        hotstart_setup_script = AdcircNEMSSetupScript(
            nems_configure_filename=Path('../..') / 'nems.configure.hotstart',
            model_configure_filename=Path('../..') / 'model_configure.hotstart',
            config_rc_filename=Path('../..') / 'config.rc.hotstart',
            fort67_filename=Path('../..') / 'coldstart/fort.67.nc',
        )
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

        LOGGER.debug(
            f'writing hotstart setup script ' f'"{hotstart_setup_script_filename.name}"'
        )
        hotstart_setup_script.write(hotstart_setup_script_filename, overwrite=overwrite)

        LOGGER.debug(f'writing hotstart run script ' f'"{hotstart_run_script_filename.name}"')
        hotstart_run_script.write(hotstart_run_script_filename, overwrite=overwrite)

    # instantiate AdcircRun object.
    driver = coupled_configuration.adcircpy_driver

    local_fort14_filename = output_directory / 'fort.14'
    if use_original_mesh:
        LOGGER.info(f'using original mesh from "{original_fort14_filename}"')
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
        if original_fort13_filename.exists():
            create_symlink(original_fort13_filename, coldstart_directory / 'fort.13')
    create_symlink('../fort.14', coldstart_directory / 'fort.14', relative=True)

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
            fort13=None if use_original_mesh else 'fort.13',
            fort14=None,
            coldstart=None,
            hotstart='fort.15',
            driver=None,
        )
        if use_original_mesh:
            if original_fort13_filename.exists():
                create_symlink(original_fort13_filename, run_directory / 'fort.13')
        create_symlink('../../fort.14', run_directory / 'fort.14', relative=True)

    LOGGER.debug(f'writing ensemble setup script ' f'"{setup_script_filename.name}"')
    setup_script = EnsembleSetupScript(
        platform=platform,
        adcprep_job_script=adcprep_job_script_filename.name,
        coldstart_job_script=coldstart_run_script_filename.name,
        hotstart_job_script=hotstart_run_script_filename.name,
        coldstart_setup_script=coldstart_setup_script_filename.name,
        hotstart_setup_script=hotstart_setup_script_filename.name,
    )
    setup_script.write(setup_script_filename, overwrite=overwrite)

    LOGGER.info(f'writing ensemble run script "{run_script_filename.name}"')
    run_script = EnsembleRunScript(platform, setup_script_filename.name)
    run_script.write(run_script_filename, overwrite=overwrite)

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(f'writing cleanup script "{cleanup_script_filename.name}"')
    cleanup_script.write(cleanup_script_filename, overwrite=overwrite)


class ADCIRCCoupledRunConfiguration(ADCIRCRunConfiguration):
    required = [
        ModelDriverJSON,
        NEMSJSON,
        SlurmJSON,
        ADCIRCJSON,
    ]

    def __init__(
        self,
        fort13: PathLike,
        fort14: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        nems_interval: timedelta,
        nems_connections: [str],
        nems_mediations: [str],
        nems_sequence: [str],
        tidal_spinup_duration: timedelta = None,
        platform: Platform = None,
        runs: {str: (float, str)} = None,
        forcings: [ForcingJSON] = None,
        adcirc_processors: int = None,
        slurm_job_duration: timedelta = None,
        slurm_partition: str = None,
        slurm_email_address: str = None,
        nems_executable: PathLike = None,
        adcprep_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        self.__nems = None

        super().__init__(
            fort13=fort13,
            fort14=fort14,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            runs=runs,
            forcings=forcings,
            adcirc_processors=adcirc_processors,
            slurm_job_duration=slurm_job_duration,
            slurm_partition=slurm_partition,
            slurm_email_address=slurm_email_address,
            adcprep_executable=adcprep_executable,
            source_filename=source_filename,
        )

        self.__nems = NEMSJSON(
            executable_path=nems_executable,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            interval=nems_interval,
            models=self.nemspy_entries,
            connections=nems_connections,
            mediations=nems_mediations,
            sequence=nems_sequence,
        )

        self.configurations[self.nems.name] = self.nems
        self['slurm']['tasks'] = self.nems.nemspy_modeling_system.processors

    @property
    def nemspy_modeling_system(self) -> ModelingSystem:
        return self['nems'].nemspy_modeling_system

    def add_forcing(self, forcing: Forcing):
        if not isinstance(forcing, ForcingJSON):
            if isinstance(forcing, AtmosphericMeshForcing):
                forcing = ATMESHForcingJSON.from_adcircpy(forcing)
                if self['nems'] is not None:
                    self['nems']['atm'] = forcing.nemspy_entry
            elif isinstance(forcing, WaveWatch3DataForcing):
                forcing = WW3DATAForcingJSON.from_adcircpy(forcing)
                if self['nems'] is not None:
                    self['nems']['wav'] = forcing.nemspy_entry
            elif isinstance(forcing, Tides):
                forcing = TidalForcingJSON.from_adcircpy(forcing)
            else:
                raise NotImplementedError(f'unable to parse object of type {type(forcing)}')

        if forcing not in self:
            self[forcing.name] = forcing
            self['adcirc'].forcings.append(forcing)

    @classmethod
    def from_configurations(
        cls,
        driver: ModelDriverJSON,
        nems: NEMSJSON,
        slurm: SlurmJSON,
        adcirc: ADCIRCJSON,
        forcings: [ForcingJSON] = None,
    ) -> 'ADCIRCCoupledRunConfiguration':
        instance = super().from_configurations(
            driver=driver, slurm=slurm, adcirc=adcirc, forcings=None,
        )
        instance.__class__ = cls
        instance.configurations['nems'] = nems

        if forcings is not None:
            for forcing in forcings:
                instance.add_forcing(forcing)

        return instance

    @classmethod
    def read_directory(cls, directory: PathLike) -> 'ADCIRCCoupledRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent

        configurations = []
        for configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        forcings = []
        for configuration_class in cls.forcings:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                forcings.append(configuration_class.from_file(filename))

        return cls.from_configurations(
            driver=configurations[0],
            nems=configurations[1],
            slurm=configurations[2],
            adcirc=configurations[3],
            forcings=forcings,
        )
