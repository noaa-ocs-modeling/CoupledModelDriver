from datetime import datetime, timedelta
import logging
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from ..configuration import (
    ADCIRCJSON,
    ATMESHForcingJSON,
    ForcingJSON,
    LOGGER,
    ModelDriverJSON,
    RunConfiguration,
    SlurmJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from ..platforms import Platform
from ..utilities import get_logger


def generate_adcirc_configuration(
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

    source_filename = coupled_configuration['adcirc']['source_filename']
    tidal_spinup_duration = coupled_configuration['adcirc']['tidal_spinup_duration']
    original_fort13_filename = coupled_configuration['adcirc']['fort_13_path']
    original_fort14_filename = coupled_configuration['adcirc']['fort_14_path']
    adcprep_executable_path = coupled_configuration['adcirc']['adcprep_executable_path']
    use_original_mesh = coupled_configuration['adcirc']['use_original_mesh']

    nems_executable = coupled_configuration['nems']['executable_path']
    nems = coupled_configuration['nems'].nemspy_modeling_system

    job_duration = coupled_configuration['slurm']['job_duration']
    partition = coupled_configuration['slurm']['partition']
    email_type = coupled_configuration['slurm']['email_type']
    email_address = coupled_configuration['slurm']['email_address']

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

    mesh = coupled_configuration['adcirc'].adcircpy_mesh

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
        f'"{mesh_partitioning_job_script_filename.name}"'
    )
    adcprep_script.write(mesh_partitioning_job_script_filename, overwrite=overwrite)

    coldstart_setup_script = AdcircSetupScript(
        nems_configure_filename=Path('../..') / 'nems.configure.coldstart',
        model_configure_filename=Path('../..') / 'model_configure.coldstart',
        config_rc_filename=Path('../..') / 'config.rc.coldstart',
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
            nems_path=nems_executable,
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
            nems_path=nems_executable,
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
        hotstart_setup_script = AdcircSetupScript(
            nems_configure_filename=Path('../../..') / 'nems.configure.hotstart',
            model_configure_filename=Path('../../..') / 'model_configure.hotstart',
            config_rc_filename=Path('../../..') / 'config.rc.hotstart',
            fort67_filename=Path('../../..') / 'coldstart/fort.67.nc',
        )
        hotstart_run_script = AdcircRunJob(
            platform=platform,
            slurm_tasks=nems.processors,
            slurm_account=slurm_account,
            slurm_duration=job_duration,
            slurm_run_name=adcirc_hotstart_run_name,
            nems_path=nems_executable,
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
    driver = coupled_configuration['adcirc'].adcircpy_driver

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
    create_symlink(local_fort14_filename, coldstart_directory / 'fort.14')

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
        create_symlink(local_fort14_filename, run_directory / 'fort.14')

    LOGGER.debug(f'writing ensemble setup script ' f'"{setup_script_filename.name}"')
    setup_script = EnsembleSetupScript(platform)
    setup_script.write(setup_script_filename, overwrite=overwrite)

    LOGGER.info(f'writing ensemble run script "{run_script_filename.name}"')
    run_script = EnsembleRunScript(platform, setup_script_filename.name)
    run_script.write(run_script_filename, overwrite=overwrite)

    cleanup_script = EnsembleCleanupScript()
    LOGGER.debug(f'writing cleanup script "{cleanup_script_filename.name}"')
    cleanup_script.write(cleanup_script_filename, overwrite=overwrite)


class ADCIRCRunConfiguration(RunConfiguration):
    required = [
        ModelDriverJSON,
        SlurmJSON,
        ADCIRCJSON,
    ]
    forcings = [
        TidalForcingJSON,
        ATMESHForcingJSON,
        WW3DATAForcingJSON,
    ]

    def __init__(
        self,
        fort13: PathLike,
        fort14: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        tidal_spinup_duration: timedelta = None,
        platform: Platform = None,
        runs: {str: (float, str)} = None,
        forcings: [ForcingJSON] = None,
        adcirc_processors: int = None,
        slurm_job_duration: timedelta = None,
        slurm_partition: str = None,
        slurm_email_address: str = None,
        adcprep_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        """
        Generate required configuration files for an ADCIRC run.

        :param fort13: path to input mesh values (`fort.13`)
        :param fort14: path to input mesh nodes (`fort.14`)
        :param modeled_start_time: start time within the modeled system
        :param modeled_end_time: end time within the modeled system
        :param modeled_timestep: time interval within the modeled system
        :param adcirc_processors: numbers of processors to use for Slurm job
        :param platform: HPC platform for which to configure
        :param tidal_spinup_duration: spinup time for ADCIRC coldstart
        :param runs: dictionary of run name to run value and mesh attribute name
        :param slurm_job_duration: wall clock time of job
        :param slurm_partition: Slurm partition
        :param slurm_email_address: email address to send Slurm notifications
        :param adcprep_executable: filename of compiled `adcprep`
        :param source_filename: path to module file to `source`
        """

        if platform is None:
            platform = Platform.LOCAL

        if forcings is None:
            forcings = []

        if adcirc_processors is None:
            adcirc_processors = 11

        if adcprep_executable is None:
            adcprep_executable = 'adcprep'

        self.__slurm = SlurmJSON(
            account=platform.value['slurm_account'],
            tasks=adcirc_processors,
            partition=slurm_partition,
            job_duration=slurm_job_duration,
            email_address=slurm_email_address,
        )

        self.__adcirc = ADCIRCJSON(
            adcprep_executable_path=adcprep_executable,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            modeled_timestep=modeled_timestep,
            fort_13_path=fort13,
            fort_14_path=fort14,
            tidal_spinup_duration=tidal_spinup_duration,
            source_filename=source_filename,
            slurm_configuration=self.__slurm,
            processors=adcirc_processors,
        )

        self.__driver = ModelDriverJSON(platform=platform, runs=runs)
        super().__init__([self.__driver, self.__slurm, self.__adcirc])

        for forcing in forcings:
            self.add_forcing(forcing)

    @property
    def driver(self) -> ModelDriverJSON:
        return self.__driver

    @property
    def slurm(self) -> SlurmJSON:
        return self.__slurm

    @property
    def adcirc(self) -> ADCIRCJSON:
        return self.__adcirc

    def add_forcing(self, forcing: ForcingJSON):
        if not isinstance(forcing, ForcingJSON):
            if isinstance(forcing, AtmosphericMeshForcing):
                forcing = ATMESHForcingJSON.from_adcircpy(forcing)
            elif isinstance(forcing, WaveWatch3DataForcing):
                forcing = WW3DATAForcingJSON.from_adcircpy(forcing)
            elif isinstance(forcing, Tides):
                forcing = TidalForcingJSON.from_adcircpy(forcing)
            else:
                raise NotImplementedError(f'unable to parse object of type {type(forcing)}')

        if forcing not in self.configurations:
            self.configurations[forcing.name] = forcing
            self.adcirc.forcings.append(forcing)

    @property
    def adcircpy_mesh(self) -> AdcircMesh:
        return self.adcirc.adcircpy_mesh

    @property
    def adcircpy_driver(self) -> AdcircRun:
        return self.adcirc.adcircpy_driver

    @classmethod
    def read_directory(cls, directory: PathLike) -> 'RunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent

        configurations = []
        for name, configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        driver = configurations[0]
        slurm = configurations[1]
        adcirc = configurations[2]

        instance = cls(
            fort13=adcirc['fort_13_path'],
            fort14=adcirc['fort_14_path'],
            modeled_start_time=adcirc['modeled_start_time'],
            modeled_end_time=adcirc['modeled_end_time'],
            modeled_timestep=adcirc['modeled_timestep'],
            tidal_spinup_duration=adcirc['tidal_spinup_duration'],
            platform=driver['platform'],
            runs=driver['runs'],
            slurm_processors=slurm['tasks'],
            slurm_job_duration=slurm['job_duration'],
            slurm_partition=slurm['partition'],
            slurm_email_address=slurm['email_address'],
            adcprep_executable=adcirc['adcprep_executable_path'],
            source_filename=adcirc['source_filename'],
        )

        for name, configuration_class in cls.forcings:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                instance.add_forcing(configuration_class.from_file(filename))

        return instance
